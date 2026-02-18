#include "rtbot_sql/parser/ast_converter.h"

#include <algorithm>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

namespace rtbot_sql::parser {

using json = nlohmann::json;

namespace {

// --- Expression conversion ---

ast::Expr convert_expr(const json& node);

ast::Expr convert_column_ref(const json& node) {
  ast::ColumnRef ref;
  const auto& fields = node["fields"];
  if (fields.size() == 1) {
    ref.column_name = fields[0]["String"]["sval"].get<std::string>();
  } else if (fields.size() == 2) {
    ref.table_alias = fields[0]["String"]["sval"].get<std::string>();
    ref.column_name = fields[1]["String"]["sval"].get<std::string>();
  }
  return ref;
}

ast::Expr convert_a_const(const json& node) {
  if (node.contains("ival")) {
    return ast::Constant{static_cast<double>(node["ival"]["ival"].get<int64_t>())};
  }
  if (node.contains("fval")) {
    return ast::Constant{std::stod(node["fval"]["fval"].get<std::string>())};
  }
  if (node.contains("sval")) {
    // String constants not supported — all values are numeric in RTBot
    throw std::runtime_error("string constants not supported");
  }
  throw std::runtime_error("unknown A_Const type");
}

bool is_comparison_op(const std::string& op) {
  return op == ">" || op == "<" || op == ">=" || op == "<=" ||
         op == "=" || op == "<>" || op == "!=";
}

bool is_arithmetic_op(const std::string& op) {
  return op == "+" || op == "-" || op == "*" || op == "/";
}

ast::Expr convert_a_expr(const json& node) {
  std::string kind = node["kind"].get<std::string>();
  std::string op;
  if (node.contains("name") && !node["name"].empty()) {
    op = node["name"][0]["String"]["sval"].get<std::string>();
  }

  // Normalize <> to !=
  if (op == "<>") op = "!=";

  if (kind == "AEXPR_OP") {
    auto left = convert_expr(node.at("lexpr"));
    auto right = convert_expr(node.at("rexpr"));

    if (is_comparison_op(op)) {
      auto e = std::make_unique<ast::ComparisonExpr>();
      e->op = op;
      e->left = std::move(left);
      e->right = std::move(right);
      return e;
    }
    if (is_arithmetic_op(op)) {
      auto e = std::make_unique<ast::BinaryExpr>();
      e->op = op;
      e->left = std::move(left);
      e->right = std::move(right);
      return e;
    }
    throw std::runtime_error("unsupported operator: " + op);
  }

  if (kind == "AEXPR_BETWEEN" || kind == "AEXPR_NOT_BETWEEN") {
    throw std::runtime_error("BETWEEN not yet supported in converter");
  }

  throw std::runtime_error("unsupported A_Expr kind: " + kind);
}

ast::Expr convert_func_call(const json& node) {
  auto f = std::make_unique<ast::FuncCall>();

  // Function name
  const auto& funcname = node["funcname"];
  f->name = funcname.back()["String"]["sval"].get<std::string>();
  // Uppercase for consistency
  std::transform(f->name.begin(), f->name.end(), f->name.begin(), ::toupper);

  // COUNT(*) has agg_star=true and no args
  if (node.contains("agg_star") && node["agg_star"].get<bool>()) {
    // COUNT(*) — no args
    return f;
  }

  // Args
  if (node.contains("args")) {
    for (const auto& arg : node["args"]) {
      f->args.push_back(convert_expr(arg));
    }
  }

  return f;
}

ast::Expr convert_bool_expr(const json& node) {
  std::string boolop = node["boolop"].get<std::string>();

  if (boolop == "NOT_EXPR") {
    auto e = std::make_unique<ast::NotExpr>();
    e->operand = convert_expr(node["args"][0]);
    return e;
  }

  // AND_EXPR / OR_EXPR — can have multiple args, chain them pairwise
  std::string op = (boolop == "AND_EXPR") ? "AND" : "OR";
  const auto& args = node["args"];

  auto result = convert_expr(args[0]);
  for (size_t i = 1; i < args.size(); ++i) {
    auto e = std::make_unique<ast::LogicalExpr>();
    e->op = op;
    e->left = std::move(result);
    e->right = convert_expr(args[i]);
    result = std::move(e);
  }
  return result;
}

ast::Expr convert_case_expr(const json& node) {
  auto e = std::make_unique<ast::CaseExpr>();

  for (const auto& arg : node["args"]) {
    const auto& when = arg["CaseWhen"];
    ast::CaseWhenClause clause;
    clause.condition = convert_expr(when["expr"]);
    clause.result = convert_expr(when["result"]);
    e->when_clauses.push_back(std::move(clause));
  }

  if (node.contains("defresult") && !node["defresult"].is_null()) {
    e->else_result = convert_expr(node["defresult"]);
  }

  return e;
}

ast::Expr convert_expr(const json& node) {
  if (node.contains("ColumnRef")) return convert_column_ref(node["ColumnRef"]);
  if (node.contains("A_Const")) return convert_a_const(node["A_Const"]);
  if (node.contains("A_Expr")) return convert_a_expr(node["A_Expr"]);
  if (node.contains("FuncCall")) return convert_func_call(node["FuncCall"]);
  if (node.contains("BoolExpr")) return convert_bool_expr(node["BoolExpr"]);
  if (node.contains("CaseExpr")) return convert_case_expr(node["CaseExpr"]);

  throw std::runtime_error("unsupported expression node type");
}

// --- SELECT statement ---

ast::SelectStmt convert_select_stmt(const json& node) {
  ast::SelectStmt stmt;

  // Target list (SELECT items)
  // SELECT * produces a ColumnRef with A_Star — we represent that as empty select_list.
  if (node.contains("targetList")) {
    bool is_star = false;
    if (node["targetList"].size() == 1) {
      const auto& val = node["targetList"][0]["ResTarget"]["val"];
      if (val.contains("ColumnRef") &&
          !val["ColumnRef"]["fields"].empty() &&
          val["ColumnRef"]["fields"][0].contains("A_Star")) {
        is_star = true;
      }
    }
    if (!is_star) {
      for (const auto& target : node["targetList"]) {
        const auto& rt = target["ResTarget"];
        ast::SelectItem item;
        item.expr = convert_expr(rt["val"]);
        if (rt.contains("name") && !rt["name"].is_null()) {
          item.alias = rt["name"].get<std::string>();
        }
        stmt.select_list.push_back(std::move(item));
      }
    }
  }

  // FROM clause
  if (node.contains("fromClause") && !node["fromClause"].empty()) {
    const auto& from = node["fromClause"][0];
    if (from.contains("RangeVar")) {
      stmt.from_table = from["RangeVar"]["relname"].get<std::string>();
      if (from["RangeVar"].contains("alias") &&
          !from["RangeVar"]["alias"].is_null()) {
        stmt.from_alias =
            from["RangeVar"]["alias"]["aliasname"].get<std::string>();
      }
    }
  }

  // WHERE clause
  if (node.contains("whereClause") && !node["whereClause"].is_null()) {
    stmt.where_clause = convert_expr(node["whereClause"]);
  }

  // GROUP BY
  if (node.contains("groupClause")) {
    for (const auto& g : node["groupClause"]) {
      stmt.group_by.push_back(convert_expr(g));
    }
  }

  // HAVING
  if (node.contains("havingClause") && !node["havingClause"].is_null()) {
    stmt.having = convert_expr(node["havingClause"]);
  }

  // LIMIT
  if (node.contains("limitCount") && !node["limitCount"].is_null()) {
    const auto& lc = node["limitCount"];
    if (lc.contains("A_Const")) {
      if (lc["A_Const"].contains("ival")) {
        stmt.limit = lc["A_Const"]["ival"]["ival"].get<int>();
      }
    }
  }

  return stmt;
}

// --- CREATE TABLE/STREAM ---

ast::CreateStreamStmt convert_create_stmt(const json& node) {
  ast::CreateStreamStmt stmt;
  stmt.name = node["relation"]["relname"].get<std::string>();

  if (node.contains("tableElts")) {
    for (const auto& elt : node["tableElts"]) {
      if (elt.contains("ColumnDef")) {
        ast::ColumnDefAST col;
        col.name = elt["ColumnDef"]["colname"].get<std::string>();
        // Type — extract last name component
        if (elt["ColumnDef"].contains("typeName")) {
          const auto& names = elt["ColumnDef"]["typeName"]["names"];
          col.type_name = names.back()["String"]["sval"].get<std::string>();
        }
        stmt.columns.push_back(std::move(col));
      }
    }
  }

  return stmt;
}

// --- CREATE MATERIALIZED VIEW ---

ast::CreateViewStmt convert_create_table_as_stmt(const json& node) {
  ast::CreateViewStmt stmt;
  stmt.name = node["into"]["rel"]["relname"].get<std::string>();

  std::string objtype = node.value("objtype", "");
  stmt.materialized = (objtype == "OBJECT_MATVIEW");

  stmt.query = convert_select_stmt(node["query"]["SelectStmt"]);

  return stmt;
}

// --- CREATE VIEW ---

ast::CreateViewStmt convert_view_stmt(const json& node) {
  ast::CreateViewStmt stmt;
  stmt.name = node["view"]["relname"].get<std::string>();
  stmt.materialized = false;
  stmt.query = convert_select_stmt(node["query"]["SelectStmt"]);
  return stmt;
}

// --- INSERT ---

ast::InsertStmt convert_insert_stmt(const json& node) {
  ast::InsertStmt stmt;
  stmt.table_name = node["relation"]["relname"].get<std::string>();

  // Columns (optional)
  if (node.contains("cols")) {
    for (const auto& col : node["cols"]) {
      stmt.columns.push_back(
          col["ResTarget"]["name"].get<std::string>());
    }
  }

  // Values — from selectStmt.SelectStmt.valuesLists[0].List.items
  if (node.contains("selectStmt")) {
    const auto& sel = node["selectStmt"]["SelectStmt"];
    if (sel.contains("valuesLists") && !sel["valuesLists"].empty()) {
      const auto& items = sel["valuesLists"][0]["List"]["items"];
      for (const auto& item : items) {
        stmt.values.push_back(convert_expr(item));
      }
    }
  }

  return stmt;
}

// --- DROP ---

ast::DropStmt convert_drop_stmt(const json& node) {
  ast::DropStmt stmt;

  // Extract entity name from objects list
  if (node.contains("objects") && !node["objects"].empty()) {
    const auto& obj = node["objects"][0];
    if (obj.contains("List")) {
      const auto& items = obj["List"]["items"];
      if (!items.empty()) {
        stmt.name = items.back()["String"]["sval"].get<std::string>();
      }
    } else if (obj.contains("String")) {
      stmt.name = obj["String"]["sval"].get<std::string>();
    }
  }

  // Entity type
  std::string remove_type = node.value("removeType", "");
  if (remove_type == "OBJECT_TABLE")
    stmt.entity_type = "TABLE";
  else if (remove_type == "OBJECT_VIEW")
    stmt.entity_type = "VIEW";
  else if (remove_type == "OBJECT_MATVIEW")
    stmt.entity_type = "MATERIALIZED_VIEW";
  else
    stmt.entity_type = "STREAM";  // CREATE TABLE used for streams

  stmt.if_exists =
      node.contains("missing_ok") && node["missing_ok"].get<bool>();

  return stmt;
}

}  // namespace

ast::Statement convert_parse_tree(const std::string& json_str) {
  auto root = json::parse(json_str);

  if (!root.contains("stmts") || root["stmts"].empty()) {
    throw std::runtime_error("empty parse tree");
  }

  const auto& stmt_wrapper = root["stmts"][0]["stmt"];

  if (stmt_wrapper.contains("SelectStmt")) {
    return convert_select_stmt(stmt_wrapper["SelectStmt"]);
  }
  if (stmt_wrapper.contains("CreateStmt")) {
    return convert_create_stmt(stmt_wrapper["CreateStmt"]);
  }
  if (stmt_wrapper.contains("CreateTableAsStmt")) {
    return convert_create_table_as_stmt(stmt_wrapper["CreateTableAsStmt"]);
  }
  if (stmt_wrapper.contains("ViewStmt")) {
    return convert_view_stmt(stmt_wrapper["ViewStmt"]);
  }
  if (stmt_wrapper.contains("InsertStmt")) {
    return convert_insert_stmt(stmt_wrapper["InsertStmt"]);
  }
  if (stmt_wrapper.contains("DropStmt")) {
    return convert_drop_stmt(stmt_wrapper["DropStmt"]);
  }

  throw std::runtime_error("unsupported statement type");
}

}  // namespace rtbot_sql::parser
