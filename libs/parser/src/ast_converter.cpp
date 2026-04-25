#include "rtbot_sql/parser/ast_converter.h"

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "rtbot_sql/parser/source_text.h"

namespace rtbot_sql::parser {

using json = nlohmann::json;

namespace {

// Read libpg_query's `location` field (byte offset) from a JSON node and
// convert it to a SourceLocation. Returns {-1,-1} if src is null or the
// node has no location.
ast::SourceLocation node_loc(const json& node, const SourceText* src) {
  if (!src) return {};
  if (!node.is_object()) return {};
  auto it = node.find("location");
  if (it == node.end() || !it->is_number_integer()) return {};
  return compute_location(*src, it->get<int>());
}

// --- Expression conversion ---

ast::Expr convert_expr(const json& node, const SourceText* src);

std::optional<double> parse_numeric_const_value(const json& node) {
  if (node.contains("ival")) {
    const auto& iv = node["ival"];
    if (iv.is_object() && iv.contains("ival") && iv["ival"].is_number_integer()) {
      return static_cast<double>(iv["ival"].get<int64_t>());
    }
    if (iv.is_object() && iv.empty()) {
      return 0.0;
    }
    if (iv.is_number_integer()) {
      return static_cast<double>(iv.get<int64_t>());
    }
  }

  if (node.contains("fval")) {
    const auto& fv = node["fval"];
    if (fv.is_object() && fv.contains("fval") && fv["fval"].is_string()) {
      return std::stod(fv["fval"].get<std::string>());
    }
    if (fv.is_string()) {
      return std::stod(fv.get<std::string>());
    }
    if (fv.is_number()) {
      return fv.get<double>();
    }
  }

  if (node.contains("boolval")) {
    const auto& bv = node["boolval"];
    if (bv.is_object() && bv.contains("boolval") && bv["boolval"].is_boolean()) {
      return bv["boolval"].get<bool>() ? 1.0 : 0.0;
    }
    if (bv.is_boolean()) {
      return bv.get<bool>() ? 1.0 : 0.0;
    }
  }

  if (node.contains("val") && node["val"].is_object()) {
    const auto& val = node["val"];

    if (val.contains("Integer") && val["Integer"].is_object() &&
        val["Integer"].contains("ival") &&
        val["Integer"]["ival"].is_number_integer()) {
      return static_cast<double>(val["Integer"]["ival"].get<int64_t>());
    }
    if (val.contains("Integer") && val["Integer"].is_object() &&
        val["Integer"].empty()) {
      return 0.0;
    }

    if (val.contains("Float") && val["Float"].is_object()) {
      const auto& fl = val["Float"];
      if (fl.contains("fval") && fl["fval"].is_string()) {
        return std::stod(fl["fval"].get<std::string>());
      }
      if (fl.contains("str") && fl["str"].is_string()) {
        return std::stod(fl["str"].get<std::string>());
      }
    }

    if (val.contains("Boolean") && val["Boolean"].is_object() &&
        val["Boolean"].contains("boolval") &&
        val["Boolean"]["boolval"].is_boolean()) {
      return val["Boolean"]["boolval"].get<bool>() ? 1.0 : 0.0;
    }
  }

  return std::nullopt;
}

ast::Expr convert_column_ref(const json& node, const SourceText* src) {
  ast::ColumnRef ref;
  const auto& fields = node["fields"];
  if (fields.size() == 1) {
    ref.column_name = fields[0]["String"]["sval"].get<std::string>();
  } else if (fields.size() == 2) {
    ref.table_alias = fields[0]["String"]["sval"].get<std::string>();
    ref.column_name = fields[1]["String"]["sval"].get<std::string>();
  }
  ref.loc = node_loc(node, src);
  return ref;
}

ast::Expr convert_a_const(const json& node, const SourceText* src) {
  ast::SourceLocation loc = node_loc(node, src);
  if (auto numeric = parse_numeric_const_value(node); numeric.has_value()) {
    return ast::Constant{*numeric, loc};
  }
  if (node.contains("sval")) {
    return ast::StringConstant{node["sval"]["sval"].get<std::string>(), loc};
  }
  if (node.contains("val") && node["val"].is_object() &&
      node["val"].contains("String")) {
    return ast::StringConstant{
        node["val"]["String"]["sval"].get<std::string>(), loc};
  }
  throw ConverterError("unknown A_Const type: " + node.dump(),
                       node_loc(node, src));
}

bool is_comparison_op(const std::string& op) {
  return op == ">" || op == "<" || op == ">=" || op == "<=" ||
         op == "=" || op == "<>" || op == "!=";
}

bool is_arithmetic_op(const std::string& op) {
  return op == "+" || op == "-" || op == "*" || op == "/";
}

ast::Expr convert_a_expr(const json& node, const SourceText* src) {
  std::string kind = node["kind"].get<std::string>();
  std::string op;
  if (node.contains("name") && !node["name"].empty()) {
    op = node["name"][0]["String"]["sval"].get<std::string>();
  }

  // Normalize <> to !=
  if (op == "<>") op = "!=";

  ast::SourceLocation loc = node_loc(node, src);

  if (kind == "AEXPR_OP") {
    auto left = convert_expr(node.at("lexpr"), src);
    auto right = convert_expr(node.at("rexpr"), src);

    if (is_comparison_op(op)) {
      auto e = std::make_unique<ast::ComparisonExpr>();
      e->op = op;
      e->left = std::move(left);
      e->right = std::move(right);
      e->loc = loc;
      return e;
    }
    if (is_arithmetic_op(op)) {
      auto e = std::make_unique<ast::BinaryExpr>();
      e->op = op;
      e->left = std::move(left);
      e->right = std::move(right);
      e->loc = loc;
      return e;
    }
    throw ConverterError("unsupported operator: " + op, loc);
  }

  if (kind == "AEXPR_BETWEEN" || kind == "AEXPR_NOT_BETWEEN") {
    throw ConverterError("BETWEEN not yet supported in converter", loc);
  }

  throw ConverterError("unsupported A_Expr kind: " + kind, loc);
}

ast::Expr convert_func_call(const json& node, const SourceText* src) {
  auto f = std::make_unique<ast::FuncCall>();

  // Function name
  const auto& funcname = node["funcname"];
  f->name = funcname.back()["String"]["sval"].get<std::string>();
  // Uppercase for consistency
  std::transform(f->name.begin(), f->name.end(), f->name.begin(), ::toupper);

  f->loc = node_loc(node, src);

  // COUNT(*) has agg_star=true and no args
  if (node.contains("agg_star") && node["agg_star"].get<bool>()) {
    // COUNT(*) — no args
    return f;
  }

  // Args
  if (node.contains("args")) {
    for (const auto& arg : node["args"]) {
      f->args.push_back(convert_expr(arg, src));
    }
  }

  return f;
}

ast::Expr convert_bool_expr(const json& node, const SourceText* src) {
  std::string boolop = node["boolop"].get<std::string>();
  ast::SourceLocation loc = node_loc(node, src);

  if (boolop == "NOT_EXPR") {
    auto e = std::make_unique<ast::NotExpr>();
    e->operand = convert_expr(node["args"][0], src);
    e->loc = loc;
    return e;
  }

  // AND_EXPR / OR_EXPR — can have multiple args, chain them pairwise
  std::string op = (boolop == "AND_EXPR") ? "AND" : "OR";
  const auto& args = node["args"];

  auto result = convert_expr(args[0], src);
  for (size_t i = 1; i < args.size(); ++i) {
    auto e = std::make_unique<ast::LogicalExpr>();
    e->op = op;
    e->left = std::move(result);
    e->right = convert_expr(args[i], src);
    e->loc = loc;
    result = std::move(e);
  }
  return result;
}

ast::Expr convert_a_array_expr(const json& node, const SourceText* src) {
  ast::ArrayLiteral arr;
  arr.loc = node_loc(node, src);
  if (node.contains("elements")) {
    for (const auto& elem : node["elements"]) {
      if (!elem.contains("A_Const")) {
        throw ConverterError("ARRAY elements must be numeric constants",
                             node_loc(elem, src));
      }
      auto expr = convert_a_const(elem["A_Const"], src);
      if (auto* c = std::get_if<ast::Constant>(&expr)) {
        arr.values.push_back(c->value);
      } else {
        throw ConverterError("ARRAY elements must be numeric constants",
                             node_loc(elem["A_Const"], src));
      }
    }
  }
  return arr;
}

ast::Expr convert_case_expr(const json& node, const SourceText* src) {
  auto e = std::make_unique<ast::CaseExpr>();
  e->loc = node_loc(node, src);

  for (const auto& arg : node["args"]) {
    const auto& when = arg["CaseWhen"];
    ast::CaseWhenClause clause;
    clause.condition = convert_expr(when["expr"], src);
    clause.result = convert_expr(when["result"], src);
    clause.loc = node_loc(when, src);
    e->when_clauses.push_back(std::move(clause));
  }

  if (node.contains("defresult") && !node["defresult"].is_null()) {
    e->else_result = convert_expr(node["defresult"], src);
  }

  return e;
}

ast::Expr convert_expr(const json& node, const SourceText* src) {
  if (node.contains("ColumnRef")) return convert_column_ref(node["ColumnRef"], src);
  if (node.contains("A_Const")) return convert_a_const(node["A_Const"], src);
  if (node.contains("A_Expr")) return convert_a_expr(node["A_Expr"], src);
  if (node.contains("FuncCall")) return convert_func_call(node["FuncCall"], src);
  if (node.contains("BoolExpr")) return convert_bool_expr(node["BoolExpr"], src);
  if (node.contains("CaseExpr")) return convert_case_expr(node["CaseExpr"], src);
  if (node.contains("A_ArrayExpr")) return convert_a_array_expr(node["A_ArrayExpr"], src);

  // No discriminating "kind" key matched. Report at the wrapper's
  // location when libpg_query attached one to it.
  throw ConverterError("unsupported expression node type",
                       node_loc(node, src));
}

// --- SELECT statement ---

ast::SelectStmt convert_select_stmt(const json& node, const SourceText* src) {
  ast::SelectStmt stmt;
  stmt.loc = node_loc(node, src);

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
        item.expr = convert_expr(rt["val"], src);
        if (rt.contains("name") && !rt["name"].is_null()) {
          item.alias = rt["name"].get<std::string>();
        }
        item.loc = node_loc(rt, src);
        stmt.select_list.push_back(std::move(item));
      }
    }
  }

  // FROM clause
  if (node.contains("fromClause") && !node["fromClause"].empty()) {
    for (const auto& from : node["fromClause"]) {
      if (from.contains("RangeVar")) {
        ast::FromSource fsrc;
        fsrc.table_name = from["RangeVar"]["relname"].get<std::string>();
        if (from["RangeVar"].contains("alias") &&
            !from["RangeVar"]["alias"].is_null()) {
          fsrc.alias =
              from["RangeVar"]["alias"]["aliasname"].get<std::string>();
        }
        fsrc.loc = node_loc(from["RangeVar"], src);
        stmt.from_tables.push_back(std::move(fsrc));
      } else if (from.contains("JoinExpr")) {
        // JOIN: left arg is the main table, right arg is the join target
        const auto& join_expr = from["JoinExpr"];

        // Left (main) table
        if (join_expr.contains("larg") && join_expr["larg"].contains("RangeVar")) {
          const auto& larg = join_expr["larg"]["RangeVar"];
          stmt.from_table = larg["relname"].get<std::string>();
          if (larg.contains("alias") && !larg["alias"].is_null()) {
            stmt.from_alias = larg["alias"]["aliasname"].get<std::string>();
          }
        }

        // Right (join target)
        if (join_expr.contains("rarg") && join_expr["rarg"].contains("RangeVar")) {
          ast::JoinClause jc;
          const auto& rarg = join_expr["rarg"]["RangeVar"];
          jc.table_name = rarg["relname"].get<std::string>();
          if (rarg.contains("alias") && !rarg["alias"].is_null()) {
            jc.table_alias = rarg["alias"]["aliasname"].get<std::string>();
          }

          std::string jointype = join_expr.value("jointype", "JOIN_INNER");
          if (jointype == "JOIN_LEFT") jc.join_type = "LEFT";
          else if (jointype == "JOIN_RIGHT") jc.join_type = "RIGHT";
          else jc.join_type = "INNER";

          if (join_expr.contains("quals") && !join_expr["quals"].is_null()) {
            jc.on_condition = convert_expr(join_expr["quals"], src);
          }

          jc.loc = node_loc(rarg, src);
          stmt.join_clauses.push_back(std::move(jc));
        }
      }
    }

    if (!stmt.from_tables.empty()) {
      stmt.from_table = stmt.from_tables[0].table_name;
      stmt.from_alias = stmt.from_tables[0].alias;
    }
  }

  // WHERE clause
  if (node.contains("whereClause") && !node["whereClause"].is_null()) {
    stmt.where_clause = convert_expr(node["whereClause"], src);
  }

  // GROUP BY
  if (node.contains("groupClause")) {
    for (const auto& g : node["groupClause"]) {
      stmt.group_by.push_back(convert_expr(g, src));
    }
  }

  // HAVING
  if (node.contains("havingClause") && !node["havingClause"].is_null()) {
    stmt.having = convert_expr(node["havingClause"], src);
  }

  // ORDER BY
  if (node.contains("sortClause") && !node["sortClause"].is_null()) {
    for (const auto& sort_item : node["sortClause"]) {
      if (sort_item.contains("SortBy")) {
        const auto& sb = sort_item["SortBy"];
        ast::OrderByItem item;
        item.expr = convert_expr(sb["node"], src);
        // sortby_dir: pg_query uses "SORTBY_DESC" for descending.
        // ("SORTBY_DIR_DESC" was a legacy format; "SORTBY_DESC" is current.)
        bool descending = false;
        if (sb.contains("sortby_dir")) {
          const auto& dir = sb["sortby_dir"];
          if (dir.is_string()) {
            const auto& s = dir.get<std::string>();
            descending = (s == "SORTBY_DESC" || s == "SORTBY_DIR_DESC");
          } else if (dir.is_number()) {
            descending = (dir.get<int>() == 2);  // 2 = SORTBY_DIR_DESC
          }
        }
        item.descending = descending;
        // libpg_query's SortBy node often has no `location` field; fall
        // back to the inner expression's location so analyzer diagnostics
        // about ORDER BY items always carry a source position.
        item.loc = node_loc(sb, src);
        if (item.loc.line == -1) item.loc = ast::loc_of(item.expr);
        stmt.order_by.push_back(std::move(item));
      }
    }
  }

  // LIMIT
  if (node.contains("limitCount") && !node["limitCount"].is_null()) {
    const auto& lc = node["limitCount"];
    if (lc.contains("A_Const")) {
      if (auto limit_value = parse_numeric_const_value(lc["A_Const"]);
          limit_value.has_value()) {
        stmt.limit = static_cast<int>(*limit_value);
        stmt.limit_loc = node_loc(lc["A_Const"], src);
      }
    }
  }

  return stmt;
}

// --- CREATE TABLE/STREAM ---

ast::CreateStreamStmt convert_create_stmt(const json& node,
                                          const SourceText* src) {
  ast::CreateStreamStmt stmt;
  stmt.name = node["relation"]["relname"].get<std::string>();
  stmt.loc = node_loc(node["relation"], src);

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
        // Check for PRIMARY KEY constraint
        if (elt["ColumnDef"].contains("constraints")) {
          for (const auto& constraint : elt["ColumnDef"]["constraints"]) {
            if (constraint.contains("Constraint")) {
              std::string contype = constraint["Constraint"].value("contype", "");
              if (contype == "CONSTR_PRIMARY") {
                col.primary_key = true;
              }
            }
          }
        }
        col.loc = node_loc(elt["ColumnDef"], src);
        stmt.columns.push_back(std::move(col));
      }
    }
  }

  return stmt;
}

// --- CREATE MATERIALIZED VIEW ---

ast::CreateViewStmt convert_create_table_as_stmt(const json& node,
                                                 const SourceText* src) {
  ast::CreateViewStmt stmt;
  stmt.name = node["into"]["rel"]["relname"].get<std::string>();
  stmt.loc = node_loc(node["into"]["rel"], src);

  std::string objtype = node.value("objtype", "");
  stmt.materialized = (objtype == "OBJECT_MATVIEW");

  stmt.query = convert_select_stmt(node["query"]["SelectStmt"], src);

  return stmt;
}

// --- CREATE VIEW ---

ast::CreateViewStmt convert_view_stmt(const json& node,
                                      const SourceText* src) {
  ast::CreateViewStmt stmt;
  stmt.name = node["view"]["relname"].get<std::string>();
  stmt.loc = node_loc(node["view"], src);
  stmt.materialized = false;
  stmt.query = convert_select_stmt(node["query"]["SelectStmt"], src);
  return stmt;
}

// --- INSERT ---

ast::InsertStmt convert_insert_stmt(const json& node, const SourceText* src) {
  ast::InsertStmt stmt;
  stmt.table_name = node["relation"]["relname"].get<std::string>();
  stmt.loc = node_loc(node["relation"], src);

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
        stmt.values.push_back(convert_expr(item, src));
      }
    }
  }

  return stmt;
}

// --- DROP ---

ast::DropStmt convert_drop_stmt(const json& node, const SourceText* src) {
  ast::DropStmt stmt;
  stmt.loc = node_loc(node, src);

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

// --- DELETE ---

ast::DeleteStmt convert_delete_stmt(const json& node, const SourceText* src) {
  ast::DeleteStmt stmt;
  stmt.table_name = node["relation"]["relname"].get<std::string>();
  stmt.loc = node_loc(node["relation"], src);
  if (node.contains("whereClause") && !node["whereClause"].is_null()) {
    stmt.where_clause = convert_expr(node["whereClause"], src);
  }
  return stmt;
}

// Patch a Statement's top-level `loc` to `fallback` when libpg_query
// didn't give the inner node its own location. Used for statement kinds
// like DropStmt that have no `location` field of their own; the parent
// wrapper's `stmt_location` is the next-best source position.
void patch_stmt_loc(ast::Statement& stmt, ast::SourceLocation fallback) {
  if (fallback.line == -1) return;
  std::visit(
      [&](auto& s) {
        if (s.loc.line == -1) s.loc = fallback;
      },
      stmt);
}

ast::Statement convert_parse_tree_impl(const std::string& json_str,
                                       const SourceText* src) {
  auto root = json::parse(json_str);

  if (!root.contains("stmts") || root["stmts"].empty()) {
    // Empty SQL (or a parse tree with no statements) — no source location
    // to attach.
    throw ConverterError("empty parse tree", {});
  }

  const auto& stmt_top = root["stmts"][0];
  const auto& stmt_wrapper = stmt_top["stmt"];

  // libpg_query attaches `stmt_location` (byte offset to start of the
  // statement) to the wrapper. Use it as a fallback for statement kinds
  // whose inner node has no `location` field. The field is omitted (or
  // null/empty) when the statement starts at offset 0 — treat that as 0
  // explicitly so single-statement parses still get (1, 1).
  ast::SourceLocation stmt_loc;
  if (src) {
    int byte_off = 0;
    if (stmt_top.contains("stmt_location") &&
        stmt_top["stmt_location"].is_number_integer()) {
      byte_off = stmt_top["stmt_location"].get<int>();
    }
    stmt_loc = compute_location(*src, byte_off);
  }

  ast::Statement result;
  if (stmt_wrapper.contains("SelectStmt")) {
    result = convert_select_stmt(stmt_wrapper["SelectStmt"], src);
  } else if (stmt_wrapper.contains("CreateStmt")) {
    result = convert_create_stmt(stmt_wrapper["CreateStmt"], src);
  } else if (stmt_wrapper.contains("CreateTableAsStmt")) {
    result = convert_create_table_as_stmt(stmt_wrapper["CreateTableAsStmt"],
                                          src);
  } else if (stmt_wrapper.contains("ViewStmt")) {
    result = convert_view_stmt(stmt_wrapper["ViewStmt"], src);
  } else if (stmt_wrapper.contains("InsertStmt")) {
    result = convert_insert_stmt(stmt_wrapper["InsertStmt"], src);
  } else if (stmt_wrapper.contains("DropStmt")) {
    result = convert_drop_stmt(stmt_wrapper["DropStmt"], src);
  } else if (stmt_wrapper.contains("DeleteStmt")) {
    result = convert_delete_stmt(stmt_wrapper["DeleteStmt"], src);
  } else {
    // Unhandled top-level statement kind (e.g. UPDATE, COPY, ...).
    // Use the wrapper's stmt_location so the error points at the start
    // of the offending statement.
    throw ConverterError("unsupported statement type", stmt_loc);
  }

  patch_stmt_loc(result, stmt_loc);
  return result;
}

}  // namespace

ast::Statement convert_parse_tree(const std::string& json_str) {
  return convert_parse_tree_impl(json_str, nullptr);
}

ast::Statement convert_parse_tree(const std::string& source_sql,
                                  const std::string& json_str) {
  SourceText src = make_source_text(source_sql);
  return convert_parse_tree_impl(json_str, &src);
}

}  // namespace rtbot_sql::parser
