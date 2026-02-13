#pragma once

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace rtbot_sql::parser::ast {

enum class StmtType {
  CREATE_STREAM,
  CREATE_VIEW,
  CREATE_MATERIALIZED_VIEW,
  CREATE_TABLE,
  INSERT,
  SELECT,
  SUBSCRIBE,
  DROP,
};

// Forward declarations
struct BinaryExpr;
struct ComparisonExpr;
struct FuncCall;

struct ColumnRef {
  std::string table_alias;
  std::string column_name;
};

struct Constant {
  double value;
};

struct ArrayLiteral {
  std::vector<double> values;
};

using Expr = std::variant<
    ColumnRef,
    Constant,
    ArrayLiteral,
    std::unique_ptr<BinaryExpr>,
    std::unique_ptr<ComparisonExpr>,
    std::unique_ptr<FuncCall>>;

struct BinaryExpr {
  std::string op;  // +, -, *, /
  Expr left;
  Expr right;
};

struct ComparisonExpr {
  std::string op;  // >, <, >=, <=, =, !=
  Expr left;
  Expr right;
};

struct FuncCall {
  std::string name;
  std::vector<Expr> args;
};

struct SelectItem {
  Expr expr;
  std::optional<std::string> alias;
};

struct SelectStmt {
  std::vector<SelectItem> select_list;
  std::string from_table;
  std::string from_alias;
  std::optional<Expr> where_clause;
  std::vector<Expr> group_by;
  std::optional<Expr> having;
};

struct ColumnDefAST {
  std::string name;
  std::string type_name;
};

struct CreateStreamStmt {
  std::string name;
  std::vector<ColumnDefAST> columns;
};

struct CreateViewStmt {
  std::string name;
  bool materialized;
  SelectStmt query;
};

struct InsertStmt {
  std::string table_name;
  std::vector<std::string> columns;
  std::vector<Expr> values;
};

struct DropStmt {
  std::string name;
  std::string entity_type;  // STREAM, VIEW, TABLE, etc.
  bool if_exists;
};

using Statement = std::variant<
    SelectStmt,
    CreateStreamStmt,
    CreateViewStmt,
    InsertStmt,
    DropStmt>;

}  // namespace rtbot_sql::parser::ast
