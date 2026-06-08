#pragma once

#include <memory>
#include <optional>
#include <string>
#include <type_traits>
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

// Source location of an AST node within the original SQL string.
//
// `line` / `column` mark the start of the token (1-based).
// `end_line` / `end_column` mark the position immediately after the last
// character of the token (1-based, exclusive — like editor selections).
// All fields default to -1 ("not available", e.g. for synthetic nodes).
struct SourceLocation {
  int line = -1;
  int column = -1;
  int end_line = -1;
  int end_column = -1;
};

// Forward declarations
struct BinaryExpr;
struct ComparisonExpr;
struct FuncCall;
struct LogicalExpr;
struct NotExpr;
struct BetweenExpr;
struct CaseExpr;

struct ColumnRef {
  std::string table_alias;
  std::string column_name;
  SourceLocation loc;
};

struct Constant {
  double value;
  SourceLocation loc;
};

struct StringConstant {
  std::string value;
  SourceLocation loc;
};

struct ArrayLiteral {
  std::vector<double> values;
  SourceLocation loc;
};

using Expr = std::variant<
    ColumnRef,
    Constant,
    StringConstant,
    ArrayLiteral,
    std::unique_ptr<BinaryExpr>,
    std::unique_ptr<ComparisonExpr>,
    std::unique_ptr<FuncCall>,
    std::unique_ptr<LogicalExpr>,
    std::unique_ptr<NotExpr>,
    std::unique_ptr<BetweenExpr>,
    std::unique_ptr<CaseExpr>>;

struct BinaryExpr {
  std::string op;  // +, -, *, /
  Expr left;
  Expr right;
  SourceLocation loc;
};

struct ComparisonExpr {
  std::string op;  // >, <, >=, <=, =, !=
  Expr left;
  Expr right;
  SourceLocation loc;
};

struct FuncCall {
  std::string name;
  std::vector<Expr> args;
  SourceLocation loc;
};

struct LogicalExpr {
  std::string op;  // AND, OR
  Expr left;
  Expr right;
  SourceLocation loc;
};

struct NotExpr {
  Expr operand;
  SourceLocation loc;
};

struct BetweenExpr {
  Expr expr;
  Expr low;
  Expr high;
  SourceLocation loc;
};

struct CaseWhenClause {
  Expr condition;
  Expr result;
  SourceLocation loc;
};

struct CaseExpr {
  std::vector<CaseWhenClause> when_clauses;
  std::optional<Expr> else_result;
  SourceLocation loc;
};

struct SelectItem {
  Expr expr;
  std::optional<std::string> alias;
  SourceLocation loc;
};

struct JoinClause {
  std::string join_type;    // "INNER", "LEFT", "RIGHT"
  std::string table_name;
  std::string table_alias;
  std::optional<Expr> on_condition;
  SourceLocation loc;
};

struct FromSource {
  std::string table_name;
  std::string alias;
  SourceLocation loc;
};

struct OrderByItem {
  Expr expr;
  bool descending = false;  // ASC=false, DESC=true
  SourceLocation loc;
};

struct SelectStmt {
  std::vector<SelectItem> select_list;
  std::string from_table;
  std::string from_alias;
  std::vector<FromSource> from_tables;
  std::vector<JoinClause> join_clauses;
  std::optional<Expr> where_clause;
  std::vector<Expr> group_by;
  std::optional<Expr> having;
  std::vector<OrderByItem> order_by;
  std::optional<int> limit;
  SourceLocation limit_loc;  // location of the LIMIT value, when present
  SourceLocation loc;
};

struct ColumnDefAST {
  std::string name;
  std::string type_name;
  bool primary_key = false;
  SourceLocation loc;
};

enum class SourceType { SCALAR, CSV_BURST, UNKNOWN };

// The `TYPE <scalar|csv_burst>` clause. `raw` preserves the user-written
// identifier verbatim — used by the analyzer when emitting "unknown TYPE"
// diagnostics. `value` is UNKNOWN when `raw` is neither "scalar" nor
// "csv_burst" (case-insensitive), so the analyzer can flag it. The span
// covers the value identifier (inclusive start, exclusive end, 1-based).
struct SourceTypeClause {
  SourceType value = SourceType::SCALAR;
  std::string raw;
  int line = -1;
  int column = -1;
  int end_line = -1;
  int end_column = -1;
};

// The `WINDOW <int>` clause. Span over the digits.
struct SourceWindowClause {
  int value = 0;
  int line = -1;
  int column = -1;
  int end_line = -1;
  int end_column = -1;
};

// Metadata for `FROM "..."` on CREATE STREAM. `name` is the unquoted source
// identifier; `line`/`column`/`end_line`/`end_column` span the `"..."`
// token (inclusive start, exclusive end, 1-based).
//
// `type` and `window` are present only when the user wrote the respective
// clauses. Validation of their contents — unknown TYPE values, csv_burst
// requiring WINDOW, WINDOW being valid only with csv_burst — lives in the
// analyzer (libs/analyzer/src/ddl_analyzer.cpp::analyze_create_stream).
struct Source {
  std::string name;
  int line = -1;
  int column = -1;
  int end_line = -1;
  int end_column = -1;
  std::optional<SourceTypeClause> type;
  std::optional<SourceWindowClause> window;

  SourceType effective_type() const {
    return type.has_value() ? type->value : SourceType::SCALAR;
  }
};

struct CreateStreamStmt {
  std::string name;
  std::vector<ColumnDefAST> columns;
  std::optional<Source> source;
  SourceLocation loc;
};

struct CreateViewStmt {
  std::string name;
  bool materialized;
  SelectStmt query;
  SourceLocation loc;
};

struct InsertStmt {
  std::string table_name;
  std::vector<std::string> columns;
  std::vector<Expr> values;
  SourceLocation loc;
};

struct DropStmt {
  std::string name;
  std::string entity_type;  // STREAM, VIEW, TABLE, etc.
  bool if_exists;
  SourceLocation loc;
};

struct DeleteStmt {
  std::string table_name;
  std::optional<Expr> where_clause;
  SourceLocation loc;
};

using Statement = std::variant<
    SelectStmt,
    CreateStreamStmt,
    CreateViewStmt,
    InsertStmt,
    DropStmt,
    DeleteStmt>;

namespace detail {
template <typename T>
struct is_unique_ptr : std::false_type {};
template <typename T>
struct is_unique_ptr<std::unique_ptr<T>> : std::true_type {};
}  // namespace detail

// Returns the SourceLocation of any Expr, transparent across the variant.
inline SourceLocation loc_of(const Expr& e) {
  return std::visit(
      [](const auto& v) -> SourceLocation {
        using T = std::decay_t<decltype(v)>;
        if constexpr (detail::is_unique_ptr<T>::value) {
          return v ? v->loc : SourceLocation{};
        } else {
          return v.loc;
        }
      },
      e);
}

// Returns the SourceLocation of any Statement.
inline SourceLocation loc_of(const Statement& s) {
  return std::visit(
      [](const auto& v) -> SourceLocation { return v.loc; }, s);
}

}  // namespace rtbot_sql::parser::ast
