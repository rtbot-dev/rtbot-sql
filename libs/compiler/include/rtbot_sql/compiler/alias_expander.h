#pragma once
#include <map>
#include <string>
#include <vector>
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

using AliasMap = std::map<std::string, parser::ast::Expr>;

// Deep-clone an Expr (required because Expr contains unique_ptr alternatives).
parser::ast::Expr deep_clone(const parser::ast::Expr& expr);

// Build alias map from the SELECT list in definition order.
// Each alias resolves to its defining expression with earlier aliases already
// expanded, so forward references within the SELECT list work automatically.
AliasMap build_alias_map(
    const std::vector<parser::ast::SelectItem>& select_list);

// Return a new Expr with every ColumnRef whose name is an alias key substituted
// by a deep clone of the mapped defining expression. Non-alias refs are cloned
// as-is.
parser::ast::Expr expand_aliases(const parser::ast::Expr& expr,
                                 const AliasMap& alias_map);

// True if expr (after expansion) contains a top-level or nested aggregate call
// (SUM, COUNT, AVG, MIN, MAX). Used to reject aggregate aliases in WHERE.
bool expr_has_aggregate(const parser::ast::Expr& expr);

}  // namespace rtbot_sql::compiler
