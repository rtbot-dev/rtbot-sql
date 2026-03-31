#pragma once

#include <optional>
#include <string>
#include <vector>

#include "rtbot_sql/analyzer/scope.h"
#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/compiler/select_compiler.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

// --- GROUP BY item classification ---

enum class GroupByItemKind { PERSISTENT_KEY, SEGMENT_EXPRESSION };

struct GroupByItem {
  GroupByItemKind kind;
  int key_index;         // valid when PERSISTENT_KEY
  std::string key_name;  // column name when PERSISTENT_KEY
};

struct GroupByClassification {
  std::vector<GroupByItem> items;

  bool has_persistent_keys() const;
  bool has_segment_expressions() const;
  int persistent_key_count() const;
  int segment_expression_count() const;
};

// Classify each GROUP BY item as either a persistent partition key (bare
// ColumnRef resolving in scope) or a segment expression (anything else).
GroupByClassification classify_group_by(
    const std::vector<parser::ast::Expr>& group_by,
    const analyzer::Scope& scope);

// Compile a GROUP BY query into a KeyedPipeline + Prototype structure.
// The field map has the key column(s) at the start, followed by non-key items.
// num_input_cols: number of columns in the input stream (required for composite
// GROUP BY to augment the vector with a hash key; 0 = single-key only mode).
SelectResult compile_group_by(
    const std::vector<parser::ast::SelectItem>& select_list,
    const std::vector<parser::ast::Expr>& group_by,
    const std::optional<parser::ast::Expr>& having,
    const Endpoint& input_endpoint,
    const analyzer::Scope& scope,
    GraphBuilder& builder,
    int num_input_cols = 0);

}  // namespace rtbot_sql::compiler
