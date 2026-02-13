#include "rtbot_sql/compiler/expr_cache.h"

#include <algorithm>
#include <sstream>

namespace rtbot_sql::compiler {

namespace {

void canonicalize_impl(const parser::ast::Expr& expr, std::ostringstream& os) {
  using namespace parser::ast;

  if (auto* col = std::get_if<ColumnRef>(&expr)) {
    if (!col->table_alias.empty()) {
      os << col->table_alias << ".";
    }
    os << col->column_name;
    return;
  }

  if (auto* c = std::get_if<Constant>(&expr)) {
    os << c->value;
    return;
  }

  if (auto* arr = std::get_if<ArrayLiteral>(&expr)) {
    os << "ARRAY[";
    for (size_t i = 0; i < arr->values.size(); ++i) {
      if (i > 0) os << ",";
      os << arr->values[i];
    }
    os << "]";
    return;
  }

  if (auto* bin = std::get_if<std::unique_ptr<BinaryExpr>>(&expr)) {
    os << "(";
    canonicalize_impl((*bin)->left, os);
    os << (*bin)->op;
    canonicalize_impl((*bin)->right, os);
    os << ")";
    return;
  }

  if (auto* cmp = std::get_if<std::unique_ptr<ComparisonExpr>>(&expr)) {
    os << "(";
    canonicalize_impl((*cmp)->left, os);
    os << (*cmp)->op;
    canonicalize_impl((*cmp)->right, os);
    os << ")";
    return;
  }

  if (auto* func = std::get_if<std::unique_ptr<FuncCall>>(&expr)) {
    std::string upper = (*func)->name;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    os << upper << "(";
    for (size_t i = 0; i < (*func)->args.size(); ++i) {
      if (i > 0) os << ",";
      canonicalize_impl((*func)->args[i], os);
    }
    os << ")";
    return;
  }

  if (auto* log = std::get_if<std::unique_ptr<LogicalExpr>>(&expr)) {
    os << "(";
    canonicalize_impl((*log)->left, os);
    std::string upper_op = (*log)->op;
    std::transform(upper_op.begin(), upper_op.end(), upper_op.begin(),
                   ::toupper);
    os << " " << upper_op << " ";
    canonicalize_impl((*log)->right, os);
    os << ")";
    return;
  }

  if (auto* not_e = std::get_if<std::unique_ptr<NotExpr>>(&expr)) {
    os << "NOT(";
    canonicalize_impl((*not_e)->operand, os);
    os << ")";
    return;
  }

  if (auto* btw = std::get_if<std::unique_ptr<BetweenExpr>>(&expr)) {
    os << "BETWEEN(";
    canonicalize_impl((*btw)->expr, os);
    os << ",";
    canonicalize_impl((*btw)->low, os);
    os << ",";
    canonicalize_impl((*btw)->high, os);
    os << ")";
    return;
  }

  os << "?";
}

}  // namespace

std::string canonicalize(const parser::ast::Expr& expr) {
  std::ostringstream os;
  canonicalize_impl(expr, os);
  return os.str();
}

}  // namespace rtbot_sql::compiler
