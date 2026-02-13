#pragma once

#include <map>
#include <string>

#include "rtbot_sql/compiler/graph_builder.h"
#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::compiler {

// Canonical string representation of an expression AST node.
// Case-insensitive for function names. Used as cache key.
std::string canonicalize(const parser::ast::Expr& expr);

// Cache mapping canonical expression strings to compiled endpoints.
// Only caches Endpoint results (constants don't produce operators).
class ExprCache {
 public:
  const Endpoint* lookup(const parser::ast::Expr& expr) const {
    auto key = canonicalize(expr);
    auto it = cache_.find(key);
    if (it != cache_.end()) return &it->second;
    return nullptr;
  }

  void store(const parser::ast::Expr& expr, const Endpoint& ep) {
    cache_[canonicalize(expr)] = ep;
  }

 private:
  std::map<std::string, Endpoint> cache_;
};

}  // namespace rtbot_sql::compiler
