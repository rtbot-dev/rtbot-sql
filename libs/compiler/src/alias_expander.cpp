#include "rtbot_sql/compiler/alias_expander.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <stdexcept>
#include <string>

namespace rtbot_sql::compiler {

using namespace parser::ast;

// ─── deep_clone ─────────────────────────────────────────────────────────────

Expr deep_clone(const Expr& expr) {
  return std::visit(
      [](const auto& v) -> Expr {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, ColumnRef> ||
                      std::is_same_v<T, Constant> ||
                      std::is_same_v<T, ArrayLiteral>) {
          return v;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BinaryExpr>>) {
          auto p = std::make_unique<BinaryExpr>();
          p->op = v->op;
          p->left = deep_clone(v->left);
          p->right = deep_clone(v->right);
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>>) {
          auto p = std::make_unique<ComparisonExpr>();
          p->op = v->op;
          p->left = deep_clone(v->left);
          p->right = deep_clone(v->right);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          auto p = std::make_unique<FuncCall>();
          p->name = v->name;
          for (const auto& arg : v->args) {
            p->args.push_back(deep_clone(arg));
          }
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<LogicalExpr>>) {
          auto p = std::make_unique<LogicalExpr>();
          p->op = v->op;
          p->left = deep_clone(v->left);
          p->right = deep_clone(v->right);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          auto p = std::make_unique<NotExpr>();
          p->operand = deep_clone(v->operand);
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BetweenExpr>>) {
          auto p = std::make_unique<BetweenExpr>();
          p->expr = deep_clone(v->expr);
          p->low = deep_clone(v->low);
          p->high = deep_clone(v->high);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          auto p = std::make_unique<CaseExpr>();
          for (const auto& wc : v->when_clauses) {
            p->when_clauses.push_back(
                {deep_clone(wc.condition), deep_clone(wc.result)});
          }
          if (v->else_result.has_value()) {
            p->else_result = deep_clone(*v->else_result);
          }
          return p;

        } else {
          throw std::runtime_error("deep_clone: unhandled Expr variant");
        }
      },
      expr);
}

// ─── expand_aliases ──────────────────────────────────────────────────────────

Expr expand_aliases(const Expr& expr, const AliasMap& alias_map) {
  return std::visit(
      [&](const auto& v) -> Expr {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, ColumnRef>) {
          auto it = alias_map.find(v.column_name);
          if (it != alias_map.end()) {
            return deep_clone(it->second);
          }
          return v;

        } else if constexpr (std::is_same_v<T, Constant> ||
                              std::is_same_v<T, ArrayLiteral>) {
          return v;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BinaryExpr>>) {
          auto p = std::make_unique<BinaryExpr>();
          p->op = v->op;
          p->left = expand_aliases(v->left, alias_map);
          p->right = expand_aliases(v->right, alias_map);
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>>) {
          auto p = std::make_unique<ComparisonExpr>();
          p->op = v->op;
          p->left = expand_aliases(v->left, alias_map);
          p->right = expand_aliases(v->right, alias_map);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          auto p = std::make_unique<FuncCall>();
          p->name = v->name;
          for (const auto& arg : v->args) {
            p->args.push_back(expand_aliases(arg, alias_map));
          }
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<LogicalExpr>>) {
          auto p = std::make_unique<LogicalExpr>();
          p->op = v->op;
          p->left = expand_aliases(v->left, alias_map);
          p->right = expand_aliases(v->right, alias_map);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          auto p = std::make_unique<NotExpr>();
          p->operand = expand_aliases(v->operand, alias_map);
          return p;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BetweenExpr>>) {
          auto p = std::make_unique<BetweenExpr>();
          p->expr = expand_aliases(v->expr, alias_map);
          p->low = expand_aliases(v->low, alias_map);
          p->high = expand_aliases(v->high, alias_map);
          return p;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          auto p = std::make_unique<CaseExpr>();
          for (const auto& wc : v->when_clauses) {
            p->when_clauses.push_back({expand_aliases(wc.condition, alias_map),
                                       expand_aliases(wc.result, alias_map)});
          }
          if (v->else_result.has_value()) {
            p->else_result = expand_aliases(*v->else_result, alias_map);
          }
          return p;

        } else {
          throw std::runtime_error("expand_aliases: unhandled Expr variant");
        }
      },
      expr);
}

// ─── build_alias_map ─────────────────────────────────────────────────────────

AliasMap build_alias_map(const std::vector<SelectItem>& select_list) {
  AliasMap alias_map;
  for (const auto& item : select_list) {
    if (item.alias.has_value()) {
      alias_map[*item.alias] = expand_aliases(item.expr, alias_map);
    }
  }
  return alias_map;
}

// ─── expr_has_aggregate ──────────────────────────────────────────────────────

static bool is_aggregate_name(const std::string& name) {
  std::string upper = name;
  std::transform(upper.begin(), upper.end(), upper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return upper == "SUM" || upper == "COUNT" || upper == "AVG" ||
         upper == "MIN" || upper == "MAX";
}

bool expr_has_aggregate(const Expr& expr) {
  return std::visit(
      [](const auto& v) -> bool {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, ColumnRef> ||
                      std::is_same_v<T, Constant> ||
                      std::is_same_v<T, ArrayLiteral>) {
          return false;

        } else if constexpr (std::is_same_v<T, std::unique_ptr<FuncCall>>) {
          if (is_aggregate_name(v->name)) return true;
          for (const auto& arg : v->args) {
            if (expr_has_aggregate(arg)) return true;
          }
          return false;

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BinaryExpr>>) {
          return expr_has_aggregate(v->left) || expr_has_aggregate(v->right);

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<ComparisonExpr>>) {
          return expr_has_aggregate(v->left) || expr_has_aggregate(v->right);

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<LogicalExpr>>) {
          return expr_has_aggregate(v->left) || expr_has_aggregate(v->right);

        } else if constexpr (std::is_same_v<T, std::unique_ptr<NotExpr>>) {
          return expr_has_aggregate(v->operand);

        } else if constexpr (std::is_same_v<T,
                                             std::unique_ptr<BetweenExpr>>) {
          return expr_has_aggregate(v->expr) || expr_has_aggregate(v->low) ||
                 expr_has_aggregate(v->high);

        } else if constexpr (std::is_same_v<T, std::unique_ptr<CaseExpr>>) {
          for (const auto& wc : v->when_clauses) {
            if (expr_has_aggregate(wc.condition) ||
                expr_has_aggregate(wc.result))
              return true;
          }
          if (v->else_result.has_value()) {
            return expr_has_aggregate(*v->else_result);
          }
          return false;

        } else {
          return false;
        }
      },
      expr);
}

}  // namespace rtbot_sql::compiler
