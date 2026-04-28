// Direct unit tests for analyzer::try_fold.
//
// This folder is load-bearing for the POWER / TIMESHIFT / RESAMPLE_CONSTANT
// analyzer rules — those checks delegate to try_fold to decide whether an
// argument expression will eventually evaluate to a constant. These tests
// pin the folder's behavior independently of the analyzer's integration so
// regressions show up immediately.

#include "rtbot_sql/analyzer/const_fold.h"

#include <cmath>
#include <memory>

#include <gtest/gtest.h>

#include "rtbot_sql/parser/ast.h"

namespace rtbot_sql::analyzer {
namespace {

using parser::ast::ArrayLiteral;
using parser::ast::BinaryExpr;
using parser::ast::ColumnRef;
using parser::ast::Constant;
using parser::ast::Expr;
using parser::ast::FuncCall;
using parser::ast::StringConstant;

// AST helpers
Expr c(double v) { return Constant{v}; }
Expr col(const std::string& name) { return ColumnRef{"", name}; }
Expr str(const std::string& s) { return StringConstant{s}; }

Expr bin(const std::string& op, Expr l, Expr r) {
  auto b = std::make_unique<BinaryExpr>();
  b->op = op;
  b->left = std::move(l);
  b->right = std::move(r);
  return b;
}

Expr fn(const std::string& name, Expr arg) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args.push_back(std::move(arg));
  return f;
}

Expr fn0(const std::string& name) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  return f;
}

Expr fn2(const std::string& name, Expr a, Expr b) {
  auto f = std::make_unique<FuncCall>();
  f->name = name;
  f->args.push_back(std::move(a));
  f->args.push_back(std::move(b));
  return f;
}

// --- Literal Constants ---

TEST(ConstFoldTest, LiteralIntegerConstantFolds) {
  EXPECT_EQ(try_fold(c(42.0)).value(), 42.0);
}

TEST(ConstFoldTest, LiteralNegativeConstantFolds) {
  EXPECT_EQ(try_fold(c(-3.5)).value(), -3.5);
}

TEST(ConstFoldTest, LiteralZeroFolds) {
  EXPECT_EQ(try_fold(c(0.0)).value(), 0.0);
}

// --- Arithmetic ---

TEST(ConstFoldTest, AdditionFolds) {
  EXPECT_EQ(try_fold(bin("+", c(2.0), c(3.0))).value(), 5.0);
}

TEST(ConstFoldTest, SubtractionFolds) {
  EXPECT_EQ(try_fold(bin("-", c(10.0), c(4.0))).value(), 6.0);
}

TEST(ConstFoldTest, MultiplicationFolds) {
  EXPECT_EQ(try_fold(bin("*", c(6.0), c(7.0))).value(), 42.0);
}

TEST(ConstFoldTest, DivisionFolds) {
  EXPECT_EQ(try_fold(bin("/", c(20.0), c(4.0))).value(), 5.0);
}

TEST(ConstFoldTest, NestedArithmeticFolds) {
  // (2 + 3) * (10 - 4) = 30
  EXPECT_EQ(
      try_fold(bin("*", bin("+", c(2.0), c(3.0)), bin("-", c(10.0), c(4.0))))
          .value(),
      30.0);
}

TEST(ConstFoldTest, DeeplyNestedArithmeticFolds) {
  // ((2 * 3) + 4) - (8 / 2) = 6
  EXPECT_EQ(
      try_fold(bin("-", bin("+", bin("*", c(2.0), c(3.0)), c(4.0)),
                  bin("/", c(8.0), c(2.0))))
          .value(),
      6.0);
}

// Division by zero is undefined — folder declines rather than producing inf
// or NaN, so callers get a clean "this isn't constant-foldable" signal.
TEST(ConstFoldTest, DivisionByZeroDoesNotFold) {
  EXPECT_FALSE(try_fold(bin("/", c(1.0), c(0.0))).has_value());
}

// --- Unary math functions ---

TEST(ConstFoldTest, AbsFolds) {
  EXPECT_EQ(try_fold(fn("ABS", c(-5.0))).value(), 5.0);
  EXPECT_EQ(try_fold(fn("abs", c(-5.0))).value(), 5.0);  // case-insensitive
}

TEST(ConstFoldTest, FloorFolds) {
  EXPECT_EQ(try_fold(fn("FLOOR", c(3.7))).value(), 3.0);
}

TEST(ConstFoldTest, CeilFolds) {
  EXPECT_EQ(try_fold(fn("CEIL", c(3.2))).value(), 4.0);
  EXPECT_EQ(try_fold(fn("CEILING", c(3.2))).value(), 4.0);
}

TEST(ConstFoldTest, RoundFolds) {
  EXPECT_EQ(try_fold(fn("ROUND", c(3.5))).value(), 4.0);
}

TEST(ConstFoldTest, LogAndLnFold) {
  // LOG and LN are aliases for natural log
  EXPECT_DOUBLE_EQ(try_fold(fn("LN", c(std::exp(1.0)))).value(), 1.0);
  EXPECT_DOUBLE_EQ(try_fold(fn("LOG", c(std::exp(1.0)))).value(), 1.0);
}

TEST(ConstFoldTest, Log10Folds) {
  EXPECT_DOUBLE_EQ(try_fold(fn("LOG10", c(100.0))).value(), 2.0);
}

TEST(ConstFoldTest, ExpFolds) {
  EXPECT_DOUBLE_EQ(try_fold(fn("EXP", c(0.0))).value(), 1.0);
}

TEST(ConstFoldTest, TrigFold) {
  EXPECT_DOUBLE_EQ(try_fold(fn("SIN", c(0.0))).value(), 0.0);
  EXPECT_DOUBLE_EQ(try_fold(fn("COS", c(0.0))).value(), 1.0);
  EXPECT_DOUBLE_EQ(try_fold(fn("TAN", c(0.0))).value(), 0.0);
}

TEST(ConstFoldTest, SignFolds) {
  EXPECT_EQ(try_fold(fn("SIGN", c(5.0))).value(), 1.0);
  EXPECT_EQ(try_fold(fn("SIGN", c(-3.0))).value(), -1.0);
  EXPECT_EQ(try_fold(fn("SIGN", c(0.0))).value(), 0.0);
}

TEST(ConstFoldTest, SqrtFolds) {
  EXPECT_DOUBLE_EQ(try_fold(fn("SQRT", c(16.0))).value(), 4.0);
}

// --- Combinations: math function applied to an arithmetic expression ---

TEST(ConstFoldTest, MathOfArithmeticFolds) {
  // ABS(2 - 7) = 5
  EXPECT_EQ(try_fold(fn("ABS", bin("-", c(2.0), c(7.0)))).value(), 5.0);
}

TEST(ConstFoldTest, ArithmeticOfMathFolds) {
  // ABS(-3) * 2 = 6
  EXPECT_EQ(try_fold(bin("*", fn("ABS", c(-3.0)), c(2.0))).value(), 6.0);
}

// --- Non-foldable cases ---

TEST(ConstFoldTest, ColumnRefDoesNotFold) {
  EXPECT_FALSE(try_fold(col("price")).has_value());
}

TEST(ConstFoldTest, StringConstantDoesNotFold) {
  EXPECT_FALSE(try_fold(str("hello")).has_value());
}

TEST(ConstFoldTest, ArrayLiteralDoesNotFold) {
  Expr arr = ArrayLiteral{{1.0, 2.0, 3.0}};
  EXPECT_FALSE(try_fold(arr).has_value());
}

TEST(ConstFoldTest, ArithmeticWithColumnDoesNotFold) {
  // 2 + price — right side is non-constant
  EXPECT_FALSE(try_fold(bin("+", c(2.0), col("price"))).has_value());
}

TEST(ConstFoldTest, UnknownBinaryOperatorDoesNotFold) {
  // The folder only handles + - * /
  EXPECT_FALSE(try_fold(bin("%", c(10.0), c(3.0))).has_value());
}

TEST(ConstFoldTest, NonMathFunctionCallDoesNotFold) {
  // POWER takes 2 args; the folder only handles 1-arg math functions.
  EXPECT_FALSE(try_fold(fn2("POWER", c(2.0), c(3.0))).has_value());
}

TEST(ConstFoldTest, ZeroArgFunctionDoesNotFold) {
  EXPECT_FALSE(try_fold(fn0("TS")).has_value());
}

TEST(ConstFoldTest, MathFunctionWithNonConstantArgDoesNotFold) {
  EXPECT_FALSE(try_fold(fn("ABS", col("price"))).has_value());
}

TEST(ConstFoldTest, MathFunctionWithUnfoldableArithmeticDoesNotFold) {
  // ABS(2 + price) — inner doesn't fold
  EXPECT_FALSE(
      try_fold(fn("ABS", bin("+", c(2.0), col("price")))).has_value());
}

}  // namespace
}  // namespace rtbot_sql::analyzer
