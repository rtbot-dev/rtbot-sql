#include "rtbot_sql/analyzer/analyzer.h"

#include "rtbot_sql/analyzer/ddl_analyzer.h"
#include "rtbot_sql/analyzer/dml_analyzer.h"
#include "rtbot_sql/analyzer/select_analyzer.h"

namespace rtbot_sql::analyzer {

std::vector<Diagnostic> analyze_statement(const parser::ast::Statement& stmt,
                                          const CatalogSnapshot& catalog,
                                          std::string_view sql) {
  DiagnosticBag bag;

  std::visit(
      [&bag, &catalog, sql](const auto& s) {
        using T = std::decay_t<decltype(s)>;
        if constexpr (std::is_same_v<T, parser::ast::SelectStmt>) {
          analyze_select(s, catalog, bag, /*top_level=*/true, sql);
        } else if constexpr (std::is_same_v<T, parser::ast::CreateViewStmt>) {
          analyze_select(s.query, catalog, bag, /*top_level=*/false, sql);
        } else if constexpr (std::is_same_v<T, parser::ast::CreateStreamStmt>) {
          analyze_create_stream(s, catalog, bag);
        } else if constexpr (std::is_same_v<T, parser::ast::InsertStmt>) {
          analyze_insert(s, catalog, bag, sql);
        } else if constexpr (std::is_same_v<T, parser::ast::DropStmt>) {
          analyze_drop(s, catalog, bag);
        } else if constexpr (std::is_same_v<T, parser::ast::DeleteStmt>) {
          analyze_delete(s, catalog, bag, sql);
        }
      },
      stmt);

  return bag.diagnostics();
}

}  // namespace rtbot_sql::analyzer
