#include "rtbot_sql/parser/parser.h"

#include "rtbot_sql/parser/source_text.h"

namespace rtbot_sql::parser {

ParseResult parse(const std::string& sql) {
  ParseResult pr{};
  pr.result = pg_query_parse_protobuf(sql.c_str());

  if (pr.result.error) {
    ParseError err;
    err.message = pr.result.error->message ? pr.result.error->message
                                           : "unknown parse error";
    SourceText src = make_source_text(sql);
    // libpg_query's `cursorpos` follows PostgreSQL convention: 1-based
    // character position (0 means "no position available"). compute_location
    // takes a 0-based byte offset (matching libpg_query's `.location` field
    // on AST nodes), so we convert here.
    int cursorpos = pr.result.error->cursorpos;
    int byte_offset = cursorpos > 0 ? cursorpos - 1 : -1;
    err.loc = compute_location(src, byte_offset);
    pr.errors.push_back(std::move(err));
  }

  return pr;
}

void free_result(ParseResult& r) {
  pg_query_free_protobuf_parse_result(r.result);
  r.result = {};
  r.errors.clear();
}

}  // namespace rtbot_sql::parser
