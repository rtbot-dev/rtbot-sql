#include "rtbot_sql/parser/parser.h"

namespace rtbot_sql::parser {

ParseResult parse(const std::string& sql) {
  ParseResult pr{};
  pr.result = pg_query_parse_protobuf(sql.c_str());

  if (pr.result.error) {
    std::string msg = pr.result.error->message ? pr.result.error->message : "unknown parse error";
    pr.errors.push_back(std::move(msg));
  }

  return pr;
}

void free_result(ParseResult& r) {
  pg_query_free_protobuf_parse_result(r.result);
  r.result = {};
  r.errors.clear();
}

}  // namespace rtbot_sql::parser
