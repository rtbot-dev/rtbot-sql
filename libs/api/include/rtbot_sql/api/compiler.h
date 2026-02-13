#pragma once

#include <string>

#include "rtbot_sql/api/types.h"

namespace rtbot_sql::api {

// Compile a SQL string into a CompilationResult.
// The catalog snapshot provides schema information for name resolution.
CompilationResult compile_sql(const std::string& sql,
                              const CatalogSnapshot& catalog);

}  // namespace rtbot_sql::api
