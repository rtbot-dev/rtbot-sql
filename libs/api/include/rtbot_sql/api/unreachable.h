#pragma once

#include <cstdlib>

namespace rtbot_sql {

[[noreturn]] inline void unreachable() noexcept {
#if defined(__clang__) || defined(__GNUC__)
  __builtin_unreachable();
#elif defined(_MSC_VER)
  __assume(false);
#endif
  std::abort();
}

}  // namespace rtbot_sql
