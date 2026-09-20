// Minimal deterministic test harness for the Shuffle Fabric proof surfaces.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
//
// The harness is deliberately small and dependency-free: tests are proof
// obligations and must be readable as such. A failure aborts the current test
// case so a randomized invariant violation stops at the step that broke it,
// and the reproduction seed is reported.

#pragma once

#include <cstdint>
#include <exception>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <ostream>

#include "shuffle/fabric/digest.hpp"
#include "shuffle/fabric/edge.hpp"
#include "shuffle/fabric/error.hpp"
#include "shuffle/fabric/identity.hpp"

namespace shuffle::fabric {

// Test-only rendering so failures print identities and digests readably.
template <class Tag, class Rep>
std::ostream& operator<<(std::ostream& stream, const Id<Tag, Rep>& id) {
  return stream << id.to_string();
}

inline std::ostream& operator<<(std::ostream& stream, const Digest& digest) {
  return stream << digest.to_hex();
}

inline std::ostream& operator<<(std::ostream& stream, const EdgeKey& key) {
  return stream << key.to_string();
}

}  // namespace shuffle::fabric

namespace shuffle::test {

class TestFailure : public std::exception {
 public:
  explicit TestFailure(std::string message);
  [[nodiscard]] const char* what() const noexcept override;
  [[nodiscard]] const std::string& message() const noexcept { return message_; }

 private:
  std::string message_;
};

using TestBody = void (*)();

struct TestCase {
  std::string suite;
  std::string name;
  TestBody body;
};

void register_test(std::string_view suite, std::string_view name, TestBody body);
[[nodiscard]] const std::vector<TestCase>& all_tests();

struct Registrar {
  Registrar(std::string_view suite, std::string_view name, TestBody body);
};

// Records and throws a TestFailure.
[[noreturn]] void report_failure(const char* file, int line, std::string message);
void require(bool condition, const char* expression, const char* file, int line);

// Seed for this run: fixed by --seed=<u64>, otherwise derived from the clock
// so an unseeded run still reports exactly what to re-run.
[[nodiscard]] std::uint64_t run_seed();
// Stable per-test seed: same run seed and same test name => same stream.
[[nodiscard]] std::uint64_t derive_seed(std::string_view label);

int run_all(int argc, char** argv);

// Reads an environment variable without tripping MSVC's deprecated-getenv
// warning; returns an empty string when the variable is unset.
[[nodiscard]] std::string environment_value(std::string_view name);

template <class T, class = void>
struct is_streamable : std::false_type {};

template <class T>
struct is_streamable<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<const T&>())>>
    : std::true_type {};

template <class T>
[[nodiscard]] std::string describe(const T& value) {
  if constexpr (is_streamable<T>::value) {
    std::ostringstream stream;
    stream << value;
    return stream.str();
  } else {
    return "<unprintable>";
  }
}

template <class A, class B>
void require_eq(const A& lhs, const B& rhs, const char* lhs_text, const char* rhs_text, const char* file, int line) {
  if (!(lhs == rhs)) {
    std::ostringstream stream;
    stream << "expected " << lhs_text << " == " << rhs_text << " but " << describe(lhs) << " != " << describe(rhs);
    report_failure(file, line, stream.str());
  }
}

template <class A, class B>
void require_ne(const A& lhs, const B& rhs, const char* lhs_text, const char* rhs_text, const char* file, int line) {
  if (lhs == rhs) {
    std::ostringstream stream;
    stream << "expected " << lhs_text << " != " << rhs_text << " but both are " << describe(lhs);
    report_failure(file, line, stream.str());
  }
}

inline void require_ok(const shuffle::fabric::Status& status, const char* expression, const char* file, int line) {
  if (!status.ok()) {
    report_failure(file, line, std::string{"expected success from "} + expression + " but got " +
                                    shuffle::fabric::format_error(status.error()));
  }
}

template <class T>
void require_ok(const shuffle::fabric::Result<T>& result, const char* expression, const char* file, int line) {
  if (!result.ok()) {
    report_failure(file, line, std::string{"expected success from "} + expression + " but got " +
                                    shuffle::fabric::format_error(result.error()));
  }
}

inline void require_error(const shuffle::fabric::Status& status, shuffle::fabric::ErrorCode expected,
                          const char* expression, const char* file, int line) {
  if (status.ok() || status.code() != expected) {
    report_failure(file, line, std::string{"expected "} + expression + " to fail with " +
                                    shuffle::fabric::to_string(expected) + " but got " +
                                    (status.ok() ? std::string{"success"}
                                                 : shuffle::fabric::format_error(status.error())));
  }
}

template <class T>
void require_error(const shuffle::fabric::Result<T>& result, shuffle::fabric::ErrorCode expected, const char* expression,
                   const char* file, int line) {
  if (result.ok() || result.code() != expected) {
    report_failure(file, line, std::string{"expected "} + expression + " to fail with " +
                                    shuffle::fabric::to_string(expected) + " but got " +
                                    (result.ok() ? std::string{"success"}
                                                 : shuffle::fabric::format_error(result.error())));
  }
}

}  // namespace shuffle::test

#define SHUFFLE_TEST(suite, name)                                                  \
  static void shuffle_test_##suite##_##name();                                     \
  static const ::shuffle::test::Registrar shuffle_test_registrar_##suite##_##name{ \
      #suite, #name, &shuffle_test_##suite##_##name};                              \
  static void shuffle_test_##suite##_##name()

#define REQUIRE(expression) ::shuffle::test::require(static_cast<bool>(expression), #expression, __FILE__, __LINE__)
#define REQUIRE_FALSE(expression) \
  ::shuffle::test::require(!static_cast<bool>(expression), "!" #expression, __FILE__, __LINE__)
#define REQUIRE_EQ(lhs, rhs) ::shuffle::test::require_eq((lhs), (rhs), #lhs, #rhs, __FILE__, __LINE__)
#define REQUIRE_NE(lhs, rhs) ::shuffle::test::require_ne((lhs), (rhs), #lhs, #rhs, __FILE__, __LINE__)
#define REQUIRE_OK(expression) ::shuffle::test::require_ok((expression), #expression, __FILE__, __LINE__)
#define REQUIRE_ERROR(expression, code) \
  ::shuffle::test::require_error((expression), (code), #expression, __FILE__, __LINE__)
#define FAIL_TEST(message) ::shuffle::test::report_failure(__FILE__, __LINE__, (message))
