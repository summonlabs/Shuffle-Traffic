// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "test_support.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace shuffle::test {
namespace {

std::vector<TestCase>& registry() {
  static std::vector<TestCase> tests;
  return tests;
}

std::uint64_t& seed_storage() {
  static std::uint64_t seed = 0;
  return seed;
}

bool& seed_fixed() {
  static bool fixed = false;
  return fixed;
}

[[nodiscard]] std::uint64_t splitmix64(std::uint64_t& state) noexcept {
  state += 0x9e3779b97f4a7c15ull;
  std::uint64_t value = state;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
  return value ^ (value >> 31);
}

[[nodiscard]] bool matches_filter(const TestCase& test, const std::string& filter) {
  if (filter.empty()) {
    return true;
  }
  const std::string qualified = test.suite + "." + test.name;
  return qualified.find(filter) != std::string::npos;
}

}  // namespace

TestFailure::TestFailure(std::string message) : message_(std::move(message)) {}

const char* TestFailure::what() const noexcept {
  return message_.c_str();
}

void register_test(std::string_view suite, std::string_view name, TestBody body) {
  registry().push_back(TestCase{std::string{suite}, std::string{name}, body});
}

const std::vector<TestCase>& all_tests() {
  return registry();
}

Registrar::Registrar(std::string_view suite, std::string_view name, TestBody body) {
  register_test(suite, name, body);
}

void report_failure(const char* file, int line, std::string message) {
  std::string rendered;
  rendered.reserve(message.size() + 64);
  rendered += file;
  rendered += ":";
  rendered += std::to_string(line);
  rendered += ": ";
  rendered += message;
  throw TestFailure(std::move(rendered));
}

void require(bool condition, const char* expression, const char* file, int line) {
  if (!condition) {
    report_failure(file, line, std::string{"requirement failed: "} + expression);
  }
}

std::string environment_value(std::string_view name) {
  const std::string key{name};
#if defined(_MSC_VER)
  char* buffer = nullptr;
  std::size_t size = 0;
  if (_dupenv_s(&buffer, &size, key.c_str()) != 0 || buffer == nullptr) {
    return {};
  }
  std::string value{buffer};
  std::free(buffer);
  return value;
#else
  const char* value = std::getenv(key.c_str());
  return value != nullptr ? std::string{value} : std::string{};
#endif
}

std::uint64_t run_seed() {
  if (!seed_fixed()) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    seed_storage() = static_cast<std::uint64_t>(now);
    seed_fixed() = true;
  }
  return seed_storage();
}

std::uint64_t derive_seed(std::string_view label) {
  std::uint64_t state = run_seed() ^ 0xd1b54a32d192ed03ull;
  for (const char ch : label) {
    state = (state ^ static_cast<std::uint64_t>(static_cast<unsigned char>(ch))) * 0x100000001b3ull;
  }
  return splitmix64(state);
}

int run_all(int argc, char** argv) {
  std::string filter;
  bool list_only = false;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument{argv[index]};
    constexpr std::string_view seed_prefix = "--seed=";
    constexpr std::string_view filter_prefix = "--filter=";
    if (argument.rfind(seed_prefix, 0) == 0) {
      std::uint64_t value = 0;
      for (const char ch : argument.substr(seed_prefix.size())) {
        if (ch < '0' || ch > '9') {
          std::fprintf(stderr, "invalid --seed value\n");
          return 2;
        }
        value = (value * 10) + static_cast<std::uint64_t>(ch - '0');
      }
      seed_storage() = value;
      seed_fixed() = true;
    } else if (argument.rfind(filter_prefix, 0) == 0) {
      filter = std::string{argument.substr(filter_prefix.size())};
    } else if (argument == "--list") {
      list_only = true;
    } else {
      std::fprintf(stderr, "usage: %s [--seed=<u64>] [--filter=<substring>] [--list]\n", argv[0]);
      return 2;
    }
  }

  std::vector<TestCase> tests = registry();
  std::sort(tests.begin(), tests.end(), [](const TestCase& lhs, const TestCase& rhs) {
    if (lhs.suite != rhs.suite) {
      return lhs.suite < rhs.suite;
    }
    return lhs.name < rhs.name;
  });

  const std::uint64_t seed = run_seed();
  std::printf("shuffle-fabric test binary: %zu case(s), seed=%llu\n", tests.size(),
              static_cast<unsigned long long>(seed));

  if (list_only) {
    for (const TestCase& test : tests) {
      if (matches_filter(test, filter)) {
        std::printf("  %s.%s\n", test.suite.c_str(), test.name.c_str());
      }
    }
    return 0;
  }

  std::size_t executed = 0;
  std::size_t failed = 0;
  for (const TestCase& test : tests) {
    if (!matches_filter(test, filter)) {
      continue;
    }
    ++executed;
    const std::string qualified = test.suite + "." + test.name;
    std::printf("[ RUN  ] %s\n", qualified.c_str());
    std::fflush(stdout);
    try {
      test.body();
      std::printf("[  OK  ] %s\n", qualified.c_str());
    } catch (const TestFailure& failure) {
      ++failed;
      std::printf("[ FAIL ] %s\n        %s\n", qualified.c_str(), failure.message().c_str());
      std::printf("        reproduce: --seed=%llu --filter=%s\n", static_cast<unsigned long long>(seed),
                  qualified.c_str());
    } catch (const std::exception& error) {
      ++failed;
      std::printf("[ FAIL ] %s\n        unexpected exception: %s\n", qualified.c_str(), error.what());
      std::printf("        reproduce: --seed=%llu --filter=%s\n", static_cast<unsigned long long>(seed),
                  qualified.c_str());
    } catch (...) {
      ++failed;
      std::printf("[ FAIL ] %s\n        unexpected non-standard exception\n", qualified.c_str());
    }
    std::fflush(stdout);
  }

  std::printf("summary: %zu executed, %zu failed (seed=%llu)\n", executed, failed,
              static_cast<unsigned long long>(seed));
  if (executed == 0) {
    std::printf("summary: no test matched the filter\n");
    return 1;
  }
  return failed == 0 ? 0 : 1;
}

}  // namespace shuffle::test
