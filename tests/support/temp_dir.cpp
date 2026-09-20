// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#include "temp_dir.hpp"

#include <atomic>
#include <cstdint>
#include <system_error>

#include "test_support.hpp"

namespace shuffle::test {
namespace {

std::atomic<std::uint64_t>& counter() {
  static std::atomic<std::uint64_t> value{0};
  return value;
}

[[nodiscard]] std::string sanitize(std::string_view label) {
  std::string out;
  out.reserve(label.size());
  for (const char ch : label) {
    const bool safe = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' ||
                      ch == '_' || ch == '.';
    out.push_back(safe ? ch : '_');
  }
  if (out.empty()) {
    out = "scratch";
  }
  return out;
}

}  // namespace

std::filesystem::path TempDir::root() {
  const std::string configured = environment_value("SHUFFLE_FABRIC_TEST_ROOT");
  if (!configured.empty()) {
    return std::filesystem::path{configured};
  }
  return std::filesystem::temp_directory_path() / "shuffle-fabric-tests";
}

TempDir::TempDir(std::string_view label) {
  const std::uint64_t index = counter().fetch_add(1);
  const std::string name = sanitize(label) + "-" + std::to_string(index);
  path_ = root() / name;
  std::error_code error;
  std::filesystem::remove_all(path_, error);
  std::filesystem::create_directories(path_, error);
}

TempDir::~TempDir() {
  clean();
}

void TempDir::clean() {
  if (cleaned_) {
    return;
  }
  cleaned_ = true;
  std::error_code error;
  std::filesystem::remove_all(path_, error);
}

}  // namespace shuffle::test
