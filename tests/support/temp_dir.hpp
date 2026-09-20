// Per-test scratch directories with best-effort cleanup.
//
// Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0

#pragma once

#include <filesystem>
#include <string_view>

namespace shuffle::test {

class TempDir {
 public:
  explicit TempDir(std::string_view label);
  ~TempDir();

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  TempDir(TempDir&&) = delete;
  TempDir& operator=(TempDir&&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }
  [[nodiscard]] std::filesystem::path file(std::string_view name) const {
    return path_ / std::filesystem::path{std::string{name}};
  }
  [[nodiscard]] static std::filesystem::path root();

  void clean();

 private:
  std::filesystem::path path_;
  bool cleaned_{false};
};

}  // namespace shuffle::test
