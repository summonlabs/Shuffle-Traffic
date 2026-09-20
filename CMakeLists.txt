cmake_minimum_required(VERSION 3.25)

project(ShuffleFabric
  VERSION 1.0.0
  DESCRIPTION "Vendor-neutral C++20 runtime for governing many-to-many shuffle traffic"
  HOMEPAGE_URL "https://github.com/summonlabs/Shuffle-Traffic"
  LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(SHUFFLE_FABRIC_WARNINGS_AS_ERRORS "Treat first-party compiler warnings as errors" ON)
option(SHUFFLE_FABRIC_BUILD_TOOLS "Build the coordinator, node and inspection tools" ON)
option(SHUFFLE_FABRIC_BUILD_EXAMPLES "Build the examples" ON)
option(SHUFFLE_FABRIC_BUILD_TESTS "Build the test suite" ON)
option(SHUFFLE_FABRIC_SANITIZE_ADDRESS "Build with AddressSanitizer when the toolchain supports it" OFF)

# ---------------------------------------------------------------------------
# First-party compiler discipline: zero warnings in every supported build.
# ---------------------------------------------------------------------------
# Options are applied PRIVATE to first-party targets so that consumers of the
# installed package never inherit warnings-as-errors or sanitizer flags.
function(shuffle_fabric_apply_options target)
  if(MSVC)
    target_compile_options(${target} PRIVATE
      /W4
      /permissive-
      /utf-8
      /Zc:__cplusplus
      /Zc:preprocessor
      /EHsc)
    if(SHUFFLE_FABRIC_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE /WX)
    endif()
  else()
    target_compile_options(${target} PRIVATE
      -Wall
      -Wextra
      -Wpedantic
      -Wshadow
      -Wnon-virtual-dtor
      -Wcast-align
      -Wunused
      -Woverloaded-virtual
      -Wdouble-promotion
      -Wformat=2)
    if(SHUFFLE_FABRIC_WARNINGS_AS_ERRORS)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
  if(WIN32)
    target_compile_definitions(${target} PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
  endif()
  if(SHUFFLE_FABRIC_SANITIZE_ADDRESS)
    if(MSVC)
      target_compile_options(${target} PRIVATE /fsanitize=address /Zi)
      target_link_options(${target} PRIVATE /INCREMENTAL:NO)
    else()
      target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
      target_link_options(${target} PRIVATE -fsanitize=address)
    endif()
  endif()
endfunction()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
set(SHUFFLE_FABRIC_CORE_SOURCES
  src/bytes.cpp
  src/codec.cpp
  src/coordinator.cpp
  src/digest.cpp
  src/durable_store.cpp
  src/edge.cpp
  src/error.cpp
  src/frame.cpp
  src/hash.cpp
  src/ledger.cpp
  src/manifest.cpp
  src/policy.cpp
  src/records.cpp
  src/schedule.cpp
  src/service.cpp
  src/socket.cpp
  src/topology.cpp)

add_library(shuffle_fabric ${SHUFFLE_FABRIC_CORE_SOURCES})
add_library(ShuffleFabric::fabric ALIAS shuffle_fabric)

target_include_directories(shuffle_fabric
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_features(shuffle_fabric PUBLIC cxx_std_20)
shuffle_fabric_apply_options(shuffle_fabric)

if(WIN32)
  # Winsock is used by the transport layer; the import library is part of the
  # exported usage requirements of the installed package.
  target_link_libraries(shuffle_fabric PUBLIC ws2_32)
endif()

set_target_properties(shuffle_fabric PROPERTIES
  EXPORT_NAME fabric
  WINDOWS_EXPORT_ALL_SYMBOLS ON
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR}
  POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Install / export
# ---------------------------------------------------------------------------
install(TARGETS shuffle_fabric
  EXPORT ShuffleFabricTargets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT ShuffleFabricTargets
  FILE ShuffleFabricTargets.cmake
  NAMESPACE ShuffleFabric::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ShuffleFabric)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/ShuffleFabricConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/ShuffleFabricConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ShuffleFabric)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/ShuffleFabricConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/ShuffleFabricConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/ShuffleFabricConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ShuffleFabric)

install(DIRECTORY docs/ DESTINATION ${CMAKE_INSTALL_DOCDIR} OPTIONAL)

# ---------------------------------------------------------------------------
# Tools, examples, tests
# ---------------------------------------------------------------------------
if(SHUFFLE_FABRIC_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

if(SHUFFLE_FABRIC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(SHUFFLE_FABRIC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
