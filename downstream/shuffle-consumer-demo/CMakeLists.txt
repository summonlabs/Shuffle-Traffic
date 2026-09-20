# An independent CMake project that consumes the INSTALLED ShuffleFabric package.
#
# Copyright 2026 Summon Software Labs. SPDX-License-Identifier: Apache-2.0
#
# This project is deliberately NOT part of the ShuffleFabric build tree: it is
# never added with add_subdirectory, it never sees $<BUILD_INTERFACE:...> include
# directories and it never links the in-tree target. It resolves the package
# through find_package(... CONFIG) against the prefix named by CMAKE_PREFIX_PATH,
# so a successful build here is evidence about the installed package only.

cmake_minimum_required(VERSION 3.25)

project(ShuffleConsumerDemo
  VERSION 1.0.0
  DESCRIPTION "Downstream demonstrator for the installed ShuffleFabric package"
  LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()

set(CMAKE_CXX_EXTENSIONS OFF)

# Required version and CONFIG mode: the version file and the targets file must
# both come from the installed prefix. The C++20 requirement is not restated
# here on purpose -- ShuffleFabric::fabric carries cxx_std_20 as an interface
# requirement, so the imported target itself must be enough.
find_package(ShuffleFabric 1.0.0 CONFIG REQUIRED)

# The packaging contract: the INSTALLED package provides the imported target
# ShuffleFabric::fabric. This is asserted rather than assumed, and there is no
# fallback -- a package that does not export the documented target fails here
# instead of being silently patched up by the consumer.
if(NOT TARGET ShuffleFabric::fabric)
  message(FATAL_ERROR
    "the installed ShuffleFabric package does not provide the imported target "
    "ShuffleFabric::fabric; the targets file installed by install(EXPORT ...) "
    "must export the library under that name (EXPORT_NAME fabric).")
endif()

add_executable(shuffle_consumer_demo main.cpp)
target_link_libraries(shuffle_consumer_demo PRIVATE ShuffleFabric::fabric)

# Reported by the demo: 1 means the installed package exported the documented
# target itself, which is checked immediately above.
target_compile_definitions(shuffle_consumer_demo PRIVATE SHUFFLE_FABRIC_TARGET_NATIVE=1)

if(MSVC)
  target_compile_options(shuffle_consumer_demo PRIVATE
    /W4
    /permissive-
    /utf-8
    /Zc:__cplusplus
    /Zc:preprocessor
    /EHsc)
endif()

if(WIN32)
  target_compile_definitions(shuffle_consumer_demo PRIVATE WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

message(STATUS "ShuffleFabric package: ${ShuffleFabric_VERSION} from ${ShuffleFabric_DIR}")
