# Shuffle Fabric examples: real supported paths, runnable from a build tree.
#
# Each example is an independent executable that links the installed-shape
# target ShuffleFabric::fabric and prints deterministic output.

add_executable(example-in-process-shuffle in_process_shuffle.cpp)
target_link_libraries(example-in-process-shuffle PRIVATE ShuffleFabric::fabric)
shuffle_fabric_apply_options(example-in-process-shuffle)

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/loopback_service.cpp)
  add_executable(example-loopback-service loopback_service.cpp)
  target_link_libraries(example-loopback-service PRIVATE ShuffleFabric::fabric)
  shuffle_fabric_apply_options(example-loopback-service)
endif()
