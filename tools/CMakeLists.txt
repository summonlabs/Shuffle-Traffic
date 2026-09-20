# Shuffle Fabric tools: real processes that exercise the runtime.
#
# shuffle-fabric-coordinator serves the authority protocol, shuffle-fabric-node
# is a participant that moves real payload bytes over loopback TCP, and
# shuffle-fabric-cli inspects plans, durable state and a running coordinator.

add_executable(shuffle-fabric-coordinator shuffle-fabric-coordinator.cpp)
target_link_libraries(shuffle-fabric-coordinator PRIVATE ShuffleFabric::fabric)
shuffle_fabric_apply_options(shuffle-fabric-coordinator)

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/shuffle-fabric-node.cpp)
  add_executable(shuffle-fabric-node shuffle-fabric-node.cpp)
  target_link_libraries(shuffle-fabric-node PRIVATE ShuffleFabric::fabric)
  shuffle_fabric_apply_options(shuffle-fabric-node)
endif()

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/shuffle-fabric-cli.cpp)
  add_executable(shuffle-fabric-cli shuffle-fabric-cli.cpp)
  target_link_libraries(shuffle-fabric-cli PRIVATE ShuffleFabric::fabric)
  shuffle_fabric_apply_options(shuffle-fabric-cli)
  install(TARGETS shuffle-fabric-cli RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

install(TARGETS shuffle-fabric-coordinator RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
