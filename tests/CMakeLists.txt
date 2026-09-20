# Shuffle Fabric test surfaces.
#
# Every test is registered with CTest and runs without a timeout property: a
# hanging test is a defect to diagnose, not something to paper over.

set(SHUFFLE_FABRIC_TEST_SUPPORT
  ${CMAKE_CURRENT_SOURCE_DIR}/support/test_main.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/support/test_support.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/support/temp_dir.cpp)

# shuffle_fabric_add_test(<name> <source>...) builds a test binary that links
# the shared support code and registers a single CTest case.
function(shuffle_fabric_add_test name)
  set(sources ${ARGN})
  add_executable(${name} ${sources} ${SHUFFLE_FABRIC_TEST_SUPPORT})
  target_link_libraries(${name} PRIVATE ShuffleFabric::fabric)
  target_include_directories(${name} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/support
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${PROJECT_SOURCE_DIR}/src)
  shuffle_fabric_apply_options(${name})
  add_test(NAME ${name} COMMAND ${name})
endfunction()

shuffle_fabric_add_test(test_unit_core unit/test_core.cpp)
shuffle_fabric_add_test(test_unit_model unit/test_model.cpp)
shuffle_fabric_add_test(test_unit_ledger unit/test_ledger.cpp)
shuffle_fabric_add_test(test_unit_schedule unit/test_schedule.cpp)
shuffle_fabric_add_test(test_unit_coordinator unit/test_coordinator.cpp)
shuffle_fabric_add_test(test_scale_matrix scale/test_scale_matrix.cpp)
shuffle_fabric_add_test(test_property_shuffle property/test_property_shuffle.cpp)
shuffle_fabric_add_test(test_concurrency_accounting concurrency/test_concurrency_accounting.cpp)
shuffle_fabric_add_test(test_integration_shuffle integration/test_integration_shuffle.cpp)
shuffle_fabric_add_test(test_protocol_frames protocol/test_protocol_frames.cpp)
shuffle_fabric_add_test(test_service_loopback protocol/test_service_loopback.cpp)
shuffle_fabric_add_test(test_adversarial_frames adversarial/test_adversarial_frames.cpp)
shuffle_fabric_add_test(test_durable_store persistence/test_durable_store.cpp)
shuffle_fabric_add_test(test_coordinator_recovery persistence/test_coordinator_recovery.cpp)
shuffle_fabric_add_test(test_durable_recovery persistence/test_durable_recovery.cpp)

# The multiprocess proof is driven by scripts/validate-multiprocess.ps1: it
# starts one real coordinator service process and several real participant
# processes over loopback TCP. The participant program is built here so the
# script uses the same artifact the release ships.
if(TARGET shuffle-fabric-coordinator)
  add_executable(multiprocess-participant support/multiprocess_participant.cpp)
  target_link_libraries(multiprocess-participant PRIVATE ShuffleFabric::fabric)
  shuffle_fabric_apply_options(multiprocess-participant)
endif()
