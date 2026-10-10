cmake_minimum_required(VERSION 3.16)
project(linux_heartbeat LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "This project requires Linux (epoll, signalfd and Linux sockets).")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(heartbeat_core
    src/protocol.cpp
    src/heartbeat_registry.cpp
)
target_include_directories(heartbeat_core PUBLIC include)
target_compile_options(heartbeat_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(heartbeat_server
    src/server_main.cpp
    src/heartbeat_server.cpp
    src/heartbeat_worker.cpp
    src/heartbeat_receiver.cpp
    src/server_log.cpp
    src/admin_socket.cpp
)
target_link_libraries(heartbeat_server PRIVATE heartbeat_core Threads::Threads)
target_compile_options(heartbeat_server PRIVATE -Wall -Wextra -Wpedantic)

add_executable(heartbeat_client src/client_main.cpp)
target_link_libraries(heartbeat_client PRIVATE heartbeat_core Threads::Threads)
target_compile_options(heartbeat_client PRIVATE -Wall -Wextra -Wpedantic)

add_executable(heartbeat_bench src/benchmark_main.cpp)
target_link_libraries(heartbeat_bench PRIVATE heartbeat_core Threads::Threads)
target_compile_options(heartbeat_bench PRIVATE -Wall -Wextra -Wpedantic)

add_executable(heartbeat_admin src/admin_client_main.cpp)
target_compile_options(heartbeat_admin PRIVATE -Wall -Wextra -Wpedantic)

include(CTest)
if(BUILD_TESTING)
    add_executable(protocol_test tests/protocol_test.cpp)
    target_link_libraries(protocol_test PRIVATE heartbeat_core)
    add_test(NAME protocol_test COMMAND protocol_test)

    add_executable(registry_test tests/registry_test.cpp)
    target_link_libraries(registry_test PRIVATE heartbeat_core)
    add_test(NAME registry_test COMMAND registry_test)

    add_executable(latency_histogram_test tests/latency_histogram_test.cpp)
    target_include_directories(latency_histogram_test PRIVATE src)
    target_compile_options(latency_histogram_test PRIVATE -Wall -Wextra -Wpedantic)
    add_test(NAME latency_histogram_test COMMAND latency_histogram_test)
endif()




