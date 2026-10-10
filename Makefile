CXX ?= g++
CPPFLAGS ?= -Iinclude
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wpedantic
LDLIBS ?= -pthread
BUILD_DIR := build
CORE_SOURCES := src/protocol.cpp src/heartbeat_registry.cpp
HEADERS := $(wildcard include/heartbeat/*.h) src/admin_socket.h src/heartbeat_worker.h src/heartbeat_receiver.h src/server_log.h src/latency_histogram.h src/thread_affinity.h

.PHONY: all test clean

all: $(BUILD_DIR)/heartbeat_server $(BUILD_DIR)/heartbeat_client $(BUILD_DIR)/heartbeat_bench $(BUILD_DIR)/heartbeat_admin

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/heartbeat_server: $(CORE_SOURCES) src/heartbeat_server.cpp src/heartbeat_worker.cpp src/heartbeat_receiver.cpp src/server_log.cpp src/admin_socket.cpp src/server_main.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CORE_SOURCES) src/heartbeat_server.cpp src/heartbeat_worker.cpp src/heartbeat_receiver.cpp src/server_log.cpp src/admin_socket.cpp src/server_main.cpp $(LDLIBS) -o $@

$(BUILD_DIR)/heartbeat_client: src/protocol.cpp src/client_main.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) src/protocol.cpp src/client_main.cpp $(LDLIBS) -o $@

$(BUILD_DIR)/heartbeat_bench: $(CORE_SOURCES) src/benchmark_main.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CORE_SOURCES) src/benchmark_main.cpp $(LDLIBS) -o $@

$(BUILD_DIR)/heartbeat_admin: src/admin_client_main.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BUILD_DIR)/protocol_test: $(CORE_SOURCES) tests/protocol_test.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CORE_SOURCES) tests/protocol_test.cpp -o $@

$(BUILD_DIR)/registry_test: $(CORE_SOURCES) tests/registry_test.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CORE_SOURCES) tests/registry_test.cpp -o $@

$(BUILD_DIR)/latency_histogram_test: tests/latency_histogram_test.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) -Isrc $(CXXFLAGS) tests/latency_histogram_test.cpp -o $@

test: $(BUILD_DIR)/protocol_test $(BUILD_DIR)/registry_test $(BUILD_DIR)/latency_histogram_test
	$(BUILD_DIR)/protocol_test
	$(BUILD_DIR)/registry_test
	$(BUILD_DIR)/latency_histogram_test

clean:
	rm -rf $(BUILD_DIR)



