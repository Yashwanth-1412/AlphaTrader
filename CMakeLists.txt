cmake_minimum_required(VERSION 3.20)
project(AlphaTrader VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compiler flags
set(CMAKE_CXX_FLAGS "-O3 -march=native -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

find_package(Threads REQUIRED)

file(GLOB MARKET_DATA_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/market_data/*.cpp)
file(GLOB GATEWAY_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/order_gateway/*.cpp)
set(MARKET_ORDER_BOOK ${CMAKE_CURRENT_SOURCE_DIR}/src/market_order_book.cpp)

set(MD_CORE_SOURCES ${MARKET_DATA_SOURCES} ${MARKET_ORDER_BOOK})

set(APP_TARGETS alpha_trader test_integration test_snapshot_recovery test_replay_recovery
    test_order_gateway test_trading test_order_manager test_trade_engine)

add_executable(alpha_trader src/main.cpp ${MD_CORE_SOURCES} ${GATEWAY_SOURCES})

foreach(test IN ITEMS test_integration test_snapshot_recovery test_replay_recovery)
    add_executable(${test} test/${test}.cpp ${MD_CORE_SOURCES})
endforeach()

add_executable(test_order_gateway test/test_order_gateway.cpp ${GATEWAY_SOURCES})
add_executable(test_trading test/test_trading.cpp ${MARKET_ORDER_BOOK})
add_executable(test_order_manager test/test_order_manager.cpp)
add_executable(test_trade_engine test/test_trade_engine.cpp ${MARKET_ORDER_BOOK})

foreach(target IN LISTS APP_TARGETS)
    target_include_directories(${target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
    )
    target_link_libraries(${target} PRIVATE Threads::Threads)
endforeach()
