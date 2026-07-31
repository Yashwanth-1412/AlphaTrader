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

add_executable(alpha_trader
    src/main.cpp
    src/market_data/itch_decoder.cpp
    src/market_order_book.cpp
    src/market_data/market_data_consumer.cpp
    src/market_data/market_data_incremental.cpp
    src/market_data/market_data_snapshot.cpp
    src/order_gateway/order_gateway.cpp
)

add_executable(test_integration
    src/test_integration.cpp
    src/market_data/itch_decoder.cpp
    src/market_order_book.cpp
    src/market_data/market_data_consumer.cpp
    src/market_data/market_data_incremental.cpp
    src/market_data/market_data_snapshot.cpp
)

add_executable(test_snapshot_recovery
    src/test_snapshot_recovery.cpp
    src/market_data/itch_decoder.cpp
    src/market_order_book.cpp
    src/market_data/market_data_consumer.cpp
    src/market_data/market_data_incremental.cpp
    src/market_data/market_data_snapshot.cpp
)

target_include_directories(alpha_trader PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(alpha_trader PRIVATE Threads::Threads)

target_include_directories(test_integration PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_integration PRIVATE Threads::Threads)

add_executable(test_order_gateway
    src/test_order_gateway.cpp
    src/order_gateway/order_gateway.cpp
)

target_include_directories(test_order_gateway PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_order_gateway PRIVATE Threads::Threads)

add_executable(test_trading
    src/test_trading.cpp
    src/market_order_book.cpp
)

target_include_directories(test_trading PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_trading PRIVATE Threads::Threads)

target_include_directories(test_snapshot_recovery PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink
)

target_link_libraries(test_snapshot_recovery PRIVATE Threads::Threads)
