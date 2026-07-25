#pragma once

#include <atomic>

namespace alphatrader {

// False until an authoritative snapshot and all queued incrementals are applied.
inline std::atomic<bool> market_data_synchronized{false};

} // namespace alphatrader
