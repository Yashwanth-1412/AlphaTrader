#pragma once

#include "../types.h"
#include "itch_decoder.h"
#include "market_data_sync.h"

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/network/mcast_socket.h"
#include "QuantLink/Lib/common/macros.h"

#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace alphatrader {

class MarketDataConsumer final {
public:
    MarketDataConsumer(
        quantlink::SPSCQueue<MarketUpdate>* market_updates,
        quantlink::Logger* logger,
        const std::string& iface,
        const std::string& snapshot_ip, int snapshot_port,
        const std::string& incremental_ip, int incremental_port);

    MarketDataConsumer() = delete;
    MarketDataConsumer(const MarketDataConsumer&) = delete;
    MarketDataConsumer& operator=(const MarketDataConsumer&) = delete;
    MarketDataConsumer(MarketDataConsumer&&) = delete;
    MarketDataConsumer& operator=(MarketDataConsumer&&) = delete;

    auto start(int core_id = -1) -> void;
    auto stop() -> void;

    ~MarketDataConsumer() { stop(); }

private:
    auto run() noexcept -> void;
    auto recvCallback(quantlink::McastSocket* socket) noexcept -> void;

    auto processIncremental(const MarketUpdate& update) noexcept -> void;
    auto processSnapshot(const MarketUpdate& update) noexcept -> void;

    auto detectGap(SeqNum received_seq) noexcept -> void;
    auto queueIncremental(const MarketUpdate& update) noexcept -> void;

    auto startSnapshotSync() noexcept -> void;
    auto finishSnapshotSync(SeqNum resume_seq) noexcept -> void;
    auto abortSnapshotSync() noexcept -> void;

    auto sendToLogger(const char* fmt, auto&&... args) noexcept -> void {
        logger_->log(fmt, __FILE__, __LINE__, __FUNCTION__,
                     quantlink::utils::get_current_epoch_nanos(), std::forward<decltype(args)>(args)...);
    }

    quantlink::SPSCQueue<MarketUpdate>* incoming_md_updates_ = nullptr;
    quantlink::Logger* logger_ = nullptr;

    ItchDecoder decoder_;

    std::atomic<bool> run_{false};
    std::thread thread_;

    SeqNum next_exp_inc_seq_num_ = 0;
    std::atomic<bool> in_recovery_{false};
    bool snapshot_have_start_ = false;

    quantlink::McastSocket incremental_mcast_socket_{*logger_};
    quantlink::McastSocket snapshot_mcast_socket_{*logger_};

    const std::string iface_;
    const std::string snapshot_ip_;
    const int snapshot_port_;
    const std::string incremental_ip_;
    const int incremental_port_;

    using QueuedUpdates = std::map<SeqNum, MarketUpdate>;
    QueuedUpdates incremental_queued_updates_;
    std::vector<MarketUpdate> snapshot_queued_updates_;

};

} // namespace alphatrader
