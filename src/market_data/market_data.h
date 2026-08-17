#pragma once

#include "../types.h"
#include "QuantLink/Lib/common/macros.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/network/mcast_socket.h"
#include "itch_decoder.h"

#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace alphatrader {

// False until an authoritative snapshot and all queued incrementals are applied.
inline std::atomic<bool> market_data_synchronized{false};

class MarketDataConsumer final {
  public:
    MarketDataConsumer(quantlink::SPSCQueue<MarketUpdate>* market_updates, quantlink::Logger* logger, const std::string& iface, const std::string& snapshot_tcp_ip, int snapshot_tcp_port,
                       const std::string& incremental_ip, int incremental_port, int receive_port);

    MarketDataConsumer()                                     = delete;
    MarketDataConsumer(const MarketDataConsumer&)            = delete;
    MarketDataConsumer& operator=(const MarketDataConsumer&) = delete;
    MarketDataConsumer(MarketDataConsumer&&)                 = delete;
    MarketDataConsumer& operator=(MarketDataConsumer&&)      = delete;

    auto start(int core_id = -1) -> void;
    auto stop() -> void;

    ~MarketDataConsumer() { stop(); }

  private:
    auto run() noexcept -> void;
    auto recvCallback(quantlink::McastSocket* socket) noexcept -> void;
    auto sendRegistration() noexcept -> void;
    auto isOwnAddress(const std::string& ip) const noexcept -> bool;

    auto processIncremental(const MarketUpdate& update) noexcept -> void;
    auto processSnapshot(const MarketUpdate& update) noexcept -> void;

    auto detectGap(SeqNum received_seq) noexcept -> void;
    auto queueIncremental(const MarketUpdate& update) noexcept -> void;

    auto startSnapshotSync() noexcept -> void;
    auto startReplaySync(SeqNum client_seq, bool fresh = true) noexcept -> void;
    auto finishSnapshotSync(SeqNum resume_seq) noexcept -> void;
    auto finishReplaySync() noexcept -> void;
    auto abortSnapshotSync() noexcept -> void;
    auto readSnapshotTcp() noexcept -> void;
    auto processReplay() noexcept -> void;
    auto onSnapshotTcpEof() noexcept -> void;

    auto sendToLogger(const char* fmt, auto&&... args) noexcept -> void { logger_->log(fmt, std::forward<decltype(args)>(args)...); }

    quantlink::SPSCQueue<MarketUpdate>* incoming_md_updates_ = nullptr;
    quantlink::Logger*                  logger_              = nullptr;

    ItchDecoder decoder_;

    std::atomic<bool> run_{false};
    std::thread       thread_;

    SeqNum            next_exp_inc_seq_num_ = 0;
    std::atomic<bool> in_recovery_{false};
    bool              snapshot_have_start_ = false;

    enum class RecoveryMode : uint8_t {
        None,
        Snapshot,
        Replay
    };
    RecoveryMode recovery_mode_       = RecoveryMode::None;
    bool         replay_status_read_  = false;
    SeqNum       replay_last_applied_ = 0;

    // Local optimization: skip a doomed replay request when the gap exceeds the server's
    // replay capacity (NanoExchange src/network/market/snapshot_stream.h REPLAY_CAPACITY).
    // The server re-checks this itself and answers 'N', which the client escalates to a
    // snapshot, so this constant is not a correctness-critical contract.
    static constexpr SeqNum REPLAY_CAPACITY = 8192;

    quantlink::McastSocket incremental_mcast_socket_{*logger_};
    int                    snapshot_tcp_fd_ = -1;
    std::vector<char>      snapshot_tcp_buffer_;

    const std::string iface_;
    const std::string snapshot_tcp_ip_;
    const int         snapshot_tcp_port_;
    const std::string incremental_ip_;
    const int         incremental_port_;
    const int         receive_port_;
    int64_t           last_registration_ns_ = 0;

    using QueuedUpdates = std::map<SeqNum, MarketUpdate>;
    QueuedUpdates             incremental_queued_updates_;
    std::vector<MarketUpdate> snapshot_queued_updates_;
};

} // namespace alphatrader
