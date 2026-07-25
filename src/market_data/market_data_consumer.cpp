#include "market_data_consumer.h"

namespace alphatrader {

MarketDataConsumer::MarketDataConsumer(
    quantlink::SPSCQueue<MarketUpdate>* market_updates,
    quantlink::Logger* logger,
    const std::string& iface,
    const std::string& snapshot_ip, int snapshot_port,
    const std::string& incremental_ip, int incremental_port)
    : incoming_md_updates_(market_updates),
      logger_(logger),
      iface_(iface),
      snapshot_ip_(snapshot_ip),
      snapshot_port_(snapshot_port),
       incremental_ip_(incremental_ip),
       incremental_port_(incremental_port) {

    market_data_synchronized.store(false, std::memory_order_release);

    auto recv_cb = [this](auto* socket) { recvCallback(socket); };

    incremental_mcast_socket_.recv_callback_ = recv_cb;
    ASSERT(incremental_mcast_socket_.init(incremental_ip_, iface_, incremental_port_, true) >= 0,
           "MarketDataConsumer::MarketDataConsumer() failed to create incremental mcast socket. error: "
           + std::string(std::strerror(errno)));
    const in_addr addr{inet_addr(incremental_ip_.c_str())};
    if ((ntohl(addr.s_addr) & 0xF0000000) == 0xE0000000) {
        ASSERT(incremental_mcast_socket_.join(incremental_ip_),
               "MarketDataConsumer::MarketDataConsumer() failed to join incremental group: "
               + std::to_string(incremental_mcast_socket_.socket_fd_) + " error: " + std::string(std::strerror(errno)));
    }

    snapshot_mcast_socket_.recv_callback_ = recv_cb;
}

auto MarketDataConsumer::start(int core_id) -> void {
    startSnapshotSync();
    run_.store(true, std::memory_order_release);
    thread_ = quantlink::utils::create_and_pin_thread(core_id, "MarketDataConsumer", [this]() { run(); });
}

auto MarketDataConsumer::stop() -> void {
    run_.store(false, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
}

auto MarketDataConsumer::run() noexcept -> void {
    sendToLogger("MarketDataConsumer::run() started.\n");

    while (run_.load(std::memory_order_acquire)) {
        incremental_mcast_socket_.sendAndRecv();
        if (in_recovery_.load(std::memory_order_relaxed) && snapshot_mcast_socket_.socket_fd_ != -1) {
            snapshot_mcast_socket_.sendAndRecv();
        }
    }

    sendToLogger("MarketDataConsumer::run() exited.\n");
}

auto MarketDataConsumer::recvCallback(quantlink::McastSocket* socket) noexcept -> void {
    sendToLogger("MarketDataConsumer::recvCallback() fd=% bytes=%\n",
                 socket->socket_fd_, socket->next_rcv_valid_index_);

    const bool is_snapshot = (socket->socket_fd_ == snapshot_mcast_socket_.socket_fd_);

    if (UNLIKELY(is_snapshot && !in_recovery_.load(std::memory_order_relaxed))) {
        socket->next_rcv_valid_index_ = 0;
        sendToLogger("MarketDataConsumer::recvCallback() ignoring unexpected snapshot message.\n");
        return;
    }

    if (is_snapshot) {
        // Snapshot feed: NO seq prefix, just raw ITCH structs
        if (socket->next_rcv_valid_index_ < 1) return;

        std::size_t i = 0;
        while (i + 1 <= socket->next_rcv_valid_index_) {
            const char* data = socket->inbound_data_.data() + i;
            const std::size_t remaining = socket->next_rcv_valid_index_ - i;

            MarketUpdate update;
            if (decoder_.decodeSnapshot(std::span<const char>(data, remaining), update)) {
                processSnapshot(update);
                i += decoder_.lastDecodedSize();
            } else {
                break;
            }
        }

        if (i > 0) {
            std::memmove(socket->inbound_data_.data(), socket->inbound_data_.data() + i,
                         socket->next_rcv_valid_index_ - i);
            socket->next_rcv_valid_index_ -= i;
        }
        return;
    }

    // Incremental feed: [8-byte seq] [ITCH struct]
    if (socket->next_rcv_valid_index_ < sizeof(SeqNum) + 1) {
        return;
    }

    std::size_t i = 0;
    for (; i + sizeof(SeqNum) + 1 <= socket->next_rcv_valid_index_;) {
        const char* data = socket->inbound_data_.data() + i;
        const std::size_t remaining = socket->next_rcv_valid_index_ - i;

        MarketUpdate update;
        if (decoder_.decode(std::span<const char>(data, remaining), update)) {
            processIncremental(update);
            i += sizeof(SeqNum) + decoder_.lastDecodedSize();
        } else {
            break;
        }
    }

    if (i > 0) {
        std::memmove(socket->inbound_data_.data(), socket->inbound_data_.data() + i,
                     socket->next_rcv_valid_index_ - i);
        socket->next_rcv_valid_index_ -= i;
    }
}

} // namespace alphatrader
