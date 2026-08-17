# AlphaTrader

A low-latency trading client in C++20: consumes an ITCH market data feed,
maintains its own order book, computes features, runs strategies, and sends
orders over OUCH — with risk limits and PnL tracking on the order path.

Built on [QuantLink](https://github.com/Yashwanth-1412/QuantLink) (vendored as a
submodule). It trades against
[NanoExchange](https://github.com/Yashwanth-1412/NanoExchange).

---

## Topology

```
      ITCH feed (UDP)              TCP snapshot / replay
             │                              │
   ┌─────────▼──────────────────────────────▼─────────┐
   │              MarketDataConsumer                  │  1 thread, pinned
   │  decode → sequence check → gap → recover → queue │
   └──────────────────────┬───────────────────────────┘
                          │ md_queue : SPSCQueue<MarketUpdate>
              ┌───────────▼────────────┐
              │      TradeEngine       │  1 thread, pinned
              │  books    (per ticker, shared)
              │  features (per slot)
              │  strategy (per slot)
              │  risk + PnL (per slot)
              └──┬──────────────────▲──┘
   request_queue │                  │ response_queue
              ┌──▼──────────────────┴──┐
              │      OrderGateway      │  1 thread, pinned
              │  OUCH encode / decode  │
              └───────────┬────────────┘
                          │ TCP
                       exchange
```

Three threads, three SPSC queues, **no locks on the trading path**.

---

## C++ techniques

### Concepts as the extension mechanism

Both extension points — features and strategies — are C++20 concepts rather than
abstract base classes. Nothing in the trading path is virtual.

A feature is anything that can absorb a market update:

```cpp
template<typename Feature>
concept FeaturePlugin = requires(Feature feature, const MarketUpdate& update,
                                 const MarketOrderBook& book) {
    feature.onMarketUpdate(update, book);
};
```

A strategy is anything with the three lifecycle hooks, where the update hooks
must return something convertible to a count of orders submitted:

```cpp
template<typename S, typename Features, typename OrderManagerT, typename PnlT>
concept Strategy = requires(S s, Entry<Features>& entry, const MarketUpdate& update,
                            const OMOrder& order, OrderManagerT& om, PnlT& pnl) {
    { s.onBookUpdate(entry, update, om, pnl) } -> std::convertible_to<size_t>;
    { s.onTrade(entry, update, om, pnl) }      -> std::convertible_to<size_t>;
    s.onOrderUpdate(order);
};
```

The `{ expr } -> std::convertible_to<size_t>` form is a **compound
requirement**: it constrains both that the expression is valid and that its
result type converts. A strategy that returns `void` from `onBookUpdate` fails
at its declaration with a readable message, rather than at the call site three
templates deep.

`Strategy` is a *multi-parameter* concept — it constrains the relationship
between a strategy, its feature set, its order manager and its PnL tracker,
which no single-type interface could express. The engine applies it as a
trailing `requires` clause:

```cpp
template<typename Features, typename StrategyT,
         size_t MaxSlots = 4, size_t MaxTickers = 16, size_t MaxPendingOrders = 64>
    requires Strategy<StrategyT, Features,
                      OrderManager<RiskManager<MaxTickers, MaxPendingOrders>, PnLTracker<MaxTickers>>,
                      PnLTracker<MaxTickers>>
class TradeEngine { ... };
```

So `TradeEngine<Features, BadStrategy>` fails to *instantiate* with "constraint
not satisfied", naming the offending requirement — not with a wall of
substitution errors.

### Variadic inheritance plus a fold expression for feature dispatch

`FeatureEngine` inherits from every feature and fans updates out with a comma
fold:

```cpp
template<typename... Features>
    requires(FeaturePlugin<Features> && ...)
struct FeatureEngine : public Features... {
    void onMarketUpdate(const MarketUpdate& update, const MarketOrderBook& book) noexcept {
        (Features::onMarketUpdate(update, book), ...);
    }

    template<typename T> T&       get() noexcept       { return *static_cast<T*>(this); }
    template<typename T> const T& get() const noexcept { return *static_cast<const T*>(this); }

    static constexpr size_t count() noexcept { return sizeof...(Features); }
};
```

Several things worth noting:

- `requires(FeaturePlugin<Features> && ...)` is a **fold over a concept** — each
  pack element is checked independently, so a malformed feature is named
  individually.
- Inheriting from the pack means feature state is stored *inline* in the engine
  as base subobjects. There is no `vector<unique_ptr<Feature>>`, no indirection,
  and no allocation. The whole feature set is one contiguous object.
- `(Features::onMarketUpdate(update, book), ...)` is a qualified call on each
  base. Because the calls are non-virtual and the types are known, the compiler
  inlines the entire feature computation into the update loop and can keep
  intermediate values in registers across features.
- `get<T>()` is a `static_cast` from derived to base — a compile-time offset,
  usually zero instructions. `features.get<MktPriceFeature>().value()` costs
  nothing beyond the load.

The cost is that adding a feature is a recompile. For a system where the feature
set is known at build time, that is the correct trade.

An individual feature is a plain struct with no inheritance and no registration:

```cpp
struct OrderBookImbalanceFeature {
    void onMarketUpdate(const MarketUpdate&, const MarketOrderBook& book) noexcept {
        const BBO& bbo = book.getBBO();
        int64_t total  = static_cast<int64_t>(bbo.bid_qty) + bbo.ask_qty;
        if (UNLIKELY(bbo.bid_price == Price_INVALID || bbo.ask_price == Price_INVALID || total == 0)) {
            imbalance_ = Ratio_INVALID;
            return;
        }
        imbalance_ = static_cast<Ratio>(
            (static_cast<int64_t>(bbo.bid_qty) - bbo.ask_qty) * Ratio_SCALE / total);
    }
    Ratio value() const noexcept { return imbalance_; }
  private:
    Ratio imbalance_ = Ratio_INVALID;
};
```

It satisfies `FeaturePlugin` structurally. There is no base to derive from and
no macro to register with — conformance is the shape of the type.

### Fixed-point ratios instead of floating point

Signals are integers scaled by `Ratio_SCALE`, not `double`. Imbalance arithmetic
promotes to `int64_t` before scaling so the multiply cannot overflow, and the
divide happens last to preserve precision. Integer arithmetic is deterministic,
reproducible across machines, and avoids the comparison hazards of floating
point in threshold logic — which is the entire job of a signal.

`Ratio_INVALID` is a distinct sentinel, so "no signal" is never confused with
"signal is zero". A crossed or empty book yields `Ratio_INVALID`, and the
strategy declines to trade rather than acting on a fabricated number.

### Non-type template parameters as capacity, checked by the compiler

Everything is bounded at compile time:

```cpp
template<typename Features, typename StrategyT,
         size_t MaxSlots = 4, size_t MaxTickers = 16, size_t MaxPendingOrders = 64>
class TradeEngine { ... };

template<size_t MaxTickers = 16, size_t MaxPendingOrders = 64> class RiskManager { ... };
template<size_t MaxTickers = 16>                               class PnLTracker { ... };
template<typename RiskT, typename PnlT, size_t MaxOrders = 256> class OrderManager { ... };
```

Storage is fixed arrays sized from those parameters. **Nothing on the trading
path allocates.** Capacity exhaustion is a return value, not a `bad_alloc`:
`RiskManager::canSubmit` returns `PENDING_CAPACITY_EXCEEDED`,
`registerTicker` returns `false`.

Runtime `slot_count` is clamped to `MaxSlots` at construction, and every
slot-indexed accessor checks against both bounds:

```cpp
slot_count_(slot_count < MaxSlots ? slot_count : MaxSlots)
...
if (slot >= MaxSlots || slot >= slot_count_) return nullptr;
```

The redundant-looking `slot >= MaxSlots` is doing real work. GCC's
`-Warray-bounds` at `-O3` can only prove the array access safe if the bound is
compared against the *compile-time* extent; comparing only against the runtime
`slot_count_` leaves the dead path unprovable and the build fails under
`-Werror`. The compiler is effectively conscripted as a bounds checker.

### Order id encodes its owning slot

The engine runs N strategy slots against shared books, but the exchange returns
responses keyed only by client order id. Rather than maintain a side table, the
slot is encoded in the id's high bits:

```cpp
static constexpr size_t  SLOT_BITS    = 16;               // 65,536 slots
static constexpr size_t  COUNTER_BITS = 64 - SLOT_BITS;   // 48 bits of counter
static constexpr OrderId COUNTER_MASK = (OrderId{1} << COUNTER_BITS) - 1;

OrderId next(size_t slot) noexcept {
    return (static_cast<OrderId>(slot) << COUNTER_BITS) | (counter_++ & COUNTER_MASK);
}
size_t ownerOf(OrderId id) const noexcept {
    const size_t slot = static_cast<size_t>(id >> COUNTER_BITS);
    return slot < slot_count_ ? slot : slot_count_;
}
```

Routing a fill is a shift and a compare, not a hash lookup — and it happens on
every exchange response. The counter is masked so it can never bleed into the
slot field.

The 16/48 split rather than 48/16 matters: the counter needs 2⁴⁸ ids, whereas
slots need only 2¹⁶. The reverse split wraps after 65,536 orders and starts
misrouting fills into other strategies' PnL. `ownerOf` returning `slot_count_`
for an out-of-range id gives the engine a "not mine" sentinel without an
exception or an optional.

### Trailing return types and `noexcept` throughout

```cpp
auto applyUpdate(const MarketUpdate& update) noexcept -> void;
auto getBBO() const noexcept -> const BBO&;
[[nodiscard]] auto needsMoreData() const noexcept -> bool;
```

`noexcept` on the trading path is a design statement, not decoration: these
functions report failure by return value because there is nothing sensible to
unwind to inside a market data loop. It also lets the compiler omit landing pads
entirely.

`[[nodiscard]]` guards the results that are dangerous to ignore — a decode
result, a risk verdict, a submission outcome.

### Function pointers, not `std::function`, for callbacks

```cpp
using OrderUpdateCallback = void (*)(const OMOrder& order, void* user_data);
```

A raw function pointer plus a `void*` context, in preference to
`std::function`. `std::function` may heap-allocate, and its call is an indirect
jump through a type-erased wrapper. The C-style pair costs one indirect call and
allocates nothing — and there is exactly one callback per order manager, so the
ergonomic loss is negligible.

### `std::map` with transparent comparators for the book

```cpp
std::map<Price, PriceLevel, std::greater<>> bids_;   // descending → begin() = best bid
std::map<Price, PriceLevel, std::less<>>    asks_;   // ascending  → begin() = best ask
std::unordered_map<OrderId, OrderLocation>  order_index_;
```

`std::greater<>` and `std::less<>` are the *transparent* (void-specialised)
forms, which deduce their argument types rather than fixing them to `Price`.
Reversing the bid comparator is what makes best-bid and best-ask symmetric:
`begin()` is the top of book on both sides, with no special-casing.

`order_index_` maps an exchange order reference to `{side, level, order}`, so
processing a cancel or execution is a hash lookup followed by an intrusive
unlink — no scan of the level's order list.

Orders and price levels come from `quantlink::ObjectPool` (the intrusive-list
implementation, selected by including `object_pool_list.h`), so book churn
recycles nodes rather than allocating.

### Rule-of-five deletion for identity types

```cpp
class MarketOrderBook final {
    MarketOrderBook()                                  = delete;
    MarketOrderBook(const MarketOrderBook&)            = delete;
    MarketOrderBook& operator=(const MarketOrderBook&) = delete;
    MarketOrderBook(MarketOrderBook&&)                 = delete;
    MarketOrderBook& operator=(MarketOrderBook&&)      = delete;
```

The book is shared by pointer across strategy slots and holds pool pointers into
its own storage. Copying it would desynchronise slots; moving it would dangle
every `Entry::book`. `final` closes the type, letting the compiler devirtualise
and confirming that inheritance is not the extension mechanism here — concepts
are.

### `__int128` for overflow-free notional checks

```cpp
const __int128 notional = static_cast<__int128>(request.price) * request.qty;
if (notional > ticker->limits.max_order_notional)
    return RiskResult::ORDER_NOTIONAL_EXCEEDED;
```

Price times quantity can exceed 64 bits at realistic scaled-integer prices. A
risk check that silently overflows is worse than no risk check at all, since it
reports a *small* notional for the largest orders. Widening to `__int128` makes
the comparison exact; the arithmetic never reaches the wire.

### Scoped enums as exhaustive result types

```cpp
enum class RiskResult : uint8_t {
    ACCEPTED, TRADING_DISABLED, INVALID_REQUEST, UNKNOWN_TICKER, DUPLICATE_ORDER,
    PENDING_CAPACITY_EXCEEDED, ORDER_QTY_EXCEEDED, ORDER_NOTIONAL_EXCEEDED,
    POSITION_LIMIT_EXCEEDED
};

enum class OMOrderState : uint8_t {
    INVALID, NOT_ACTIVE, ACTIVE, PARTIALLY_FILLED, COMPLETED, CANCELLED, DEAD
};
```

A rejection reports *why*, not just that it failed — which is what makes a
rejection log actionable and what lets tests assert the specific cause rather
than a boolean. `: uint8_t` keeps these one byte so they pack into request and
order structs without adding padding, and `enum class` prevents accidental
comparison between a risk verdict and an order state.

---

## Architecture decisions

### Trading is gated on synchronised market data

```cpp
inline std::atomic<bool> market_data_synchronized{false};
```

An `inline` variable at namespace scope (C++17) — one instance across all
translation units, no `extern`/definition split. It stays false until an
authoritative snapshot *and* every incremental queued behind it have been
applied.

`main.cpp` waits on it before starting the engine, and the engine holds a
`const std::atomic<bool>*` gate so it will not act early.

A strategy acting on a partially-built book is worse than a strategy that has
not started: it reads a book missing levels and concludes the spread is enormous
— which is precisely the condition many signals treat as an opportunity. Making
the gate a *startup* condition rather than a per-update check keeps it off the
hot path.

### Recovery is a state machine with two modes

A UDP feed drops packets. `MarketDataConsumer` tracks `next_exp_inc_seq_num_`
and, on a gap, chooses a route based on how far behind it is:

```cpp
enum class RecoveryMode : uint8_t { None, Snapshot, Replay };
```

- **Replay** — request the specific missing messages over TCP. Cheap, and the
  book stays usable throughout.
- **Snapshot** — request full book state, for gaps larger than the exchange's
  replay window.

Either way, **incrementals keep arriving during recovery and are queued, not
dropped** (`queueIncremental`), then applied in order once recovered state is in
place (`finishSnapshotSync(resume_seq)`, `finishReplaySync`). Recovery that
discarded live data would simply cause a second gap on completion.

`abortSnapshotSync` and `onSnapshotTcpEof` exist because recovery itself can
fail — a truncated transfer, a dropped TCP connection, a streamer that timed the
client out. Falling back cleanly to "still out of sync" is a valid outcome;
getting stuck half-recovered is not. `test_snapshot_recovery` and
`test_replay_recovery` drive these paths directly, including interrupted
transfers.

### The decoder distinguishes "incomplete" from "invalid"

`ItchDecoder::decode()` can fail for two very different reasons, which demand
opposite responses:

| Failure | Meaning | Correct response |
|---|---|---|
| Truncated | known message type, payload not fully arrived | **wait** for more bytes |
| Malformed | type byte is not a known message | **advance one byte** and resync |

Conflating them is a genuine hazard, and was a real defect here. If a malformed
byte is treated as "wait", the buffer head never advances, compaction never
runs, and the feed **wedges permanently** while the receive buffer grows without
bound. The decoder therefore resolves payload size from the type byte *first*
and exposes the distinction explicitly:

```cpp
if (!decoder_.decode(...)) {
    if (decoder_.needsMoreData()) break;   // truncated — wait
    ++i;                                   // malformed — resync one byte
}
```

Two related hardenings sit in the consumer: `"NEXSUB"` registration tokens are
skipped whole rather than parsed as ITCH, and registration is not sent to itself
when the incremental and receive ports coincide. Both are cases where the
consumer could otherwise poison its own input buffer.

### Books are shared, features are not

Multiple slots usually trade the same ticker. Decoding an update once but
applying it into N copies of the same book wastes the most expensive work in the
loop, so `TradeEngine` keeps **one `MarketOrderBook` per ticker**, shared across
slots and handed to strategies by pointer inside `Entry<Features>`:

```cpp
template<typename Features>
struct Entry {
    TickerId                ticker_id = TickerId_INVALID;
    MarketOrderBook*        book      = nullptr;  // shared, engine-owned
    std::optional<Features> features;             // per-slot
};
```

`std::optional<Features>` gives the engine a fixed-size array of entries where
unregistered slots are genuinely empty rather than default-constructed — no
allocation, and "no features here" is representable.

Features are per-slot because a slot may want its own parameterisation of the
same signal, and feature state is cheap. `test_trade_engine` asserts both halves:
registering the same ticker in two slots produces **one** book and does not
double-apply quantities, and both slots compute identical feature values from it.

### Risk is checked before the order exists

`OrderManager` consults `RiskManager` *before* an id is allocated. Limits are
position, per-order quantity, and notional, tracked per ticker.

Crucially, risk tracks **pending exposure**, not just filled position. An order
in flight counts against the limit from the moment of submission, and the
reservation is released on fill, cancel, or reject:

```
canSubmit(request, position)   →  ACCEPTED | <specific rejection>
onSubmitted(request)           →  reserve exposure, record pending
onReplaced(old_id, new_id)     →  re-key the pending reservation
onOrderResponse(response, pnl) →  release reservation, update PnL on fills
```

Checking only *filled* position would let a strategy send ten orders inside one
round trip and end up ten times over its limit — the limit would be enforced
only against a position that had not happened yet. `pendingExposure(ticker_id)`
is exposed so tests can assert reservations are released on every terminal
state, which is the property that actually matters.

`disableTrading()` is a kill switch checked first in `canSubmit`, so it takes
effect on the very next order with no other coordination.

### The order state machine is explicit

```
NOT_ACTIVE ──accept──> ACTIVE ──partial──> PARTIALLY_FILLED ──fill──> COMPLETED
     │                   │                        │
     └──reject──> DEAD   └──cancel──> CANCELLED <─┘
```

`OMOrder` carries `is_cancel_requested` and `is_cancel_rejected` alongside the
state, because a cancel in flight is not a state — the order is still working
and can still fill while the cancel is being processed. Collapsing that into the
state enum would make a fill-after-cancel-request look like a protocol error
instead of the normal race it is.

`DEAD` is distinct from `CANCELLED`: rejected or internally inconsistent orders
must never be reused, whereas a cancelled order is a normal terminal state.

### PnL separates realised from unrealised

`PnLTracker` keeps `net_qty`, `avg_entry_price`, `realized_pnl` and
`unrealized_pnl` per ticker. `onExecution` updates the position and realises PnL
on a close or a flip; `onMark` marks to market against the book.

Keeping them apart is what makes the numbers interpretable — a strategy that is
up on paper and one that has banked the difference are not the same strategy.
`totalPnL()` returns `std::optional<int64_t>`, empty when no ticker is
registered, so "no PnL yet" is distinct from "PnL is zero".

Lookup is a linear scan over at most `MaxTickers` entries. For 16 tickers that
is a handful of contiguous comparisons and beats a hash map on both latency and
allocation.

---

## Composition

Everything is assembled in `main.cpp`, where the concrete types are chosen:

```cpp
using TradingFeatures = FeatureEngine<MktPriceFeature, SpreadFeature,
                                      OrderBookImbalanceFeature, AggTradeRatioFeature>;
using Engine          = TradeEngine<TradingFeatures, LiquidityTaker, 4, 16, 64>;
```

Four features, one strategy, four slots, sixteen tickers, sixty-four pending
orders — all fixed at compile time, all bounds-checked by the compiler, all
inlined into one loop.

`LiquidityTaker` fires IOC orders at the best quote when book imbalance crosses
`IocSignalConfig::entry_imbalance`. It sits on the inside of the book and pays
the spread, which is the honest characterisation of a taking strategy.

---

## Building

```bash
git clone --recurse-submodules https://github.com/Yashwanth-1412/AlphaTrader.git
cd AlphaTrader
cmake -S . -B build
cmake --build build -j
```

Requires a C++20 compiler and pthreads. Built with
`-O3 -march=native -Wall -Wextra -Werror`.

## Running

```bash
./build/alpha_trader [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--gateway-ip` / `--gateway-port` | `127.0.0.1:21000` | exchange OUCH endpoint |
| `--incremental-ip` / `--incremental-port` | `127.0.0.1:21001` | exchange feed gateway |
| `--receive-port` | `22001` | local UDP port for the feed |
| `--snapshot-ip` / `--snapshot-port` | `127.0.0.1:21003` | TCP snapshot / replay |
| `--ticker <id>` | `1` | register a ticker (repeatable) |
| `--position-limit` / `--order-limit` / `--notional-limit` | — | per-ticker risk limits |
| `--ioc-qty` / `--imbalance` | — | liquidity taker signal config |
| `--core-consumer` / `--core-engine` / `--core-gateway` | — | core pinning (`-1` = unpinned) |

Against a local NanoExchange:

```bash
# terminal 1
../NanoExchange/build/NanoExchange lo 21000 127.0.0.1 21001 21003 lo

# terminal 2
./build/alpha_trader --ticker 1 --ioc-qty 10 --imbalance 900000
```

`--incremental-port` and `--receive-port` must differ — the client registers
with the exchange's port and listens on its own.

On shutdown it prints a session summary: orders sent, replaces, cancels, risk
rejections, fills, filled quantity, and per-ticker position and PnL.

## Testing

```bash
cd build
for t in test_order_manager test_trade_engine test_trading test_order_gateway \
         test_snapshot_recovery test_replay_recovery test_integration; do ./$t; done
```

| Test | Focus |
|---|---|
| `test_order_manager` | order lifecycle, risk rejection, pending exposure release |
| `test_trade_engine` | slot routing, shared books, fill attribution, bounds |
| `test_trading` | features, strategy signals, PnL arithmetic |
| `test_order_gateway` | OUCH encode/decode round-trip |
| `test_snapshot_recovery` | snapshot sync, queued incrementals, aborts |
| `test_replay_recovery` | gap detection, replay requests, resume ordering |
| `test_integration` | consumer → engine → gateway end to end |

The recovery tests carry the most weight, because recovery is where the failure
modes live. The happy path is exercised constantly at runtime; a truncated
snapshot or an interrupted replay is rare enough that it will only ever be
correct if it is tested.

## Tools

- `tools/compete.py` — drives multiple client instances against one exchange
- `scripts/check_multicast.py` — reports whether an interface actually carries
  the `MULTICAST` flag, which is what motivated the UDP fan-out design
