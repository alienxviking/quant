# M8 — live, tiny capital

Written before `LiveVenue` exists, for the reason M0 wrote the data contract
before the recorder and M7 wrote the observability doc before the reader: the
seam is the expensive thing to change once anything depends on it, and this is
the first milestone where getting it wrong costs money rather than a re-derive.

Read `docs/engine-contract.md` first. This document inherits its seam and does
not renegotiate it.

---

## 1. The scoping call

**M8 is not "write a `LiveVenue`".**

`LiveVenue` is small. `ExecutionVenue` is four methods, two of which it barely
implements: `submit` formats a request and hands it to a channel, `cancel` does
the same, `observe` does nothing because a real venue needs no help deciding what
filled, and `poll` drains whatever the venue has said since last time. It is a
translator between our order types and Binance's, and translators are ordinary.

What is not ordinary is everything around it, and sorting those by *which
milestone can falsify them* — the method that shrank M1.d, M2 and M7 — leaves
three things M8 must supply and nothing else can:

1. **The engine acquires a second asynchronous input.** Every world so far has
   had exactly one: market data arrives, and the venue's answers are a pure
   function of it. A live venue speaks when it feels like it.
2. **There is a counterparty with its own books.** For the first time our record
   of what happened can be checked against someone else's, and that is a
   stronger check than anything available until now.
3. **An order can exist that we do not know about.** Between writing the
   write-ahead `submitted` line and hearing the response, the truth is
   indeterminate, and a crash in that window leaves money in a state no previous
   milestone could reach.

Everything else people associate with "going live" — the signing, the API keys,
the order formats — is mechanical. These three are the milestone.

### The defect this scoping found, before any code was written

As written before slice (a): `Engine::run` blocked on `source.next_event()`
(`crates/quant-engine/src/lib.rs`), and `venue.poll()` was called in exactly one
place, inside `step` (since replaced by `Engine::dispatch`). So **execution
reports were drained only when a market event arrived**, and
`LiveSource::next_event` called `recv()` with no timeout.

In two of the three worlds this is invisible, which is why it survived five
milestones. `SimulatedVenue` produces outcomes only in response to `observe`,
which happened inside `step` anyway — the coupling is real and costs nothing.

In the third world it is wrong. A live fill arrives from the venue's user-data
stream on its own schedule, and while no market event arrives the engine thread
is simply parked.

The tempting example is M5's twelve-hour outage, and it is the **wrong** one: a
total network loss takes the user-data socket down too, so nothing would have
been delivered either way. The window that matters is narrower and far more
common — the **market** socket stalling or being dropped while the user-data
socket is healthy. Binance closes a stream every 24 hours by design, and
`ConnectionPolicy`'s idle timeout is 120 s, so this is seconds to minutes, many
times a run, with an order possibly working across it. Seconds are enough: the
engine does not merely learn late, it cannot act, and the risk layer's tally does
not move either.

That is a change to the seam, which is why it is settled here rather than
discovered during a run. It is also the single best argument for writing this
document first: the defect is invisible from the backtest and the paper run, and
visible in ten minutes of reading once the question is "what does a live venue
need".

**Slice (a) fixed the engine's half and missed the source's.** The engine learned
to wake on `next_event_timeout` returning `Wake::Idle`, and the trait's default
can never return `Idle`, on purpose, so that a historical replay stays
bit-identical. Only a source with a second thing to wait for overrides it, and
`LiveSource`, the one source with exactly that, never did: its `next_event` still
blocked in `recv()` under a comment saying so on purpose. The idle path ran in
tests, whose test sources override the method, and never in production. Found by
slice (c)'s mapping, not by a test, and fixed in `m8a-live-source-idles` together
with the other half of the forward-only clock (§5).

---

## 2. What it is not

**Not about profit.** The strategy loses money. M4 measured it at ten basis
points a side and M5 confirmed it over a fortnight of live data: BTCUSDT realized
+0.73 on price against −69.08 in commission. Running it with real capital is not
an attempt to make money and must not be reported as one. The cheapest possible
strategy is the *correct* instrument for validating a venue adapter, because
every surprise it produces is the adapter's.

**Not a strategy milestone.** No parameter changes, no new signals, no sizing
rule. The strategy is a known quantity and that is its entire value here — if
behaviour differs from the paper run, the strategy is the one thing that cannot
be the reason.

**One exception, decided on 2026-10-15 before anything ran live: the exit sells
what the strategy holds.** A spot venue takes a buy's fee in the asset bought, so
after buying `qty` the position is `qty` minus the fee. `MaCrossover` exited by
selling a fixed `qty` and counted only an exact zero as flat, so live it would
never be flat again, and every later down-cross would sell `qty` again: refused
for want of balance or, with other BTC in the account, selling coins it never
bought. The exit now sells the position rounded down to the lot, and less than a
lot counts as flat. That is a correction rather than the kind of sizing rule this
paragraph forbids, and the paragraph's reason survives it. Wherever fees are paid
in the quote asset, which is every backtest and all of M5, the position is
exactly zero or `qty`, so the change must be a no-op: `m8c-exit-sizing` is
accepted only if M5's 824 fills and cash 31.64743842 reproduce exactly. Both
sides of every M8 comparison run the same code, so the strategy still cannot be
the reason live differs from the model, but the venue model can be.
`SimulatedVenue` charges every fee in the quote asset, so the model's exits sell
`qty` where live sells one lot less, and criteria B and C cannot pair exits
honestly until the simulator can take a buy's fee in the base asset (§4).

One consequence for sizing. With the fee taken in the base asset the exit is a
lot smaller than the entry, so a `qty` within about a lot of the venue's
minimum notional produces an exit the venue refuses on every down-cross, and a
strategy still holding a lot never trades again. The size must clear the
minimum notional by more than one lot plus the expected adverse move. M5's
0.001 BTC clears it about fifteen times over, and the live binary refuses a size
that does not.

**Not a measurement of queue position or market impact, and four documents
currently say otherwise.** `docs/engine-contract.md` §8, `docs/overview.md`,
`docs/paper-run.md` and `CLAUDE.md` all say these are unmeasured "and only M8 can
measure them". That is true of the milestone number and false of this milestone:
`MaCrossover` sends `OrderKind::Market` exclusively
(`crates/quant-backtest/src/ma.rs`), a market order never rests, and an order
that never rests has no queue position. Market impact at the venue minimum is
likewise unmeasurable — a $5 order moves nothing, which is precisely why it is
safe to send.

Measuring either needs a **resting, passive** strategy and enough size to matter,
which is a different milestone with a different risk profile. M8 measures the
*adapter*. The four claims are corrected in the same commit as this document,
and the old phrasing grepped for afterwards — the rule PRs #25 and #26 cost us.

**Not a fortnight.** M5's duration was load-bearing: a two-week unattended run
*was* the criterion, because "does this survive being left alone" was the
question. M8's questions are answered by specific events, not by elapsed time,
and most of them can be provoked deliberately in an afternoon. Capital at risk
for two weeks to re-learn that a supervisor restarts things is a bad trade.

**Not multiple symbols, processes or accounts.** Two paper processes were
independent because two `SimulatedVenue`s are. Two live processes share one
exchange balance while holding two private `Portfolio`s and two daily-loss
budgets, neither of which can see the other's fills — so each would permit its
full budget against a balance the other is also spending. One symbol, one
process, one account, to be enforced by a live mode of `ops/start-run.sh` rather
than left to care. That mode does not exist yet: `supervise.sh` accepts only
`record` and `paper`, and `start-run.sh` defaults to two symbols. Slice (c)'s last
pull request adds it, tested the way `ops/test-supervise.sh` tests the supervisor.

**Not a second way to fold our own records — but `runlog diff` as it stands
cannot be the criterion, and saying otherwise would have made M8 vacuous.**

This section originally refused new comparison code on the grounds that
`runlog diff` already does it. It does not. `decisions()`
(`crates/quant-explain/src/runlog.rs`) matches `Submitted` and `Refused` and
sends everything else to `_ => None` — so of fourteen `JournalEntry` variants it
compares two, and **every entry the venue produces** (`Filled`, `Accepted`,
`Rejected`, `Cancelled`, `Orphaned`) is excluded.

Now ask M5.c's question of M8. Live is `LiveSource + LiveVenue`; replay is
`HistoricalSource + SimulatedVenue` over the capture the live run tee'd. Strategy,
engine, risk layer and event stream are identical by construction. **The only
component that differs is the venue — and the venue's entire output is the half
the diff excludes.** `runlog diff` would print `AGREE` because nothing in it
could have made it fail. That is M5.c's vacuous identity for the sixth time, and
it would have been the acceptance criterion rather than a debugger.

It is also the same defect `runlog diff` already shipped once: §7 of
`docs/run-log.md` records it comparing 786 decisions from two genuinely different
strategies and finding every one equal, because the discriminating field had been
left out. One field-class further out, and this time load-bearing.

So the diff is **extended to the venue-produced entries** at M8, and the
by-construction argument that demoted it at M7.5 genuinely inverts here — the
venue now differs, so fill lines are no longer forced to agree. What stays
refused is a second way to fold our *own* records against each other:
`reconcile` and `runlog check` keep that job.

**Not new order types.** `Market` and `Limit`, `Gtc` and `Ioc`. Post-only and
stop-loss will be proposed because the venue offers them; both are refused,
because a type `SimulatedVenue` cannot match is a type no diff can cover, and the
diff is the criterion.

---

## 3. The acceptance criteria

**The inherited sentence is one criterion doing two incompatible jobs.** "Live
fills reconcile to the paper model within tolerance" blurs two comparisons with
opposite standards, and keeping them blurred is how a run produces a number
nobody can argue with.

M5's criterion was **exact**, and that was forced rather than earned: both sides
were `SimulatedVenue` consuming identical events with identical stamps, so
equality was structural. M8 compares a thing against a *prediction* of the thing.
A real venue fills at a price no simulation chose. No amount of engineering makes
that exact, and a criterion demanding it would fail on a healthy run — the
verifier-cries-wolf failure this project has already paid for once.

So three criteria, with three different standards.

### A — our record and the venue's record describe the same events. Exact.

Every `Filled` entry in the journal appears in Binance's `myTrades` for the
window with the same venue order id, price, quantity, commission and commission
asset; every trade in that list appears in the journal; and **total equity**,
marked at one stated price, agrees to the satoshi with the venue's two balances
marked at the same price.

Equity, not "cash and position agree". `Portfolio` holds one cash number and
charges fees in the quote currency (M4's named simplification); a spot venue
takes the fee in the **base**, so after a buy our cash and the venue's quote
balance legitimately differ by the fee while the equity totals do not. Making
them agree field by field means giving `Portfolio` two balances, which is real
`quant-engine` scope and changes every backtest's arithmetic. **That is a
decision to take explicitly, not one to acquire by implication from a
reconciliation criterion**, so M8 compares equity and leaves the two-balance
question open.

**Zero tolerance, because both sides describe the same events.** This is
`reconcile`'s two-independent-computations pattern with an external second party
for the first time, and it is the strongest check this platform has ever had
available: the venue has no access to our arithmetic and no interest in agreeing
with it.

*What makes it fail:* one `Orphaned` entry. A missed report. A fee booked in the
wrong currency — M4 charges fees in the quote currency and named that a
simplification; a spot venue takes them in the base, and this is where that bill
arrives. A quantity rounded differently on the two sides.

*Sabotages that must redden it:* delete one `Filled` line; perturb one fee by one
raw unit; move a balance by hand outside the record.

### B — the model predicts the right *kind* of outcome. Exact on category.

Pair live and model by `ClientOrderId` across the whole order tape. The pairing
must be **total** — an unpaired order on either side is a failure, not a rounding
— and for every order, accepted-or-rejected must match, filled-or-unfilled must
match, and where the venue rejected, the rule it cited must be the rule
`Filters::rejects` names.

A model that accepts what the venue rejects is wrong about the venue's *rules*,
which is a different kind of error from being imprecise about its prices, and it
is the likeliest first failure: `tick_size`, `lot_size` and `min_notional` are
currently hard-coded literals in several call sites rather than fetched from
`exchangeInfo`. This criterion is what falsifies them.

*What makes it fail:* a filter literal that disagrees with the venue's current
value. A rate limit we do not model. An order rejected for insufficient balance,
which `SimulatedVenue` never checks.

### C — the model's error is small, and bounded by a number registered in advance.

For each paired fill, `edge = model_price − actual_price`, signed in ticks.
Report the distribution, its median, its dispersion and its tail — not a single
number.

**This originally read "the model is not optimistic" and asserted the sign of the
mean. That was wrong, and `docs/engine-contract.md` §8 had already settled why:**
a model's fill lands at a *different book*, and over any horizon longer than the
latency the sign of that move is a coin flip. §8 says in as many words that
*"latency must never improve results" would have been a wrong criterion*, and the
M4 tests deliberately assert no direction. A criterion predicting the sign would
declare a bug on a correct system — the cry-wolf failure M1.d2 charged us for
once already.

What the residual **can** falsify, as three properties rather than one sign:

1. **The distribution is tight.** The large majority of fills land within a
   stated number of ticks of the counterfactual, with the tail attributable to a
   recorded reconnect or gap. Anything wider means the recorded book is not what
   Binance matched against, which is the premise every backtest in this
   repository rests on.
2. **Fees agree per fill**, against the venue's own `commission` and
   `commissionAsset`. A genuine second independent computation, and the one
   place M4's quote-currency simplification shows up as a number rather than a
   caveat.
3. **Every live fill has a counterfactual at all** — no fill lands at an instant
   where the replayed book was invalid. A fill we cannot model is not a small
   error, it is an absent comparison, and averaging over the ones that worked
   would hide it.

**The bound is pre-registered, and it is derived rather than chosen.** The first
draft of this document registered *1 basis point, one tenth of the taker fee*.
That imports M4's **economic** scale into a **microstructure** measurement, and
they are three orders of magnitude apart. At this repository's own fixture price
and `tick_size: "0.01"`, one tick is 0.0013 bps; the sabotage named to redden the
criterion — a model filling at the mid rather than the touch on a one-tick spread
— moves the mean by 0.00065 bps. Against a 1 bps bound that sabotage **cannot**
redden it, which is the project's own convention failing at the pre-registration
stage rather than in code.

So the bound comes from the artifact we already have. **Before M8 starts**, take
the M5 capture's 824 BTCUSDT submissions, compute the distribution of mid-price
change over a plausible round-trip window across those instants, and register the
bound as a stated multiple of that dispersion *and* in ticks — the grid the
model's real error lives on. 1 bps stays in the document as the **economic**
backstop it actually is, not as the microstructure bar it was pretending to be.

And the run prints the smallest bias its sample could resolve. If that is coarser
than the bound the verdict reads *"passed at the resolution available"*. The case
that will actually occur is the opposite — resolution far finer than an economic
bound — which is exactly why the bound must be set against dispersion rather than
against a fee.

### D — every state of the lifecycle was reached deliberately.

A written checklist, each state provoked on purpose and each producing the right
`ExecutionEvent` carrying the right `ClientOrderId`:

| | state | how it is provoked |
|---|---|---|
| 1 | accepted | an ordinary order |
| 2 | filled in full | a market order at minimum size |
| 3 | partially filled | a limit order larger than the touch |
| 4 | cancelled | cancel a resting limit far from the market |
| 5 | cancel loses the race | cancel a marketable order |
| 6 | rejected, filter breach | an order below `min_notional` |
| 7 | rejected, insufficient funds | an order larger than the balance |

**An unreached state is a failure, not a pass.** "Nothing went wrong" is not "the
path was tested" — M5.c's lesson arriving in a new place, and the reason this is
a criterion rather than a hope. Partial fills in particular have never once
occurred in this system's history: `SimulatedVenue::provide` fills a resting
order's entire remaining quantity when the market trades through it.

### E — a crash in the indeterminate window is survivable.

Kill the process between the journal's write-ahead `submitted` line and the
venue's response. On restart the system must look the order up by its client
order id, match it to the journal entry, and resume, or, if the venue has no such
order, record that the request never landed. It must not come back with
`MaCrossover.working` latched forever on an order nobody will ever hear about
again.

**The lookup is `GET /api/v3/order` with `origClientOrderId`, not `openOrders`.**
This criterion said `openOrders` until slice (c) read the venue's documentation
closely, and `openOrders` gets the likeliest case backwards. A market order that
landed has already filled, so it is no longer open, and recovery would record
that a filled order never landed: an engine that believes it is flat while the
venue holds the position. `GET /api/v3/order` is documented as *"Check an order's
status"*, with data source *"Memory => Database"*, so it finds a filled order as
readily as an open one.

It does not find every order by the id it was placed with, and this paragraph
used to say it did, so slice (d) must not rely on that sentence. Two exceptions
are documented. A cancel replaces the cancelled order's `clientOrderId` with the
cancel's own `newClientOrderId`, generated when none is sent, *"freeing it up
for new orders"*: stated for the WebSocket API's `order.cancel`
(web-socket-api.md:4142, :4181), while REST's `DELETE /api/v3/order` takes the
same parameter and says only that it identifies the cancel (rest-api.md:2396).
So after a cancel, ours included, the id the order was placed with may find
nothing. And an order cancelled or expired with nothing filled is archived
after 90 days, and asking for it then is answered `-2026 ORDER_ARCHIVED`
(errors.md:347-348).

Its "no such order" is conclusive only once the request can no longer reach the
matching engine, and that bound is slice (d)'s question, not settled here. The
venue checks `recvWindow` when it *forwards* a request, which bounds when one can
be forwarded but not when the matching engine gets to it. The documentation's
own `-1007` timeout is a response that took longer than ten seconds, and it says
that this *"does not always mean that the request failed in the Matching
Engine"*.

---

## 4. Slices

| | Slice | Content |
|---|---|---|
| a | The engine can receive what a venue says | **done** — wake on either input (`next_event_timeout`), forward-only clock. `LiveSource`'s half was missed (§1) and landed in `m8a-live-source-idles` (#52) |
| b | Credentials, signing, and refusing to run without them | **done** — `Credentials`, `TradeClient`, `venue-check`; reworked for Ed25519 in slice (c), `m8c-ed25519-credentials` (§5) |
| c | `LiveVenue`: submit, cancel, and the user-data stream | **part done**: `FeeAsset`, the translator (`user_data.rs`), the order encoder and classifier (`orders.rs`), the order transport (`TradeClient::place_order`, `cancel_order`), the instrument from the venue (#60), the engine seam (#65), the account stream's frame kind (#63), the journal's entries (item 4, `m8c-journal-live`), item 4's readers (`m8c-journal-readers`) and the account listener (item 6, `m8c-account-stream`) and item 7, its merge machine (`m8c-live-machine`) and its shell (`m8c-live-shell`); and item 8's binary, 8a (`m8c-live-binary`). Item 8's `ops/` live mode (8b) and its Spot Testnet run (8c) remain |
| d | The indeterminate window | `InDoubt`, recovery by `origClientOrderId` |
| e | Reconciliation against the venue | criteria A, B, C as a binary |
| f | The run, and the lifecycle checklist | criterion D, criterion E |

### Slice (c), as pull requests

Mapping what `LiveVenue` has to fit turned one item into about ten pull requests.
Six independent readers covered the engine seam, the journal, the live
transport, the raw tier, Binance's documentation (at commit `0556f1a`) and this
document, a seventh cross-checked them, and every consequential finding was then
re-read in the source by hand. Each pull request is a branch of small green
commits whose acceptance criterion can fail, with every new test seen red before
it was trusted.

**First, the defects the mapping found in shipped code.** None of them waits for a
feature.

| branch | what it fixes |
|---|---|
| `m8a-live-source-idles` | `LiveSource` never overrode `next_event_timeout`, so the idle wake never ran in production (§1); and venue reports advanced the clock on idle wakes only (§5) |
| `m8c-journal-recovery` | `journal::replay` re-stamped every fee as `Quote`, which PR #50 fixed in the writer and missed here; a restart registered placeholder filters ahead of the real ones; a torn line followed by a restart made the journal unreadable |
| `m8c-risk-reduce` | a partial reduction of an oversized position was refused, against `docs/engine-contract.md` §10, and the test named for the property sold the whole position, so it could not see the gap |
| `m8c-exit-sizing` | the exit sold a fixed quantity against a position the fee had shrunk (§2) |
| `m8b-venue-check-offset` | `venue-check` subtracted half the round trip where the midpoint estimate adds it, so the clock gate was off by a full round trip |

**Then slice (c) proper, in dependency order.**

1. **Credentials and requests.** Ed25519 keys (§5); signed `POST` and `DELETE`
   that keep the response body; an encoder from `OrderRequest` that never sends
   `timeInForce` on a market order and always sends `newOrderRespType=ACK` and the
   run-tagged `newClientOrderId`; and a classifier sorting every response into
   *placed*, *refused* (with its code and message) or *unknown*. Any 5xx, `-1007`
   and `-1006` are unknown, never refused. Accepted when the one Ed25519
   signature Binance publishes together with its private key reproduces byte for
   byte, the WebSocket API's documented signature payload is rebuilt exactly,
   and a table test maps every documented code to its class.

   That signature is the FIX API's Logon example (fix-api.md:287-312), and it is
   the only one that can be reproduced. The REST page's two Ed25519 examples are
   not Ed25519 signatures: each is 256 bytes, an RSA-2048 signature's size where
   an Ed25519 signature is always 64; the command beside them,
   `openssl dgst -sha256 -sign`, is one OpenSSL refuses for an Ed25519 key; and
   the second is byte for byte the RSA section's own (rest-api.md:477, :633,
   :641). The WebSocket API's is a genuine 64-byte Ed25519 signature, but under
   a private key the page names only as a file and never prints
   (web-socket-api.md:1067, :1177), so its payload is pinned and its signature
   can only be checked by a round trip.

   The credentials half is `m8c-ed25519-credentials` (#58): the Ed25519 key
   read from the environment; one signer, shared by REST and the WebSocket API,
   that reproduces the FIX vector; the REST query percent-encoded as
   rest-api.md:644-650 shows and verified as it went over the wire; the
   WebSocket API signer `session.logon` needs; and `venue-check` on the new key.

   The requests half landed in two. `m8c-order-encoding` (#59) is the encoder
   and the classifier, `quant-binance::orders`. `m8c-order-transport` sends
   them, `TradeClient::place_order` and `cancel_order`, and enforces four things
   the classifier depends on, each pinned by a test seen red: the encoded query
   goes out verbatim with an empty body, no redirect is followed, *not sent* is
   claimed only for a connection never made, and an order waits
   `ORDER_TIMEOUT`, 20 s, above the venue's ten plus `recvWindow`, so that the
   venue's `-1007` arrives first. With it `venue-check` proves the key's
   permission to trade with `POST /api/v3/order/test` (`TRADE`, weight 1,
   rest-api.md:2321-2333), which its `USER_DATA` request could not. READY needs
   the venue to accept that probe, the run's own order, outright: any refusal of
   it, a filter failure included, is NOT READY, because the run's orders would
   meet it. And it refuses READY while the account pays its fees in BNB: the
   translator books a fee only in the pair's base or quote asset, so on a pair
   without BNB it would refuse every fill whose fee was taken in BNB, and the
   run procedure turns the discount off.

   **The code for item 1 is done.** What remains is meeting the venue, on Spot
   Testnet first: none of it has met Binance yet, and the testnet rehearsal
   (§5) is its first real test, before any order on the live venue.
2. **The instrument comes from the venue.** `exchangeInfo` parsed into an
   `InstrumentDef`, naming the venue rules our `Filters` do not model
   (`MARKET_LOT_SIZE`, `applyMinToMarket`, `maxNotional`). Accepted when the
   documented response parses exactly and a missing asset is an error naming the
   field rather than an empty string. *As built (`quant-binance::exchange_info`),
   three things differ.* That acceptance could not fail on a filter: the
   documented response's filter list is empty (`rest-api.md` lines 857-860), so
   a parser that never read one passes it, and the examples that do carry
   filters give `minPrice` the tick's value, `minQty` the step's, and `LOT_SIZE`
   and `MARKET_LOT_SIZE` one step between them (`filters.md` lines 54-56,
   128-130 and 213), so they cannot catch a parser reading the wrong field
   either. Acceptance therefore also takes every `filters.md` example and
   fixtures in which those differ. The naming is not limited to three: every
   field of every filter the definition does not take is reported under the
   venue's filter name, which is the name a rejection carries, exchange-wide
   filters included. So is every rate limit, whole: `ORDERS` is a rule about
   orders the model does not have, and criterion B names a rate limit we do
   not model as a way it fails. So are the symbol's precisions, which bound
   the decimals an amount may carry and set the decimals a fee is calculated
   to. And nothing is persisted: criterion B's replay must judge the model
   against the rules the run used, which a fetch at judging time need not
   return, so how a run writes them down belongs to item 4.
3. **The engine seam a live venue needs.** A readiness check consulted before an
   order is journalled (§5), venue detail delivered to the observer and never to
   the strategy (§5), and a final drain at shutdown. Accepted when the simulator
   reproduces every existing test and M5's money check, and a strategy's view of
   a lifecycle is identical whichever venue produced it.

   **Built** as `m8c-venue-seam`, the argument in `docs/engine-contract.md` §11.
   A required `ready`, consulted after the seam and before risk; a
   `WriteAhead` answer from the record, latched on its first `Withhold`;
   `poll` handing back order reports with the venue's detail beside each event
   and notices for facts that are not events, the strategy told only the event;
   the venue's order id gone from `Accepted`, with a compile-time check keeping
   anything like it out; reports about orders the engine is not working withheld
   from the strategy; and `Engine::settle`, a venue-only wake for the binary to
   loop on at shutdown. Both criteria met: every test and the money check
   (824 fills, cash 31.64743842) after each commit, and one lifecycle reported
   by a plain venue and by a talkative one looks identical to the strategy.
   What the new facts are written down as is item 4.
4. **What a live run writes down.** The run tag on `Started`; venue order and
   trade ids; entries for an orphan from another run, someone else's trade, losing
   and regaining the account stream, a refused cancel, and a rejection's code;
   journal schema 3, with `read` refusing a schema it does not know. Three reader
   debts come due here too. The market's return after a gap must be recorded
   explicitly, because once a live venue speaks on idle wakes a fill after a
   `blind` line no longer proves the market came back, which is what `explain`
   assumes. An `orphaned` line must be loud in `runlog check`, which when this
   was written never read it. And lines torn before a restart must show where
   an operator looks, not only in `reconcile`'s full report. Finally, a restart
   must bring the risk layer's tally back from the journal. `RiskEngine::recover`
   restored the kill switch and nothing else. So after a restart an addition to
   a real position was checked as if from flat, a real exit could be refused as
   if it opened the opposite position, and the day's loss budget started fresh. The engine
   contract's own rule, that an NTP step cannot hand a stopped strategy a fresh
   budget, already argues against that last one.

   **The entries are built** as `m8c-journal-live`, the list in
   `docs/run-log.md` §8. Journal schema 3, and `read` refusing a session begun
   in a newer one, naming both numbers. The run tag on `started`, the first
   line's being the session's. The venue's names and words on the lines of the
   events they describe: `filled` carries `remaining`, the venue order id and
   the trade id, so one line is one `myTrades` row; `rejected` carries what the
   venue cited and the filter it named; `accepted`, `cancelled` and `orphaned`
   carry the venue's ids, and `cancelled` how the order ended. `stopped`
   carries the orders still working. Eight entries for what a venue says on
   its own (`placed`, `never_landed`, `doubt`, `cancel_refused`, `foreign`,
   `account`, `anomaly`, `stray`), mapped from every `Notice` by one
   exhaustive match in the writer. The market's return as a `sighted` line,
   which `explain` reads in place of its inference from schema 3 on, so the
   first of the three reader debts is paid. The account's `balances` and the
   constructor the live binary calls at start and stop. And
   `JournalWriter::strict`, which stops writing and withholds every order from
   its first failure. Every schema-1 and schema-2 journal reads unchanged, and
   both of the fortnight's still reconcile.

   **The reader debts are paid** in `m8c-journal-readers`, the argument in
   `docs/run-log.md` §9 and `docs/engine-contract.md` §10. `runlog check` fails
   on an `orphaned` line, a `stray` line, another run's or an unparseable
   `foreign` order and a doubt left raised, and counts a stranger's order
   without flagging it. A line torn before a restart rides on `reconcile`'s
   verdict line, which `ops/verify-loop.sh` copies into its log and
   `ops/status.sh` shows, is a finding in `runlog check`, and is printed by
   `explain`. A restart recounts the risk tally from the journal
   (`journal::recover_risk`, which `paper` now calls): the holding, and the
   day's order count and loss, with a new UTC day resetting the counters and
   never the switch. And `decisions_at` filters by stamp rather than stopping
   at the first later entry, so `explain --at` reads a `backtest --journal`
   file again, while still reading every `started` line for its schema, so a
   schema-3 backtest is not read by the old rule. Both fortnight journals
   still reconcile and agree at every checkpoint, and `runlog check` still
   finds nothing to check in them.
5. **A frame kind for the account stream.** Container version 3, and the account
   stream under its own data root (§5), where `quant-verify` runs its container
   checks and not the update-id chain. Accepted when a v3 build reads every v1 and
   v2 file on disk and `quant-verify` still exits 0 on `data/paper` and
   `data/acceptance`.

   **Built** as `m8c-account-frames` (#63), the argument in
   `docs/data-contract.md` §6. Frame kind 3 is admitted only in a v3 container,
   and each control record only in the version that introduced it, so a v2 file
   holding one is corruption rather than an account frame. Account sessions
   live under `<root>/account/`, which `catalog` reads on its own; the session
   mints its own id; `quant-verify` runs the container's checks there and says
   it skipped the update-id chain; and `normalize` refuses an account session
   outright. Met: under the v3 reader `quant-verify` exits 0 on both captures
   with the same frame counts as before, 97,937,822 and 70,545,346.
6. **Listening to the account.** `session.logon`, then `userDataStream.subscribe`,
   over the WebSocket API; resubscribing on every reconnect and on
   `serverShutdown`; every recordable frame written to raw before it is
   translated. Accepted against a local WebSocket server: recorded bytes equal sent
   bytes, and a dropped connection produces a `Gap`, a reconnect and a second
   subscribe.

   **Built** as `m8c-account-stream`, the argument in `quant-binance::account`'s
   module docs. `account::listen` connects to the WebSocket API, sends
   `session.logon` signed by the Ed25519 key over a timestamp on the venue's
   clock (the host's less the measured offset, as every order is stamped, which
   `listen` takes as an argument), subscribes, and drains, recording every frame
   it may through `Ingress::accept_account_at` before it acts on the frame.
   Every new connection logs on and subscribes again, after the market's backoff
   seeded by the account stream's name. Accepted against
   `quant-binance::fake_venue`, a local WebSocket API that verifies the logon's
   signature with a payload builder of its own and judges its timestamp. The
   recorded frames, read back with `quant-storage`'s reader, equal the venue's
   byte for byte and in order, with `ingest_seq` contiguous. Neither the key,
   nor a signature, nor an answer to a logon or a status is in the files. A
   dropped connection and a `serverShutdown` each produce a `Gap`, a reconnect,
   a second logon and a second subscribe. And `quant-verify` passes the tree
   with no findings (`crates/quant-verify/tests/account_stream.rs`). Four things
   differ from the plan.

   - **Liveness is a question the venue answers, not a timeout.** The plan
     named none, and the market's rule, applied to events, would reconnect a
     quiet account every two minutes and record a gap each time that is not
     real. The market's rule counts every frame, pings included, so the
     venue's 20-second pings would hold the socket open, but a ping proves the
     socket and not the subscription (§5). So once subscribed the listener
     asks `session.status` every 30 s and needs `userDataStream: true` within
     ten seconds, the venue's own request timeout. An unanswered probe ends
     the connection, and so does `false`. Tested both ways: a quiet account,
     with no events and no pings, is never reconnected, and a venue that stops
     answering while it still pings is found.
   - **A failure has one of two outcomes.** A refusal that will not clear by
     itself stops the listener and says why: a bad or revoked key, a bad
     signature, a timestamp outside `recvWindow` in either direction, a ban, a
     refused subscription, `-1016` (*"This service is no longer available"*),
     an HTTP status refusing the WebSocket upgrade itself (a 418 or a 403), or
     no account subscriptions on that API. Anything else (a reset, a timeout,
     a 5xx, a 429, a busy venue) ends the connection and is retried, and a
     subscribed connection's end is one gap however many attempts the
     reconnect takes. The backoff resets on a subscription, not a connection,
     so a venue that accepts the TCP connection and refuses the logon is
     retried on a curve. `eventStreamTerminated` and a status of
     `false` end a connection as a closed socket does.
   - **The consumer reads an `AccountFeed`.** The plan named the recording and
     not its reader. The listener writes through a `TeeSink` with the capture
     primary, as paper's market path does, and `AccountFeed` reads the
     secondary: each frame as the file holds it, and a `Blind` for each
     recorded gap and for each hole the tee left, always before the frame
     that revealed the hole, and at once when no frame comes to reveal it:
     the tee marks the last record it dropped, and the feed reads the mark
     when its channel is empty. A gap record held for a full capture goes out
     on the next probe rather than with the next frame. What the consumer
     misses is the tee's drops and the capture's own together. Item 7 is
     built against it.
   - **§5's recording rule is applied by request.** No outbound frame and no
     answer to `session.logon` or `session.status` is recorded. An answer is
     known by the request it answers, read from its id, which names it in a word
     (`logon-1`, `subscribe-2`, `status-3`), so a recorded answer says what it
     answers without the listener's memory. A refused subscription and the
     unsolicited revocation notice are recorded. And under the rule, a guard on
     the bytes withholds any frame that contains the API key, whatever it claims
     to be.

   Not done, and named: opening the new connection before closing the old on
   `serverShutdown`, which would avoid the gap at the price of both
   subscriptions delivering the same events for a moment, a deduplication
   item 7 would own; and waiting out a 429's `retryAfter` beyond the backoff.
   For the testnet rehearsal to settle: the live API is remembered refusing an
   id outside `^[a-zA-Z0-9-_]{1,36}$` with `-1100`, which the pinned pages do
   not say. The ids keep inside it either way. No binary runs the listener yet
   (item 8), which must hand it the same measured offset the order path takes
   off, and none of it has met the venue.
7. **`LiveVenue`.** A state machine merging REST outcomes with stream reports: one
   terminal event per order, fills deduplicated by trade id, the in-doubt state
   (§5). Behind it, `impl ExecutionVenue`. Accepted by a permutation test over
   every interleaving of the REST reply and the stream's reports, and by a test
   that losing the account stream puts every working order in doubt
   (`account_blind`), which `runlog check` learns to verify: an order working
   when the stream was lost and never put in doubt for it is a finding.
   Designed before the code in §5, *LiveVenue, decided before the code*.

   **The machine and the shell are built; item 8 is next.** `m8c-live-machine` is the pure
   half: the crate `quant-live` and its `Merge`, which takes what was heard and
   appends what the engine is told, with the readiness answer; the
   translator's and the order classifier's mappings onto the record's types;
   `journal::diverged`, which reads the latch back off the journal; and
   `runlog check`'s seventh finding, `unwatched`. Accepted by the permutation
   test, 1,547 interleavings of 47 truths: each of the 43 consistent truths
   tells the strategy the same events and writes the same lines in every
   interleaving, `runlog check` names an order in doubt when `ready` says
   `InDoubt` and never when it is Ok, and the four contradictions latch in
   every one; in all
   of them the journal agrees with a checkpoint of the engine's portfolio.
   Also by `account_blind`; by a test that the machine's latch and
   `journal::diverged` agree; and by the machine's own table, every row seen
   red under its break. What building it corrected is in
   §5, *LiveVenue, decided before the code*, §8: among it, the permutation
   test's first run found `runlog check`'s doubt fold disagreeing with itself.
   `m8c-live-shell` is the rest of it: `LiveVenue` behind `ExecutionVenue`,
   built by `quant-live::wire` from the instrument's id and venue-given
   definition, the run tag, a `Checked` (`TradeClient::checked`, the one way
   to mint it), the trade client, the `AccountFeed` and the one clock; the
   request worker, one REST request at a time behind a queue of four, with
   decision 6's `Unheard` timer; held cancels; and `quant-live::drain`, the
   two waits of the shutdown, each with its own deadline. The engine thread
   never waits on any of it. Accepted by the shell's own tests against a
   scripted local REST server and a `ManualClock`, and by ten end-to-end
   tests in `quant-explain` (`tests/live_venue.rs`) through the real
   engine, journal writer, listener, tee and feed: a market buy placed,
   accepted and filled with the venue's ids on every line, `runlog check`
   complete and the closing checkpoint agreeing; a REST refusal told as a
   rejection alone with the venue's code; a timed-out order in doubt until
   the stream clears it; a full queue answering `QueueFull` and a held
   cancel going before a later order; the `Unheard` timer raising
   `fills_missed` on an order never mentioned and not on one the stream
   reported; a shutdown with a worker that never answers ending at its
   deadlines with the order in `stopped`, and one after the runtime has gone
   not hanging and leaving the order the worker had sent in doubt, which
   `runlog check` reads as in doubt rather than unwatched; the account lost
   through the real feed; and the same
   lifecycle telling a strategy the same events through this venue and the
   simulator. Every test was seen red under its break, and every socket test
   passed 20 runs in each of 6 copies at once. What it corrected is in §5,
   *LiveVenue, decided before the code*, §8. Item 8, the binary, is next.
8. **The `live` binary and a rehearsal.** Paper's session extracted so any venue
   can run it; a startup that records first and trades last; a refusal of any
   size whose floored exit would fall below the minimum notional at the current
   mark (§2); a live mode in `ops/` enforcing one symbol. Accepted when a run on Spot Testnet (§5) reaches
   criterion D's states 1, 2, 4 and 6, `reconcile` agrees, `quant-verify` is clean
   on both data roots, and replaying the recorded account frames through the
   translator reproduces every venue-produced journal entry.

   It is three pieces of work, and the first is done.

   - **8a, the binary, is built** (`m8c-live-binary`), rehearsed against local
     stand-ins for every endpoint: `quant-live::fixtures::rest` for REST,
     `quant-binance::fake_venue` for the account side and
     `quant-binance::fake_market`, new, for the market stream. Paper's session
     is `quant-backtest::session`, and `paper` runs it unchanged, proven by
     M5's own criterion: a rehearsal of paper's session against the local
     market, normalized and backtested, reproduces its fills and cash exactly.
     `Engine::cancel_working` is the stop's cancel through the record
     (`docs/engine-contract.md` §11). The binary is `live`, its body
     `quant-backtest::live::run`: `--env testnet` by default and `--env live`
     only when typed, every endpoint overridable for a local stand-in on a
     loopback host and nothing else; seven
     gates before the capture, any of which fails into recording only; the
     capture of both streams under one clock; and §1's shutdown order, the
     closing lines only while the strict writer stands. The account replay
     check is `replay-account` (`docs/run-log.md` §10). Accepted against the
     stand-ins by `quant-explain`'s `tests/live_run.rs`: a full rehearsal that
     passes the gates, trades one round trip, cancels nothing, and ends with
     `reconcile` agreeing, `runlog check` complete, `quant-verify` clean on
     both roots and the replay check agreeing (and disagreeing with a fill
     deleted, a fee moved by one raw unit, a stamp by a nanosecond, or the
     capture taken away); each gate failing in turn, recording
     and never trading; a restart onto a journal that latched `Diverged` and
     one after a stop that left an order working, each recording only; a stop
     that cancels a resting order and settles its end; a journal that fails,
     after which no order goes and no closing line is written; and a size
     whose floored exit is under the minimum, refused. Every test seen red
     under its break; what building it corrected is in §5, *LiveVenue,
     decided before the code*, §8.
   - **8b, the live mode in `ops/`, remains**, and needs the owner's decision
     first: a PowerShell port or a WSL port of the bash harness for this
     Windows host (*Where it runs*, below). Either way it enforces one symbol,
     one process, one account, and is tested as `ops/test-supervise.sh` tests
     the supervisor. The binary exits 3 on a refused gate, so 8b's
     supervisor must end supervision on 3 as on 0, or it restarts the run
     into the same refusal; `supervise.sh` as it stands restarts on any code
     but 0, and would.
   - **8c, the Spot Testnet run, remains**, and needs two things from the
     owner: this host's clock fixed (it read 2.1 s ahead of Binance with
     `w32time` stopped, and the venue-checks gate refuses past 1 s), and an
     Ed25519 testnet key pair registered with Spot Testnet, loaded as
     `venue-check --help` says. It confirms the testnet market stream's
     address, which 8a took from memory (`SPOT_TESTNET_WS`), and its run is
     this item's acceptance.

Outside slice (c): resolving an in-doubt order by lookup is slice (d), criteria
A, B and C as a binary are slice (e), and the run with criteria D and E is slice
(f). Slice (e) needed one model change first, and it has landed (#64):
`SimulatedVenue` takes a buy's fee in the asset bought when configured to
(`backtest --fee-asset received`, §2). It is off by default, so every backtest
and M5 still reproduce. The model replay for criteria B and C runs with it on,
or it predicts some exits a lot bigger than the ones live sends: not every exit,
because one trip's dust tops up the next buy, but over M5's fortnight 42 of 412.

### Where it runs, which is not yet settled

M5 ran on the MacBook Air, and `ops/`'s bash half is the only harness with a
paper or live mode — the `.ps1` half is **record-only**, deliberately, because
*"an untested paper mode there would be exactly the harness-never-run M1 warns
about"*. So an M8 run on the Windows box needs WSL or a PowerShell port that is
then rehearsed, and this used to say the bash harness runs under WSL unmodified.
It does not: it has only ever run on macOS, and it uses BSD `date -u -r EPOCH`
(`start-run.sh`, `supervise.sh`, `verify-loop.sh`), `caffeinate` (`supervise.sh`)
and BSD `stat -f` (`status.sh`), none of which behaves the same under GNU. Either
route is a slice of work in its own right and must not be written the night
before a run.

The clock constrains the choice more than the harness does. Slice (b) made a
host more than 1000 ms from the venue **unable to trade at all**, where M1 had
only warned about it — and M1 declined to start its acceptance run on the Windows
box partly because `w32time` was stopped and it sat ~2 s ahead of Binance.
Whatever machine runs M8 has to pass `venue-check` first, and that is a cheaper
thing to find out now than on the afternoon of the run.

`docs/machine-move.md` covers moving the artifacts between the two, and why the
3.2 GB normalized tier is rebuilt rather than copied.

`venue-check` is runnable today with no money at risk: it makes three signed
requests and places nothing. `GET /api/v3/account` exercises the API key, the
Ed25519 signature, the header and the clock in a single round trip and reads
`canTrade`. That endpoint is `USER_DATA`, and *"by default, an API key cannot
`TRADE`"* until trading is enabled for it in API Management, so a key without
that permission passes it, and the venue never defines `canTrade`. So
`POST /api/v3/order/test`, a `TRADE` request at weight 1 that validates a market
buy without sending it to the matching engine, is what proves the permission,
and READY needs it to accept the probe outright. The probe is the run's own
order, a market buy of the run's symbol at the run's size, and nothing else of
`venue-check`'s reaches the request, so any refusal of it is one the run's orders
would meet. A filter failure is NOT READY and names the filter, whether that
filter judges the order's numbers or the account's open orders and position
(filters.md:217-219, :253-260, :331-333). Such a refusal also suggests the key
may trade, since the venue judged the order, but only on a reading of the
documentation that the Spot Testnet rehearsal has to confirm
(`TradeClient::check_trade_permission`), and READY never rests on it.
`GET /api/v3/account/commission` then checks that the account does not pay its
fees in BNB. READY means all three passed. It prints the loaded key's public
half to compare with the one registered, names why a refusal was refused, and
with `--testnet` checks a Spot Testnet pair instead (§5). Under slice (b)'s
HMAC secret, a wrong secret came back HTTP 401, which is the venue's auth layer
answering — proof the request was well formed enough to be judged on its
credentials rather than its shape. The Ed25519 key has not met the venue yet:
on 2026-10-15 this host's clock, 2.4 s ahead of Spot Testnet's, stopped
`venue-check` before it signed anything, so its first run after the clock is
fixed, with a testnet pair, is the first real test of the signer and of the
probe.

Slice (a) is first and is not negotiable: it is a seam change, every slice after
it depends on the guarantee, and it is the one thing that can be built and tested
with **no credentials and no money** — a fake transport is enough. Doing it last
would mean discovering during a funded run that the engine cannot hear.

---

## 5. Decided before the code

**`ClientOrderId` goes to the venue as `newClientOrderId`, and it is a lookup
key, not an idempotency key.** `docs/engine-contract.md` §3 made it exist before
the request leaves, for correlation, which is exactly what finding an order after
a timeout needs. It is *not* what makes a retry safe, and this paragraph used to
say it was: *"a retry after a timeout cannot open a second position, because the
venue refuses the duplicate"*. The venue's documentation for `newClientOrderId`
says otherwise: *"A unique id among open orders"*, and *"Orders with the same
`newClientOrderID` can be accepted only when the previous one is filled,
otherwise the order will be rejected."* A duplicate is refused only while the
first order is still open, and a market order is never open long enough for that
to matter: at our size it fills the moment it lands. So a blind resend of a
timed-out market order is **accepted** precisely when the first one went
through, and the position doubles. A timed-out submission is therefore never
resent. It is resolved by looking it up, per criterion E.

The id carries a run-scoped prefix, `q-<run>-<id>` (`quant-binance::user_data`),
and the same rule makes that prefix load-bearing rather than tidy. The bare
counter restarts at 1 after a lost journal. A reminted id that collides with an
order still resting is refused, loudly; one that collides with a *filled* order
is accepted, silently, and every later lookup by that name is ambiguous. So the
run tag must stay the same for the life of a journal, so that a restart still
recognises its own orders, and must change whenever a journal is lost.

**One authority per fact.** Every acceptance, with the venue's order id, and
every `Filled` come from the **user-data stream**. Rejections come from the REST
response, because the stream reports on orders and a rejected request was never
an order — which is precisely why the venue's order id is optional where it
travels. That is beside the event rather than in it: `OrderReport::Accepted`
carries `venue_order_id: Option<VenueOrderId>` to the record, and the
`ExecutionEvent` the strategy is told carries no venue id at all.
`newOrderRespType=ACK`, so a fuller response cannot deliver a fill the stream
will also deliver.

**The user-data socket is recorded into the raw tier before it is parsed.**
Market data has been immutable-bytes-first since M1 on the argument that our
model of the venue may be wrong. Nothing about execution reports weakens that
argument; everything about money strengthens it. A new `FrameKind`, bytes
verbatim, container version 2 → 3 with a migration note in
`docs/data-contract.md` §6. Without it, a disagreement between our record and the
venue's can only be re-litigated from our own interpretation of what it said.

**`LiveVenue` emits one `Filled` per venue trade, not one per order.**
`SimulatedVenue::take` deliberately collapses a multi-level walk into a single
fill at the size-weighted average — *"a venue reports an execution, and a
strategy that had to reassemble three prints into an average price would be
doing arithmetic the venue already did"*. Binance splits a market order across
levels into several `myTrades` rows. A `LiveVenue` that aggregated to match the
simulator would make criterion A fail arithmetically on the first multi-level
fill, for a reason that is not a disagreement about anything. The venue's rows
are the unit.

**The fee asset is modelled before the run, and it is cheaper than two
balances.** §3's criterion A leaves the two-balance question open; this is the
narrower fix that closes it. `Portfolio::apply_fill` does `cash -= gross + fee`
with the fee in the quote currency, which M4 named as a simplification; a spot
venue takes the taker fee in the **received** asset. So `Fill` gains a
`fee_asset`, `apply_fill` branches on it, and `SimulatedVenue` defaults it to
`Quote` — which is what makes every existing backtest reproduce to the last
digit, the `Costs::NONE` no-op property in a third place.

The identity that makes it checkable: after a buy of `q` at `px` with fee `f`,
our equity at mark `px` is `C₀ − f`, and the venue's two balances marked at the
same price are `(C₀ − q·px) + (q − f/px)·px = C₀ − f`. **Equity agrees whichever
asset the fee lands in; cash and position agree only if the asset is modelled.**
That is why criterion A compares equity as its floor — and why, with
`fee_asset`, it can compare the balances too.

**`LiveVenue::submit` does not await.** Stamp, format, `try_send`, return. The
engine loop is synchronous and must not block on a round trip — that is the whole
of §2's fire-and-forget argument, and the first place it has ever actually
mattered. A full outbound channel is **our** failure, distinct from a venue
rejection, on the same argument that made `RefusedBy` distinguish the seam from
the risk layer. This said it "gets its own `RejectReason`", but that argument
points at `RefusedBy`, not at a reason inside `Rejected`, and slice (c) followed it
there: the adapter refuses *before* the order is journalled (below).

**Venue-sourced filters block submission, not recording.** Criterion B will
probably fail first on a hard-coded `tick_size` or `min_notional`, so the live
path fetches them from `exchangeInfo`. The tempting rule — refuse to start when
that fetch fails — is wrong: at M8 one process both captures and trades
(`paper.rs` runs `capture::run` with the `TeeSink`), so it would stop an
irreplaceable capture over a filter table. `quant-meta`'s rule applies, that
nothing optional may stop a recording. So: **refuse to submit** without
venue-sourced filters, and record regardless. The same shape as a money limit
refusing when there is no mark. Item 7's design keeps the rule and moves where it
is enforced: without venue-sourced filters no `LiveVenue` is built, and the run
records without trading (*LiveVenue, decided before the code*, question 2); and a
`LiveVenue` refuses `NoFilters` for any instrument but the one whose filters it
was built with.

**The engine clock becomes forward-only.** `now = max(now, ts)`, with the clamps
counted. Two asynchronous inputs can deliver out of order; `HistoricalSource` is
monotone, so this must change no backtest number to the last digit — the same
no-op property that made `Costs::NONE` checkable. Mirrors
`quant-recorder::segment`'s never-roll-backwards rule and its `backdated_records`
counter.

Half of this shipped with slice (a). The clock advanced from venue reports on
idle wakes only, so on a market wake a later report was journalled at its own
time while the strategy's clock stayed earlier, and the journal's timestamps
could run backwards. `runlog`'s `decisions_at` then stopped at the first entry
stamped after the instant it was asked about, so one entry out of order hid every
entry behind it. The market-wake half landed in `m8a-live-source-idles` (#52),
and `decisions_at` has since been changed to skip a later entry and read on
(`runlog.rs:825-855`, `docs/run-log.md` §9), for a backtest journal's
wall-clock `started` line.

**The kill switch must stop a live run, and stopping is not flattening.** M6
decided that a tripped switch refuses everything including the order that would
close a position, and named the trade-off: closing out becomes manual. That was
cheap in paper. It is a real decision with real money and it is **not** reversed
here — a switch that trades after tripping is not a switch — but the run
procedure must state plainly that a trip leaves a position open and a human
closes it.

### Decided in slice (c)

Settled while mapping what `LiveVenue` must fit, on the evidence of the code and
of Binance's documentation at commit `0556f1a`. The key type and the rehearsal
were the owner's calls, as was §2's exit rule; the rest follow from the
arguments given.

**The account stream is `session.logon` then `userDataStream.subscribe`, signed
with an Ed25519 key.** The route this project had assumed is gone: the changelog
for 2026-01-21 lists `POST /api/v3/userDataStream` among endpoints that *"will no
longer be available starting from 2026-02-20,07:00 UTC"*. Session authentication
is Ed25519 only (*"Only _Ed25519_ keys are supported for this feature"*), and
`userDataStream.subscribe` *"requires an authenticated WebSocket connection using
Ed25519 keys"*. An HMAC key has one route left,
`userDataStream.subscribe.signature`, which the changelog says *"can work with any
API Key type"* while the method's own example signature is base64, and Binance's
key-type page says *"HMAC keys are deprecated."* So the live path signs with Ed25519, which `ring`, already linked
for TLS, provides. The key pair is generated locally and only its public half is
registered with Binance. The private key reaches the process as its PEM text in
`BINANCE_ED25519_PRIVATE_KEY` rather than as a path, which keeps slice (b)'s
rule that key material comes from the environment and not from a file
(`quant-binance/src/credentials.rs`). Orders stay on REST, per *one authority per fact*: one
WebSocket carrying both would let a single disconnect blind the stream and leave
every in-flight order's outcome unknown at the same instant.

**Losing the account stream is ours to notice; the venue does not announce it.**
`eventStreamTerminated` is sent when a listen token expires, when a logon
subscription is ended by `session.logout`, and when we unsubscribe, and we cause
none of those involuntarily. A lost subscription shows up as transport instead:
the socket closing, a `session.status` probe unanswered or answering `false`, a
`serverShutdown` event, a resubscribe that fails. A subscription belongs to one
connection and nothing is replayed after a reconnect, so every reconnect
resubscribes, and an order working across the blind stretch is in doubt (below)
until slice (d)'s lookup settles it. The venue's pings every 20 seconds prove
the socket, not the subscription; `session.status` reports whether we still
hold one, and is what item 6 built liveness on: asked every 30 s, answered
`true` within ten, or the connection is over. This said "the idle timeout
firing" before item 6, which would have meant a timeout over a stream that is
silent for hours whenever the account trades nothing.

**The adapter refuses before an order is journalled, not after.** By the time
`ExecutionVenue::submit` runs, `on_submitted` has written the order down and the
engine's ledger holds it, so the only way back would be a `Rejected` through
`poll`, recorded as *"the venue declined an order it had been told about"*, which
is false for an order that never left. So every refusal that starts in the
adapter (a full outbound queue, the account stream down, an order in doubt, a
failed clock recheck) is a readiness check the engine consults
in `Context::submit`, after the seam check and before the risk layer. It is
refused as `RefusedBy::Venue`, one of four kinds of refusal, which
`Context::submit` asks in this order: the seam's, the record's, the venue's and
the risk layer's. Like the others it is journalled as a refusal, and it consumes
none of the risk layer's daily count. The check cannot race: the engine thread is the outbound queue's only
producer, so room can only grow between the check and the send.
`ready` is a required method rather than a defaulted one, so that a wrapper
cannot inherit "ready" around a venue that is not; `SimulatedVenue` answers it
always ready and its behaviour does not change. A failed journal append is the
engine's refusal rather than the venue's: the record answers `Withhold`, the
order is refused as `RefusedBy::Record`, and nothing more is sent.

**An unknown outcome is not a rejection.** Until slice (d) exists, a submission
whose outcome is unknown (a 5xx, `-1007`, `-1006`, a timeout after the request was
sent) produces no event at all. The order stays working, the venue marks itself
in doubt, and readiness refuses every new order until the stream mentions that
order or slice (d)'s lookup settles it. `Rejected` would be the convenient lie: a
rejection is terminal and removes the order from the engine's ledger, so the real
fill arriving a second later would be booked as an orphan and never reach the
portfolio. Loud and stuck is the safe failure, and recording carries on
throughout. The client's own timeout sits above the venue's ten seconds, so the
venue's `-1007` arrives first when it can: 20 s, `ORDER_TIMEOUT` in
`quant-binance::trade`, which says why twenty.

**Venue facts reach the journal and never the strategy.** Criterion A needs the
venue's order and trade ids, orphans from other runs, other people's trades on
our symbol, a lost stream, a refused cancel and a rejection's code written down.
None of them may reach a strategy, because a strategy that can tell which venue
it is wired to is a bug. One did, and the seam pull request closed
it: `ExecutionEvent::Accepted` used to carry a venue order id that the translator
filled and `SimulatedVenue` left empty. `poll` now hands back `VenueReport`s, so
venue detail travels beside each event in an `OrderReport`, to the observer
only, and the strategy is told `OrderReport::event()`, whose acceptance has no
venue id to read. The field cannot come back quietly: `quant-core`'s `Modelled`
guard names every field of every `ExecutionEvent` variant and fails to compile
one whose type a simulator cannot produce as a live venue does, and a
`compile_fail` doctest pins that an acceptance with a venue order id does not
build. A strategy's view of every lifecycle is identical whichever venue
produced it.

**The account stream is recorded under its own data root.** Interleaved into the
market capture, an account gap would become a market `Gap` and clear the order
book over a socket that has nothing to do with it, and account frames would
compete for the market's channel. A session under `raw/` would be handed to every
market reader. A second root, `<run-root>/account/`, reuses `Ingress`, `TeeSink`,
`CaptureSession`, `FileStore` and `quant-verify`, so day rolling, trailers and
holes as evidence come for free, and no market tool ever sees the files. Reuse
is not "unchanged", which this said before #63 built it: `CaptureTarget` names
a stream rather than a symbol, `CaptureSession::account` mints the session's
id, ingress takes account frames through `accept_account_at`, and
`quant-verify` checks an account session with the container's checks alone. **No frame carrying key material is recorded**, because raw is kept
forever. Every outbound request is out; so are the responses to `session.logon`
and `session.status`, which echo the API key in their `result`. Those are our
session's bookkeeping rather than facts about the account. Everything else
inbound (events, `serverShutdown`, subscription responses) is recorded verbatim.

**The run tag lives on `Started`.** Minted only when the journal is empty, and
read from the *first* `Started` thereafter, the rule starting cash already
follows. An older build reading the journal ignores an unknown field, where it
would fail on an unknown entry type, and a sidecar file is the second file
`docs/run-log.md` refuses. A live run that finds a non-empty journal without a
tag refuses to trade and records anyway.

**Rehearse on Spot Testnet before any real order.** Every protocol path
(logon, subscribe, place, cancel, reject, reconnect) can be exercised at
`wss://ws-api.testnet.binance.vision/ws-api/v3` with no money at risk, and with
its own key pair. Its market is not the real one, so it cannot stand in for
criteria A, C or D; it is where adapter bugs are supposed to surface first.

### LiveVenue, decided before the code

Written before item 7's code, for the reason `docs/engine-contract.md` was
written before the engine. The seam is fixed (`docs/engine-contract.md` §11,
built in #65). What goes behind it is settled here, because merging two
asynchronous accounts of one order is the part a live run cannot debug. Code is
cited at `f676e6d`, the venue's documentation at `0556f1a`.

**Two pieces, on purpose.** A **merge machine**, `Merge`: no socket, no clock,
no runtime, only a function from what was heard to what the engine is told, and
a readiness answer. Every decision that touches money is in it, so a test can
drive it through every interleaving in milliseconds. And a **shell**,
`LiveVenue`, which owns the machine, implements `ExecutionVenue`, and moves
inputs in and requests out without ever waiting. It is the split `orders.rs`
and `user_data.rs` already make, pure logic with the transport elsewhere
(`orders.rs:1-11`, `user_data.rs:5-8`), one level up.

Both go in a crate of their own, **`quant-live`** (the owner's decision 5, §7), depending on
`quant-binance` for the machine's inputs (`Placement`, `CancelReply`, the
translator's `Inbound`), `TradeClient` and `AccountFeed`, and on `quant-engine`
for `ExecutionVenue` and `Unready`, as `quant-sim` does
(`crates/quant-sim/Cargo.toml:18`). The first draft put them in `quant-binance`,
which would then have depended on `quant-engine`. There would have been no
cycle, since `quant-engine` depends on `quant-core` and `quant-book` alone
(`crates/quant-engine/Cargo.toml:17-18`), but the cost was not only a crate of
one module. `quant-verify` and `quant-normalize` both depend on `quant-binance`
(`crates/quant-verify/Cargo.toml:20`, `crates/quant-normalize/Cargo.toml:27`),
so the data tier would have compiled against the trading engine, and
`quant-binance` against `quant-book`, which its manifest keeps to its tests
(`crates/quant-binance/Cargo.toml:101-105`). The project already chose against
that shape once: `EventSource` lives in `quant-core` so that crates providing
events need not depend on the engine (CLAUDE.md, M3.b). Moving
`ExecutionVenue` and `Unready` down a layer instead is harder, because
`observe` takes a `&Book` (`venue.rs:133`). Nothing depends on `quant-live` but
the binary's crate, `quant-backtest`, where `paper` already lives.

#### 1. Tasks and channels

```text
  engine thread (sync)                        tokio runtime (the binary's)
  ────────────────────                        ────────────────────────────
  Engine ─submit/cancel─► LiveVenue ─orders, bounded─► request worker ─REST─► venue
         ◄─────poll────── │  ▲                              │
                          │  └───────inbox, bounded◄────────┘ answers, stamped
                          └◄─ AccountFeed ◄─ tee ◄─ account listener ◄─WS API─ venue
```

| | Decided | Why, and what breaks the other way |
|---|---|---|
| Runtime | The binary's, as paper's is (`paper.rs:257-266`). `LiveVenue` lives on the engine's own thread (`paper.rs:248-254`), owns no runtime, spawns nothing and awaits nothing. `wire(..)` returns the venue and the request worker's future, and the binary spawns the future. | A runtime inside the venue is a second set of threads the binary's shutdown cannot see. A runtime handle in the venue spawning a task per request sends requests in no particular order, and a cancel can then reach the venue before the order it names. |
| Requests | One **request worker**: one REST request in flight, first in first out, through `TradeClient::place_order` and `cancel_order` (`trade.rs:913`, `:925`). | A cancel sent beside its order can arrive first, come back `-2011` *"Unknown order sent."* (`orders.rs:141-153`), and leave the order resting with nobody cancelling it. One at a time also keeps the venue's `ORDERS` limits out of reach. The cost is head-of-line blocking behind a request that times out, at most `ORDER_TIMEOUT`, 20 s (`trade.rs:108`): nothing at one order a minute, and it shows as `QueueFull` rather than as silence. |
| `orders` | `tokio::sync::mpsc`, bounded at **4**. The engine thread is its only producer, with `try_send`, which needs no runtime. | Full is `Unready::QueueFull`, refused before anything is written (`venue.rs:62-64`). It cannot race: `ready` (`strategy.rs:179`) and `submit` (`:243`) run on one thread with no poll between them. Four, because depth buys nothing: an order queued behind a request that times out leaves more than 20 s after the `timestamp` it carries, past the 5 s `recvWindow` (`trade.rs:82`), and the venue refuses it on arrival (rest-api.md:225-241). Unbounded would hide a stalled worker behind a growing queue, every order in it journalled `submitted`. |
| `submit`, `cancel` | Encode, tell the machine, `try_send`, return. A cancel that finds `orders` full is held (below), never refused for it. | The engine thread is the market's only reader. Blocked for one 20 s timeout it books nothing and tallies nothing, and once the tee's channel fills it loses market records as `LocalOverflow`, after which the book waits for the next snapshot, up to an hour. §5's *does not await* is the rule; that is what breaking it costs. |
| A request's `timestamp` | An order's is the engine's `now`, less the offset measured at startup; no clock is read in `submit`. A cancel's is taken by the worker when it sends it, from its injected clock, less the same offset. | An order's stamp is when the strategy decided, so that the venue refuses an order that waited too long (`trade.rs:58-61`, `:901-906`). `now` is the stamp of the input the strategy was acting on, the invariant-4 reading of that instant; a wall-clock read here would be the only one on the engine's path. A cancel has no such reason: wanting an order gone does not go stale. Stamped at the engine's `now`, a cancel queued behind a request that timed out would be refused for its age and leave the order resting, which is the outcome holding cancels exists to prevent. The worker's clock is injected (4) and off the engine's path. `cancel_order`'s doc, which says its stamp is as `place_order`'s (`trade.rs:920-923`), changes in item 7. |
| Held cancels | A cancel that finds `orders` full waits in the shell, first in first out, at most one per order, and goes out at the start of the next `poll` that finds room, before anything else that wake can send. While one is held, `ready` answers `QueueFull`, so no new order takes the slot ahead of it. A second cancel for an order whose cancel is held is `cancel_refused { not_sent }`. After `stop_sending`, held cancels come back `NotSent { why: "stopping" }`. | `orders` is full exactly when a request is stuck behind a timeout for up to 20 s. Refused then, the cancel was a `cancel_refused` notice the strategy is never told (§11 of `docs/engine-contract.md`: nothing of a notice reaches it), the strategy went on believing it had asked, and the resting order stayed on the venue, free to fill hours later. Held, it costs a short wait. At most one per working order keeps the hold bounded by the engine's own ledger. |
| `inbox` | `tokio::sync::mpsc`, bounded at 8; the worker awaits `send` and never drops. | A dropped answer is an order whose outcome nobody holds, and a dropped `Unknown` is an order nobody doubts. The worker can wait only while the engine is not polling, and the engine polls on every wake (`lib.rs:837-838`), an idle one at least every `IDLE_TICK` (`lib.rs:522`). Between two polls the answers are usually no more than the four queued and the one in flight, but not always: a strategy submitting in a loop refills `orders` while the worker drains it within one wake, and item 8's `Control` sends too. When the bound is reached the worker waits for the next poll, so the cost is a delay and never a loss. |
| Account | `AccountFeed`, as #70 built it, read with `poll`, which never blocks (`account.rs:1155-1157`). | Its channel is the tee's secondary with the capture primary, so a consumer that falls behind loses its own copy, as a `Blind { LocalOverflow }`, and never the file's. |
| `poll` | Drain `inbox` with `try_recv` and the feed until `Empty` or `Closed`; merge the two by stamp as a **two-way merge**, each source kept in its own order; step the machine; append what it says. | A sort would reorder one source's own frames whenever the host clock steps back, since `local_recv_ts` is wall-clock time (CLAUDE.md, M2.e). Order within a source is evidence. This is the arrival order the trait asks for (`venue.rs:155`): within a source it is arrival itself, and across the two, both stamped by one clock when they were received (4), ordering by stamp is arrival order unless that clock stepped. Order across the two decides only the order of lines, and the permutation test (5) shows it changes nothing else. |

**Shutdown, and every holder of a sender.** M1.c2's recorder hung at shutdown
because a forgotten clone kept a channel open. Here:

| | Senders | Receiver | Closed by |
|---|---|---|---|
| `orders` | `LiveVenue`, alone | the worker | dropping the engine, which owns the venue |
| `inbox` | the worker; item 8's `Control`, if it keeps one for rechecks (3) | `LiveVenue` | nothing waits for it to close; a worker whose `send` fails stops |
| account feed | the listener's `TeeSink` | `LiveVenue`'s `AccountFeed` | dropping the listener's future (`account.rs:718-722`), last |
| stop | an `Arc<AtomicBool>`, set by `LiveVenue`, read by the worker | | not a channel |

The stop flag exists because a tokio receiver still yields what is buffered after
every sender is gone. Without it, an order queued when the engine was dropped
would still be sent, by a worker with nobody left to hear the answer: an order
on the venue that no journal holds. With it, the worker answers every request it
has not yet sent with `NotSent { why: "stopping" }`, sends nothing, and the
machine records each as `never_landed`. `LiveVenue::stop_sending(&self)` sets it,
`Drop` sets it too so that a careless drop cannot release the queue, and
`LiveVenue::outstanding(&self)` counts requests handed to the worker and not yet
answered. Both take `&self`, because the binary reaches the venue only through
`Engine::venue` (`lib.rs:1090`).

**The worker never panics on a channel.** A failed `inbox.send` means the venue
is gone: the worker stops, sending nothing more, and never `expect`s on it.
Under `panic = "abort"` (`Cargo.toml:86`) an `expect` there would take the
process down before step 6 below sealed the account capture, losing the one
record of what the venue said about the run's last orders.

`Engine::settle` (`lib.rs:708-714`) is how a run ends, in item 8's order:

1. `run()` returns when the market source ends.
2. The owner chose to cancel at a stop (§7, decision 3), so `Engine::cancel_working()`:
   for each working order, `on_cancel_requested` and then the venue's `cancel`,
   at the engine's `now`, with no strategy call. Nothing could issue those
   cancels before item 8. The engine had no cancel of its own, only
   `Context::cancel` reaches the venue, and only from a strategy callback, which
   `settle` never makes. A cancel sent around the engine would skip
   `on_cancel_requested`, and the journal would hold a `cancelled` with no
   request behind it. So it is a seam addition, built in item 8
   (`m8c-live-binary`), beside `settle` in `docs/engine-contract.md` §11, and
   its test asserts one `cancel_requested` line per working order before any
   cancel reaches the venue.
3. `settle()` until nothing is working or a deadline longer than `ORDER_TIMEOUT`
   passes (`lib.rs:700-704`), pausing `IDLE_TICK` when nothing came.
4. `stop_sending()`, then `settle()` until `outstanding()` is zero **or a second
   deadline passes**, `ORDER_TIMEOUT` and a margin, measured by the binary.
   `outstanding()` reaches zero only if the worker is running, and a worker
   never spawned, or one that panicked in a debug build, would hold it above
   zero for ever. Passing the deadline is logged as an error, and every order
   still unanswered is in `stopped.working`, which duty 6 then refuses to trade
   past.
5. The closing checkpoint, `stopped { working }`, `balances`, only while the
   strict writer has not failed: a line written after a torn one would sit in
   the middle of the file, where `read` refuses the whole journal. The writer's
   `Latch` is what the binary writes them through (8a).
6. Drop the engine: `orders` closes, the worker returns, and its `inbox` sender
   goes with it.
7. Stop the listener, which seals the account capture.
8. Only then shut the runtime down. It drives the worker in steps 3 and 4 and
   the listener until step 7; dropped earlier, the worker never answers, and
   the capture is left trailerless, which reads as a crash.

Nothing in that sequence waits for a channel to close, and each wait has a
deadline, which is what keeps it from hanging.

#### 2. The merge machine

**Inputs**, each carrying the stamp of when it was heard (4).

| Input | From | Values |
|---|---|---|
| `Submitted` | the shell's `submit` | id, quantity (the side is not kept, §8) |
| `Placement` | the worker | `Placed`, `Refused`, `Unknown`, `NotSent` (`orders.rs:585-614`) |
| `CancelReply` | the worker | `Acknowledged`, `NotOpen`, `Refused`, `Unknown`, `NotSent` (`orders.rs:641-661`) |
| `Unsent` | the shell | an order that would not encode, or reached `submit` unready (a bug), with or without a `Submitted` before it; a cancel for an order ended or never sent, a second cancel for an order whose cancel is held, or one after `stop_sending` |
| `Report` | the translator | `Ours`: an acceptance, a trade, or an end (`CANCELED`, `EXPIRED`, `TRADE_PREVENTION`); `Foreign`; `StreamTerminated` |
| `Unreadable` | the translator's error | with whose order it concerns: one of ours, a foreign one, a foreign trade, or unknown (§8) |
| `Subscribed`, `ServerShutdown` | `account::classify` on a recorded frame | the subscription's accepting answer; the venue's `serverShutdown` |
| `Blind` | the feed | its `GapCause` |
| `Check` | item 8, if it rechecks | a passing check or a failing one; the offset is the shell's (§8) |

**Per order** the machine keeps the request's side and quantity; what REST said
(`Pending`, `Placed`, `Unknown`, `Refused`, `NotSent`); the venue's order id, from
whichever input names it first; whether the stream has **mentioned** the order
and whether it has been **heard**; the quantity booked and the trade ids booked;
whether it has ended, and whether REST or the stream ended it; its open doubt
causes and its deferred ones, two sets; and any reports held behind a hole.

*Mentioned* means any stream report about the order has arrived, held or
released: the venue has the order. *Heard* means one has been **released**
(rule 2): the stream's account of the order is continuous from its start. A
report held behind a hole mentions an order without hearing it. The two are
kept apart because they answer different questions. Whether REST can still
refuse the order is a question of existence, so it reads *mentioned*; whether
an `Unknown` is already answered is a question of whether we know what happened,
so it reads *heard*. With one word for both, a held report followed by `Unknown`
and `Unknown` followed by a held report would write different lines, and a held
report followed by `Refused` would leave an order the stream proved exists
recorded as rejected, in doubt for ever and with nothing latched.

**The rules.**

1. **One authority per fact** (§5). An acceptance and every fill come from the
   stream alone, a rejection from REST alone, an end from the stream alone.
   `Placed` is a notice, never an acceptance.
2. **Cumulative order.** Every execution report carries `z`, the quantity filled
   so far (user-data-stream.md:100), which the translator hands over as
   `remaining`, so `z` is the request's quantity less `remaining`. A report is
   **released** only if it continues what is booked: a trade when `z` is what is
   booked plus its own `l`, anything else when `z` is what is booked. A report
   whose `z` runs ahead follows fills we never heard: it is **held**, with every
   later report about that order, and the order is in doubt for `fills_missed`.
   One whose `z` runs behind says less traded than we booked, a contradiction.
   This is the book's rule while unanchored (M2.b) for an order: apply nothing
   past a hole, and keep what came after it.
3. **Fills deduplicated by trade id**, `t` (user-data-stream.md:105), which the
   translator reads in place of the execution id `I` (`user_data.rs:622-630`). A
   trade id already booked is `anomaly { duplicate_trade }` and books nothing,
   **in every phase and before every other rule but 6** (*Precedence*, below), so
   a resent copy of an order's last trade is a duplicate and not a fill after the
   order's end. Nothing produces one today, since the listener never overlaps two
   subscriptions (item 6, *not done*); if a later listener does, this is what makes
   its resent trades safe, and the anomaly is what moves to counted. A resent
   `NEW` or end is not covered: such a listener must first say how it tells a
   resent frame from a contradiction, because rule 4 and *Precedence* treat
   both as the venue being wrong.
4. **One terminal per order.** A second end is `anomaly { second_end }`. A fill
   after an end is `anomaly { after_end }`, and the fill itself is handed over as
   an order report, which the engine, no longer working the order, writes down
   as `orphaned` with its ids and books nowhere (`lib.rs:1053-1082`); every
   orphaned fill latches `Diverged`. That is the only report handed over after an
   end. The trait asks for none before an order's last fill (`venue.rs:159-161`)
   so that the engine never books a fill for an order it has forgotten, and here
   it does not; a contract written for a venue that is right must not make the
   record drop money when the venue is wrong. The trait's docs say so in item 7's
   pull request.
5. **An acceptance comes first.** If the first released report about an order is
   not `NEW`, which happens only when `NEW` fell in a hole, an `Accepted` naming
   the report's venue order id is emitted before it, at its stamp. A trade proves
   the venue had the order. So every lifecycle `LiveVenue` tells begins with an
   acceptance or is a rejection alone, which is the simulator's shape too since
   `m8c-sim-declines-unannounced` (§6).
6. **The venue's id for an order is checked, not trusted.** The first `orderId`
   any input names is the order's: `Placed`'s (`orders.rs:592-598`), a stream
   report's `i` (user-data-stream.md:98), held reports included, or a cancel's
   `Acknowledged { order_id }` (`orders.rs:644`). Every later one, from any of the
   three and in every phase, is compared with it. One that differs is
   `anomaly { contradiction }` and `Diverged`, and the input that carried it
   changes nothing else: two orders answering to one name of ours, which
   `RunTag` exists to prevent (`user_data.rs:115-121`). Comparing only the
   stream against itself would miss a `Placed` and a `NEW` that disagree, in
   either order.
7. **A cancel names the order in `C`, not `c`.** On a `CANCELED` report `c` is the
   cancel request's id, which the venue generates because we send none
   (`orders.rs:331-344`), and which replaces the order's own (web-socket-api.md:4181).
   The translator reads `C` there (`user_data.rs:445-454`), so the machine, keyed
   by our `ClientOrderId`, never sees the replacement. It relies on two
   consequences. Nothing about an order can follow its `CANCELED`, so rule 4
   holds. And `Placed`'s order id is the one name that still finds a cancelled
   order, which is why `placed` is journalled (`report.rs:396-401`) and what slice
   (d)'s lookup will use.

**Precedence.** Several rules can match one stream report: a resent copy of an
older trade has a booked trade id *and* a `z` behind what is booked. So a report
about one of our orders is judged in this order, and the first that applies
decides it alone:

1. its `orderId` differs from the order's (rule 6): `contradiction`, `Diverged`;
2. a trade whose trade id is booked (rule 3): `duplicate_trade`;
3. the order has ended. Ended by REST, any report is `contradiction` and
   `Diverged`, a trade among them also handed over as an orphan (*REST against
   the stream*). Ended by the stream, a trade is `after_end` and an orphan, an end
   is `second_end`, and a `NEW` is `after_end`;
4. a `NEW` about an order already heard: `contradiction` and `Diverged`, whatever
   its `z`, since a `NEW` always carries `z` of zero and the order may have
   filled since;
5. `z` behind what is booked: `contradiction` and `Diverged`;
6. `z` ahead of what is booked, or reports already held for the order: held, and
   `fills_missed` (rule 2);
7. otherwise it is released.

**Every `contradiction` latches `Diverged`.** The first draft let a second `NEW`
through as a contradiction without the latch, so the kind on a line did not say
whether the process had stopped trading. A contradiction is the venue saying two
things about one order of ours, and nothing this system runs can make the venue
repeat itself (rule 3); it needs a person whichever it is. One rule also gives
item 8 a predicate over the journal (§6, duty 8) that does not have to know
which contradiction it was.

**Doubt**, which is where interleavings could disagree, so its rules are written
to make them agree.

- A doubt is a set of causes per order, each raised once while open, one `doubt`
  line each. `Unknown` raises the cause its `WhyUnknown` mirrors
  (`report.rs:471-503`, `orders.rs:839-887`) with what the venue said. Losing the
  stream raises `account_blind` on every working order. An unreadable report
  raises `fills_missed` on the order it concerns, or on every working order when
  it cannot be attributed, since it could have been any of theirs. A hole (rule
  2) raises `fills_missed`.
- **Only the stream clears it.** The first released report about the order
  clears every open cause, one `cleared { reported }` line per cause, before the
  report itself: one per cause, so every raised doubt has its clearing in
  whatever order they came. A held report clears nothing, because nothing about
  the order is known past its hole. REST never clears one: what REST did not
  know, the stream may already have answered, and not the reverse.
  `cleared { looked_up }` is slice (d)'s.
- **`Unknown` about an order already heard is raised and cleared at once.** The
  stream has said the order exists and what happened to it, and whatever a later
  blind stretch may hide has its own `account_blind`. This is what makes "REST
  answered first" and "the stream answered first" write the same lines. About an
  order only mentioned, `Unknown` is raised and stays open, as it would had it
  come first: the held report's own `fills_missed` is open beside it either way.
- **`IdInUse` latches `Diverged` in every phase**, not only when REST answers
  first. It says some order is open under a name of ours that should mean one
  order (`orders.rs:855-870`), which is as true after the stream has spoken as
  before; latching it in one phase only made `ready` end differently by which
  source answered first.
- **A cause about an order REST has not answered for and the stream has never
  mentioned is deferred**, not raised. Losing the stream, or an unreadable report
  that cannot be attributed, marks it. The first of three things resolves it.
  `Placed` or `Unknown` raises it, because the order may have been live in the
  dark. `Refused` or `NotSent` drops it, because an order that never existed
  missed nothing. A stream report about the order raises it, because the venue
  has the order; and when that report is released, its release then clears it
  with every other open cause, so the deferred cause is raised and cleared in one
  step. Without the third, an order the stream reported before REST answered
  kept a deferred cause nothing would ever raise or drop, and stayed `InDoubt`
  for the rest of the process, while the same order with REST first wrote a
  raise and a clearing and ended ready.
- An order in doubt is never rejected on that account (§5, *an unknown outcome is
  not a rejection*).

**REST against the stream.** A refusal is certain only because the classifier
calls nothing refused that may have been placed (`orders.rs:80-89`). If the
stream mentions an order REST refused or never sent, before that answer or after
it, held or released, the classifier was wrong about the venue: an order exists
that our record says does not, and it may trade with nobody watching. That is
`anomaly { contradiction }` and `Diverged`, never a second terminal. When REST
answered first, the order has already ended and a fill on it is an orphan (rule
4). When the stream came first, no `Rejected` is emitted and the order stays
working, so its fills are booked; both orders latch, and contradictions are
tested on their own (5).

**Cancels.** `Acknowledged` emits nothing unless it names another order (rule 6),
or the order is one REST refused or never sent: the venue cancelling an order
says it had it, as a stream report would, so that is a contradiction in either
order (§8). The stream says how the order ended.
`NotOpen`, `Refused`, `Unknown` and `NotSent` each emit `cancel_refused` with the
venue's words. None changes the order, and an `Unknown` cancel puts nothing in
doubt, because the stream decides whether the order ended and is still
listening. A cancel the shell did not send is `cancel_refused { not_sent }`.

**The account.** Every recorded account frame is routed by `account::classify`
(`account.rs:388`) before anything reaches the translator, which reads only
execution reports; a frame sent to it otherwise would come back unreadable and
unattributable, and put every working order in doubt for nothing. What is
recorded is set by `keep` (`account.rs:456-470`): every event, every answer but
the logon's and the probe's, and every frame `classify` cannot read.

| Recorded frame | Becomes |
|---|---|
| an `executionReport` event | the translator, then `Report` or `Unreadable` |
| `serverShutdown`, `eventStreamTerminated` | `ServerShutdown`, `StreamTerminated` |
| any other event (`outboundAccountPosition`, `balanceUpdate`, ...) | counted, not journalled; and, after an overflow while down, the sign the account is heard (below) |
| the subscription's answer, accepting | `Subscribed` |
| the subscription's answer refusing, or an answer to no request we sent | counted and logged. No transition: the listener ends or stops the connection on a failure (`account.rs:1000-1006`), and the gap it records next is the loss |
| a frame `classify` calls `Unrecognised` | `Unreadable` with no order: `anomaly { unreadable }`, and `fills_missed` on every working order, deferred where REST has not answered. Not JSON, or an event with no `e`, could have been a fill (invariant 5) |

The stream is **down** from construction until it is heard.

- Down to heard: `account { regained }`, the record's mark that the account is
  heard from here. The evidence is the accepting subscription answer, or, after
  a `Blind { LocalOverflow }` while down, the first account event of any kind,
  because that hole may have swallowed the answer: with the answer as the only
  evidence, a lost answer left `StreamDown` on a healthy stream until the next
  reconnect, possibly a day later. An event is enough because the venue sends
  account events only on an active subscription, and the probe's answers, the
  other evidence, are never recorded (`account.rs:298-308`).
- Heard to down: `account { lost }`, and `account_blind` on every working order,
  deferred where REST has not answered (above). The cause is the first evidence:
  `serverShutdown` gives `server_shutdown`, `eventStreamTerminated`
  `stream_terminated`, and otherwise the feed's `Blind`, a `Disconnect` being
  `socket_closed`.
- **`socket_closed` means the connection ended, for a reason the record does not
  carry**, and item 7 writes that on the variant, which has no doc today
  (`report.rs:602`). The listener knows why it ended a connection (`Ended`,
  `account.rs:670-693`) and logs it, but the capture holds only
  `Gap { Disconnect }`, and the machine reads what is recorded, so that item 8's
  replay reproduces it. `idle_timeout` would be as much a guess in the other
  direction for a socket the venue did close, so it stays for a source that can
  tell, and nothing produces it yet.
- `Blind { LocalOverflow }` while heard, a hole the tee left in our copy, is
  `lost { local_overflow }`, the doubts, and `regained` at once: in the ordinary
  case the subscription never ended, only our copy of it.
- A `Subscribed` while heard is a loss we did not see: an accepting answer comes
  only on a new connection, so the hole of an overflow hid a disconnect, and
  orders sent since may have been reported in the dark. It writes
  `lost { local_overflow }`, `account_blind` on every working order, and
  `regained`, at its stamp.
- A `Blind` while down, the session's opening `RecorderRestart` among them, is no
  transition and writes nothing.
- The feed `Closed` means the listener stopped for good (`account.rs:174-186`):
  down for the rest of the process.

**Other people's orders.** The translator's `Orphan` and `NotOurs` become one
`Inbound::Foreign`, read leniently so that a stranger's trade paid in BNB is
recorded rather than refused (`report.rs:567-579`), and every one is a `foreign`
line. A foreign **trade** on our symbol also latches `Diverged`, the owner's rule
(`venue.rs:65-75`), whoever made it. A foreign acceptance or end does not; it
moves no balance. A report about an id of this run that this machine never sent,
which only an earlier process of the same run can leave, goes to the engine as
an order report and is withheld there as a stray or an orphan
(`lib.rs:1062-1082`); a fill there latches `Diverged` too. Other symbols, and
account events the machine does not read (`outboundAccountPosition`,
`balanceUpdate`), are counted and not journalled.

**The state table.** Phases: **Sent** (not heard, not ended), **Live** (heard,
not ended), **Ended**, by REST or by the stream. Doubts, deferred causes and held
reports lie across them. Stream reports are judged by *Precedence* first; the
rows below are what is left once its steps 1 to 5 have not applied.

| Phase | Input | Emits | Next |
|---|---|---|---|
| Sent | `Placed` | `placed`; deferred causes raised | Sent |
| Sent, not mentioned | `Refused(r)` | `Rejected { reason, declined { cited, breach } }`; deferred causes dropped | Ended by REST |
| Sent, not mentioned | `NotSent`, or an `Unsent` order | `Rejected { Venue, never_landed { not_sent } }`; deferred causes dropped | Ended by REST |
| Sent, mentioned | `Refused`, `NotSent` | `anomaly { contradiction }`; `Diverged` | Sent |
| Sent | `Unknown(w)` | `doubt raised { cause, said }`; deferred causes raised | Sent |
| Sent | released `NEW` | deferred causes raised; every open cause cleared; `Accepted` | Live |
| Sent | released trade or end | deferred causes raised; every open cause cleared; `Accepted` (rule 5); the fill or the end | Live, or Ended by the stream |
| Sent, Live | a report held (*Precedence* 6) | deferred causes raised; `doubt raised { fills_missed }`, once | unchanged |
| Sent, Live | the stream lost | `account_blind` raised where REST answered or the stream mentioned, deferred elsewhere | unchanged |
| Live | `Placed` | `placed` | Live |
| Live | `Unknown(w)` | raised and cleared at once | Live |
| Live | `Refused`, `NotSent` | `anomaly { contradiction }`; `Diverged` | Live |
| Live | released trade, `remaining` above zero | every open cause cleared; `Filled` with its ids | Live |
| Live | released trade, `remaining` zero | every open cause cleared; `Filled` with its ids | Ended by the stream |
| Live | released end | every open cause cleared; `Cancelled` with how it ended | Ended by the stream |
| Ended | `Placed` | `placed` | Ended |
| Ended by the stream | `Unknown(w)` | raised and cleared at once | Ended |
| Ended by the stream | `Refused`, `NotSent` | `anomaly { contradiction }`; `Diverged` | Ended |
| any | `Unknown(IdInUse)` | the row for its phase, and `Diverged` | as that row |
| any | `Placed`, or `Acknowledged`, naming another `orderId` | `anomaly { contradiction }`; `Diverged`; nothing else | unchanged |
| any | a `CancelReply` but `Acknowledged`, or an `Unsent` cancel | `cancel_refused` | unchanged |

REST answers once per order, so an order ended by REST never meets a second REST
answer, and only *Precedence* step 3 reaches it.

`reason` is `RefusalCause::reject_reason` (`orders.rs:727-736`); item 7 adds
`RefusalCause::breach`, `Refusal::cited` and `WhyUnknown`'s cause and words, so
that the mapping onto `quant-core`'s types is written once, in `orders.rs`.

#### 3. Readiness

`ready` reads the request's instrument, the machine and the queue's capacity and
nothing else, and changes nothing (`&self`, `venue.rs:99-100`).

| `Unready` | When `LiveVenue` answers it |
|---|---|
| `Diverged` | Latched by every `contradiction` (two venue ids for one of ours, REST and the stream or an acknowledged cancel disagreeing about whether an order exists, a second `NEW`, a `z` behind what is booked, a second answer to one order's request), every orphaned fill (a fill after an order's end, a fill about an id of this run this machine never sent), a foreign trade on our symbol, read or not, and `IdInUse` in any phase. Never cleared in the process (`venue.rs:65-75`), and not cleared by a restart either: item 8 reads it back off the journal (§6, duty 8). |
| `Unchecked` | False at construction, which takes a `Checked`: minted only by the checks `venue-check` runs (`trade.rs:986`, `:1070`, `:1104`), and carrying the measured offset every order and logon is stamped with. True after a failing `Check`, false after a passing one. **Not** set by an order refused `-1021` or for its key: that is a rejection with the venue's code on its line, every later order would be refused the same way and harmlessly, and a latch on `-1021` would also fire on an order that only waited too long in the queue. |
| `StreamDown` | From construction until the account is heard, and from each loss until it is heard again (§2, *The account*). Never across a `LocalOverflow` while heard. |
| `InDoubt` | While any order has an open doubt cause, a deferred one, or a held report. |
| `QueueFull` | `orders` has no room, a cancel is held (§1), or `orders` is closed. Closed cannot happen in a release build, where `panic = "abort"` (`Cargo.toml:86`) takes the process with a worker that panics; a closed queue takes nothing, which is what the variant says. |
| `NoFilters` | For any instrument but the one the venue was wired for, which is that variant's own meaning (`venue.rs:47-49`): this venue holds venue-sourced filters for one instrument and none for another. `wire` takes the `InstrumentId` beside the `VenueInstrument` (`exchange_info.rs:229-239`), because an `InstrumentDef` carries no id to check `request.instrument` against (`orders.rs:184-186`) and `NewOrder::encode` writes `symbol` from whatever definition it is handed (`orders.rs:206`). Without the check, an order for instrument B passes `ready` and risk and goes out as the wired symbol, with real money. For the wired instrument, never: `wire` cannot be called without its filters. A `submit` for another instrument, which only a `ready` that lied could let through, is `never_landed { not_sent }` and is sent nowhere. |
| `Untagged`, `NoBaseline` | **Never.** They are preconditions of building one. `wire` takes a `RunTag`, and the definition both the encoder and the translator need (`orders.rs:198-203`, `user_data.rs:384`). A run whose `exchangeInfo` fetch failed, whose journal is non-empty and untagged, or (question 1 below) whose start balances were not written down, builds no `LiveVenue`: item 8 records and does not trade. They stay in the set because the set is serialized (`venue.rs:38-40`). |

When more than one holds, a refusal names one, in this order: `NoFilters`,
`Diverged`, `Unchecked`, `StreamDown`, `InDoubt`, `QueueFull`. What this venue can
never send, then what needs a person, then what needs a check, then what clears
by itself, so that a `refused` line points its reader at the cause that will
last longest.

#### 4. Time

Every report the machine emits carries the stamp of the input that caused it,
and the machine has no clock to give it another.

| Report from | Stamped |
|---|---|
| a stream frame | its `local_recv_ts`, taken by ingress before the frame is parsed (`account.rs:149-156`) and copied by the translator (`user_data.rs:401-403`) |
| a `Blind` | the `Blind`'s stamp: when the gap was recorded or the hole revealed (`account.rs:1102-1115`) |
| a REST answer | `Clock::now()`, read by the worker when the request's future resolves, from an injected `Arc<dyn Clock>` |
| the shell (`Unsent`) | the engine's `now`, handed to `submit` or `cancel` |
| an immediate clearing, an implied acceptance | the stamp of the input that caused it |

The worker's clock is the clock that stamps `local_recv_ts`, as the poll contract
requires (`venue.rs:155-158`), and injected, so a test drives it with
`ManualClock`. `capture::run` makes its own `SystemClock` today (`capture.rs:264`);
item 8 hands one clock to the market ingress, the account ingress and the worker.

**A report cannot move the engine's clock backwards.** The engine moves only to a
stamp later than its clock (`lib.rs:855`, `:884`), through `advance_to`, which
never moves back (`lib.rs:738-744`). A REST answer that sat in `inbox` while
market events carried the clock past its stamp is written down at its own stamp
and moves nothing. What the merge can do is put a line after one stamped later,
when a stream frame overtakes a stamped answer into the next poll: a window of
the worker's stamp-to-send gap, which the readers tolerate because they filter by
stamp: `decisions_at` skips an entry stamped after the instant and reads on
(`runlog.rs:825-855`; `docs/run-log.md` §9, *`explain --at` reads backtest
journals again*). The machine compares no stamps. It decides on the order the
shell feeds it, which is each source's own order, merged across the two by stamp
in `poll` (1); so a host clock that steps can change which source's input it
sees first, which the permutation test covers, and never the order within one.

#### 5. Tests, and the acceptance

**The permutation test**, item 7's acceptance. A scenario is one order's truth:
what REST answered, a cancel's answer if there is a cancel, and the stream's
records for it in socket order, `Blind` and `Subscribed` included. Every
interleaving of the REST answers among those records, stamped by position so the
engine's clock moves sanely, is driven through the machine, wrapped as an
`ExecutionVenue`, into a real `Engine` with the real `JournalWriter`. For every
interleaving:

- the strategy is told the same events, compared without their stamps;
- the journal holds the same lines as a multiset, compared without `at`;
- the portfolio, `working()` and `ready` end the same, and `ready` ends
  `Diverged` exactly when `journal::diverged` holds over the journal (§6, duty 8);
- `runlog::check` (`crates/quant-explain/src/runlog.rs:301`) reports the same
  findings. The multiset comparison cannot see order, and the check's doubt fold
  can: a `cleared` written before its `raised` leaves the doubt open
  (`runlog.rs:432-446`), so it shows as `in doubt` in one interleaving and not
  in another;
- no order has two terminals, and no trade id is booked twice.

Scenarios: a market buy filled in one trade, and in three (a walk); filled in
part, then `EXPIRED`; a resting limit cancelled, answered `Acknowledged` and
answered `NotOpen`; a cancel that lost the race; each `WhyUnknown`, with the
stream reporting and with it silent; `Refused` and `NotSent` with the stream
silent; a report held behind a hole, against each REST answer but a refusal; and
each with a `Blind` and `Subscribed` placed before, among and after the order's
records, which is where a deferred cause meets a stream report before its REST
answer. A contradiction is not a consistent truth and is tested on its own, and
so is `IdInUse`, as a latch in each of the three phases. It lives in
`quant-explain`'s tests, the one crate that may link the machine, the engine,
the writer and `runlog::check` together: it sits at the top of the graph and
nothing depends on it (`crates/quant-explain/Cargo.toml:10-15`), so taking
`quant-live` as a dev-dependency costs nothing.

Breaks that must redden it: `Placed` emitted as an acceptance; deduplicating by
`I`; `Unknown` raised without the immediate clearing when the stream came first;
one `cleared` line for several causes; `account_blind` raised on an order REST
has not answered for; a REST refusal emitted after the stream reported; a
released report that leaves a deferred cause unresolved; a held report counted
as heard.

**`account_blind`**, item 7's second acceptance: two working orders and the stream
lost give `account { lost }` and one `doubt raised { account_blind }` each;
`ready` answers `StreamDown`, then after the resubscribe `InDoubt`, then nothing
once each order's next report clears it. The journal is then read by
`runlog::check`, which must flag `in doubt` when a doubt is never cleared.

That check could not fail as first written. It asked that `runlog check` not
flag the `account` line, and the check ignores `account` lines entirely
(`runlog.rs:549-554`), so it held for any `LiveVenue`; worse, a `LiveVenue` that
forgot to raise `account_blind` made the check *cleaner*, since no doubt means
nothing left in doubt. So item 7 gives `runlog check` the coverage the
`account` arm's comment already claims, as a seventh finding beside
`docs/run-log.md` §9's six:

| label | finding | from |
|---|---|---|
| `unwatched` | an order working when the account stream was lost that was never in doubt for it: what happened to it in the dark is recorded as known | each `account { lost }` line, against every order with a `submitted` line before it and no end before it |

Such an order passes when it is in doubt for `account_blind` at the `lost` line
or is raised for it afterwards, or when it ends in `rejected` or `never_landed`
with no `placed` line and no line the stream produced (`accepted`, `filled`,
`cancelled`): an order REST refused never existed, so its deferred cause was
dropped (§2). Both directions are seen red before either is trusted: a
`LiveVenue` that skips the raise makes the finding appear in the acceptance, and
a check that skips `lost` lines misses it on a hand-built journal with no raise.
The coverage half and the permutation test share `quant-explain`'s tests.

**The machine's own tests**, each seen red under its break first:

| Asserts | Break |
|---|---|
| a duplicate trade id books once, beside an anomaly | deduplicate by `I`, or not at all |
| a resent copy of an order's last trade is `duplicate_trade`, not `after_end`, and latches nothing | judge the end before the trade id |
| a second `NEW` before any fill, whose `z` of zero continues what is booked, is a `contradiction` and `Diverged` | judge it by `z` alone, which releases it |
| `Placed`, then `Acknowledged` naming another order id: `Diverged` | compare the stream's ids only |
| a hole holds every later report and raises `fills_missed` once | release past the hole |
| a fill after an end is an orphan, an anomaly and `Diverged`; a second end only an anomaly | emit the second end |
| `Refused` after `NEW`, and `NEW` after `Refused`: `Diverged`, no second terminal | emit the refusal |
| `Placed` and `NEW` naming different order ids: `Diverged` | keep the later id |
| a trade as an order's first report brings an `Accepted` before it | omit it |
| each `WhyUnknown` maps to its cause, with what the venue said | map two to one |
| a foreign trade paid in BNB is a `foreign` line and `Diverged`; a foreign acceptance only a line | refuse the BNB fee |
| an overflow while down, then an account event: `regained` and ready; no overflow, then an event: still down | take only the subscription answer as heard, or take any event |
| a `Subscribed` while heard: `lost`, `account_blind` on each working order, `regained` | ignore it as a repeat |
| a refused subscription answer and an `Unrecognised` frame never reach the translator | route by `e` alone |
| a rejection keeps the venue's words, the filter it named and why nothing left, written out by hand | hand the engine `why: None`, or a `Cited` of zeros |
| an order's request is answered once, whether REST or the shell's `Unsent` answers it | send `Unsent` past the guard |
| an `Unsent` with no `Submitted` is `never_landed` and latches nothing; a `Submitted` after it is a contradiction | require a `Submitted` first |
| `Acknowledged` and a refusal for one order, in either order: `Diverged` | record the venue id alone |
| a stranger's unreadable trade is `unreadable_foreign_trade` and `Diverged`; their unreadable acceptance only an anomaly | treat every stranger's unreadable report alike |
| the readiness order, one test per pair | swap two |
| every output's stamp is its input's | stamp from a clock |

**The shell's tests**, against a scripted local HTTP server as `trade.rs`'s tests
use one (`trade.rs:1467`), and a `ManualClock`: a cancel goes after the answer to
the order it names; an answer is stamped when it arrived; the `timestamp` sent is
the engine's `now` less the offset, read back off the request the server
received, which is the sign `venue-check` once had wrong
(`m8b-venue-check-offset`); an order for an instrument other than the one wired
is refused `NoFilters` by `ready`, and if `submit` is called for it regardless,
nothing reaches the server; with the worker stalled, `submit` returns, `ready`
answers `QueueFull`, and a cancel is held and reaches the server once room
appears, before any order submitted after it; a cancel reaches the server
stamped by the worker's clock at sending, not by the engine's `now`; after
`stop_sending` nothing reaches the server, every queued order comes back
`never_landed`, every held cancel `cancel_refused`, and `outstanding` reaches
zero; and with the worker never spawned, the shutdown deadline (1) still ends
the wait.

**And one end to end**, through the real listener: `fake_venue` (#70) as the
account side and a scripted REST server for orders; a market buy acknowledged,
its `NEW` and trade pushed with `FakeVenue::event` (`fake_venue.rs:400`); then
`drop_connection` (`:417`) and the `account_blind` path through the real feed.

**`fake_venue` is not extended into an exchange.** It stays a stand-in for the
account side's transport, and each test says what the venue does on both
channels. A fake that matched orders and wrote its own reports would be a second
model of the venue, to agree with the simulator and with Binance forever with
nothing to notice when it stopped: the argument that there is no `PaperVenue`
(`docs/engine-contract.md` §9).

#### 6. Not in item 7, and what item 8 must enforce

Not in item 7:

- **Lookup and adoption**, slice (d): `GET /api/v3/order` by `origClientOrderId`
  or by `Placed`'s order id, `cleared { looked_up }`, `never_landed { not_found }`,
  `DoubtCause::Restart`, releasing held reports once the fills they wait for are
  found, and taking over a previous process's working orders. The release must
  deduplicate by trade id among the held reports too: *Precedence* 2 reads the
  trade ids booked, so a resent copy of a held trade is held a second time,
  and released as it stands would book one trade twice. Until then every
  doubt and every held report is loud and stuck, as §5 decided.
- **The binary**, item 8: the gates, the settle loop, the strict writer,
  `balances`, the live mode in `ops/`.
- Overlapping two subscriptions across `serverShutdown`; waiting out a 429's
  `retryAfter`; refetching `exchangeInfo` or rechecking the clock during a run
  (the `Check` input is there for item 8 to use); anything read from
  `outboundAccountPosition`.

**Done before item 8: the simulator announces only what it will not refuse.**
`SimulatedVenue` announced every order `Accepted` before it judged its filters or
its market (`quant-sim/src/lib.rs:647-690` at `f676e6d`), so a filter breach or
`NoMarket` reached a strategy as an acceptance and then a rejection, where live a
refused request is a rejection alone (rule 1). The first draft filed this for slice (e)
on the ground that the seam's guarantee covers fields and types and not the
shape of a lifecycle. That was wrong twice over. `docs/engine-contract.md` §11
ticks "a strategy's view of a lifecycle is identical whichever venue produced
it", naming the declined order; and CLAUDE.md calls a strategy that can tell
which pair it is wired to a bug, whatever a checkbox says. So the simulator
now judges filters and market first and announces only an order it will fill or
rest (`m8c-sim-declines-unannounced`). As predicted, M5's money check is
unchanged, 824 fills and cash 31.64743842, because that run refused nothing;
and ETHUSDT over the fortnight, where every order is below the minimum
notional, writes its 413 rejections without their 413 `accepted` lines and is
otherwise byte-identical. `MaCrossover` reads only an order's terminal event
(`ma.rs:472-475`), so no decision moves. No test had pinned the lifecycle's
shape either way; one does now, in both directions.

Item 8 must enforce:

1. **The gates before the engine starts**: the `venue-check` checks, giving a
   `Checked`; `exchangeInfo`, giving a `VenueInstrument`; the run tag from the
   journal, refusing a non-empty journal without one; and the start `balances`
   line. If one fails, record and do not trade. The gates go before the capture
   starts, so the engine's market channel never fills unread: an engine started
   late begins on a book that waits up to an hour for a snapshot.
2. **One `Arc<dyn Clock>`** for the market ingress, the account ingress and the
   worker, and the `Checked` offset to the listener as to the worker (item 6).
3. **The account tee built with `marking_into`**, which `AccountFeed::new`
   requires (`account.rs:1174-1186`).
4. **`JournalWriter::strict`.**
5. **The shutdown order of (1)**, and the engine never dropped while
   `outstanding()` is above zero before step 4's deadline.
6. **Until slice (d), no trading** when the last session has no `stopped` line or
   its `stopped.working` is not empty.
7. **Its acceptance's replay, read precisely.** REST answers are not in the raw
   tier (`report.rs:326-329`), so replaying the recorded account frames can
   reproduce only the entries the stream produced. The REST answers come back off
   their own journal lines, and the machine being pure is what makes the two
   together replayable at all.
8. **A latch survives a restart.** `Diverged` lives in the process, and duty 6
   catches a restart only while orders were working: a stranger's trade with
   nothing working, then a clean stop and a restart on the same journal, would
   trade again against a position nobody knows. That is M6's *a kill switch that
   forgets is not a kill switch* (`RiskEngine::recover`, `risk.rs:305`), so the
   journal answers it. `journal::diverged(entries)`, in `quant-engine` beside
   `recover_risk` (`journal.rs:1493`), is true when the journal holds a line that
   latches: an `anomaly { contradiction }`, an `orphaned` fill, a `foreign` line
   with a trade or an `anomaly { unreadable_foreign_trade }`, or a
   `doubt raised { id_in_use }`. When it is true, item 8 records
   and does not trade, as for any failed gate, until a person starts a new journal
   under a new run tag (`venue.rs:65-75`). No new line is written for the latch:
   the lines that caused it are already there, and a `diverged` line beside them
   would be a second record of one fact that must agree with the first for ever.
   Instead the two are made to agree under test: in the machine's and the
   permutation tests, `ready` ends `Diverged` exactly when `journal::diverged`
   holds over that run's journal. The machine latches from what it heard, the
   predicate from what was written, and neither is derived from the other.

**As commits**, each green: the helpers in `orders.rs` and `user_data.rs`
(`Inbound::Foreign`, attributed errors, `Checked`, `cancel_order`'s stamp);
`journal::diverged`, with its own tests; `quant-live` with the machine and its
tests; the shell and the worker with theirs; `runlog check`'s `unwatched`
finding, with its fixtures; the permutation and `account_blind` tests in
`quant-explain`; the end-to-end test; the docs, then a grep for what they
replace.

#### 7. Decided by the owner, 2026-10-20

Six questions were the owner's. Each was asked with a recommendation, and each
recommendation was taken. They are decisions now, not options, and changing one
needs a new argument.

1. **No baseline, no trading (`NoBaseline`).** A live run journals a start
   balances snapshot before it may trade, and records without trading if it
   cannot. Criterion A's baseline cannot be taken afterwards, and refusing costs
   one REST call at startup.
2. **A failed gate at startup means record only.** No strategy runs against a
   venue that refuses everything: no order could go, and a run of refusals is
   noise criterion B would have to exclude. `NoFilters` is still returned for
   any instrument other than the one wired (§3).
3. **A stop cancels resting orders, then settles.** `Engine::cancel_working`
   (§1, step 2) journals each cancel as asked for, then `settle` runs until
   their ends arrive. Cancelling is not flattening (§5), and an order left
   resting can fill after the record has stopped.
4. **A stranger's trade on another symbol does not latch `Diverged`.** The rule
   names our symbol, the one a position is being traded on, and §2's one
   account is the defence. Revisit if the account is ever shared.
5. **The machine and the shell live in a new crate, `quant-live`.** In
   `quant-binance` they would make the data tier (`quant-verify`,
   `quant-normalize`) compile against the trading engine, against the
   precedent that put `EventSource` in `quant-core`.
6. **An order `Placed` that a healthy stream never mentions goes into doubt.**
   The worker sends an `Unheard` input `ORDER_TIMEOUT` (20 s) after each
   `Placed`, and the machine raises `fills_missed` on that order if the stream
   has not mentioned it, cleared by the stream like any doubt. Twenty seconds
   is above the worst transport delay seen so far (p99 14 s at M7). It is a
   timeout, so where it lands among the stream's records depends on timing:
   tested on its own, and kept out of the permutation test's equality.

#### 8. What building the machine and the shell corrected

The machine is `quant-live::Merge`, built from §2 and §3 and tested by §5's
tables; the shell is `quant-live::LiveVenue`, with `wire`, the request worker
and `drain`, built from §1, §3 and §4. Where the code showed the design wrong
or silent, it followed the design's stated intent, and the difference is
written here rather than left for a reader to find. The machine's come first,
then the shell's.

- **`Submitted` carries no side.** No rule reads it; the quantity is kept
  because a report's `z` is the request's quantity less its `remaining`.
- **The machine routes frames itself**, `Merge::route`, so the routing the
  design puts before the translator is pure and tested with the machine: an
  answer and an unrecognised frame never reach the translator, and
  `serverShutdown` is read before it.
- **An unreadable report says whose it was in three ways, not two.** The
  translator's `read` names one of ours, a foreign order, or nobody
  (`user_data::Concerns`). A stranger's report read far enough to know it is
  not ours puts none of ours in doubt; with two ways it could only have been
  "unattributed", and doubted every working order for a report known not to
  be about any of them. It is still an `anomaly { unreadable }`, and latches
  nothing, unless it was a trade: a stranger's trade on our symbol moved the
  balances whether or not it could be read, so the translator names it
  (`Concerns::ForeignTrade`) and the machine writes
  `anomaly { unreadable_foreign_trade }` and latches, as for a readable one.
  A kind of its own, because the latch survives a restart only through
  `journal::diverged`, which reads the kind on a line.
- **A report attributed to one of our orders is a mention**, readable or not:
  the venue has the order. So an unreadable one raises the order's deferred
  causes with its `fills_missed`, a REST refusal after it is a contradiction,
  one about an order REST ended is a contradiction, and one about an order
  the stream ended is the anomaly alone, since nothing is working to doubt.
- **A stranger's cancel-replace or amend is counted, not refused.** The
  translator refuses `REJECTED` and `REPLACED` for our orders, which never
  cause them; refused for a stranger's, an ordinary report of theirs would
  have come back unreadable. Read leniently, as the BNB fee is.
- **A trade handed over as an orphan is recorded as accounted for**, after an
  end or on an order REST ended, so that a resent copy of it is
  `duplicate_trade` rather than a second orphan of one trade.
- **What our own plumbing cannot produce is a contradiction:** a REST answer
  about an order the machine never sent, a second answer to one order's
  request, whether REST's or the shell's `Unsent`, a second submission under
  one id, a rejection from the stream (rule 1). Each latches, so
  `journal::diverged` agrees with the machine on them too. The first build
  sent `Unsent` past the guard REST's answers go through, so a second one was
  a second terminal and one after `Placed` rejected a placed order.
- **`Unsent` needs no `Submitted`.** A shell may refuse an order before it says
  one is about to go, and treating that as a REST answer about an order never
  sent latched `Diverged` for a harmless local failure and told the engine
  nothing, leaving its order working. So an `Unsent` makes the order and ends
  it; a `Submitted` under that id afterwards is a second submission.
- **An acknowledged cancel is the venue saying it had the order.** The table
  was silent on one for an order REST refused or never sent. It is the same
  fact as the stream mentioning that order, so it is the same contradiction,
  in either order: after the refusal it latches; before it, the refusal that
  follows latches and the order stays working.
- **The account, where the table is silent.** A `Blind` while down sets the
  overflow mark for `LocalOverflow` and clears it for any other cause, since
  a new connection brings its own answer. The feed closing while heard is
  `lost { socket_closed }` (the listener records a `Disconnect` before it
  stops, so this is a fallback), and nothing after it moves the account.
- **`Check` carries no offset in the machine**, which never stamps a request.
  The offset is the shell's. A machine is built checked, because the shell's
  `wire` will take a `Checked`.
- **`ready` takes the queue's state from the shell** (`Queue`), so the whole
  order of §3 is in one function and tested pair by pair.
- **`runlog check`'s doubt fold read a clearing as the end of every cause**,
  and named only an order's first; the permutation test found the check
  reporting different findings for two interleavings of one truth. The venue
  writes one `cleared` line per cause and a raise-and-clear as an adjacent
  pair, so a clearing now answers the latest open cause, and each cause still
  open is a finding (`docs/run-log.md` §9). The design had foreseen the fold
  seeing order through a `cleared` before its `raised`, which the machine
  never writes, and not through this.
- **A report held behind a hole is tested in two shapes, not one:** the hole
  after the order's `NEW`, so the order is heard and then held, and the hole
  swallowing the `NEW`, so the order is mentioned and never heard. Only the
  second can tell the two words apart, and "a held report counted as heard"
  reddened the test only once it was there.
- **The permutation test**, `crates/quant-explain/tests/live_merge.rs`, runs
  1,547 interleavings over 313 scenario variants: 47 truths (including
  every `WhyUnknown` reporting, silent and held, and four contradictions held
  to the invariants alone), each as it is and with a disconnect and its
  resubscribe, and an overflow, at every position among the order's records.
  Equality across interleavings cannot see a mistake every interleaving
  shares, so every run also ends with a checkpoint that `journal::agrees`
  must accept, and a consistent truth's findings must fit its readiness.
- **Held reports are not deduplicated by trade id.** *Precedence* 2 reads the
  ids booked, as the design says, so a resent copy of a held trade is held a
  second time. Nothing releases a held report before slice (d), whose release
  must deduplicate them (§6).

The shell's:

- **`Checked` did not exist.** The list of commits in §6 put it among the
  helpers, and nothing had built it. It is `quant-binance::trade::Checked`,
  minted only by `TradeClient::checked`, which runs `venue-check`'s three
  checks in its order and stops at the first refusal, and carries the
  offset the clock check passed with. It is not `Clone`: one check, one
  venue. `venue-check` itself still runs the three one by one, to print a
  line for each, and item 8's binary calls `checked`.
- **`wire` takes the clock too.** §1's list of what it takes left out the
  injected clock §4 says the worker stamps answers with; without it the
  worker would read a clock of its own. It takes the `VenueInstrument` by
  reference, as §3 says, and refuses a definition the translator could book
  no fee for or the encoder could write no order for (`WireError`), so a
  venue that could send nothing is never built.
- **After `stop_sending`, `ready` answers `QueueFull`.** §1 had the worker
  answer what was queued and the shell refuse held cancels, and was silent
  on what `ready` says then. A queue that takes nothing is what the variant
  says, as for a closed one, so the engine refuses rather than journalling a
  submission only the shell's `Unsent` would answer. A cancel asked for
  after it, and every held cancel, is the machine's `Unsent::Cancel`, its
  own input for "one after the shell stopped sending", which writes the same
  `cancel_refused { not_sent }` that a worker's `NotSent { "stopping" }`
  writes for a queued one.
- **`submit` asks `ready` again.** §3 called an order for another instrument
  reaching `submit` something only a `ready` that lied could let through,
  and said it is `never_landed` and sent nowhere. The shell makes that true
  of every reason: an order that reaches `submit` unready is `Unsent` with
  the reason in its words, with no `Submitted` before it, and goes nowhere.
- **A cancel names a working order.** "A cancel for an order ended or never
  sent" is the machine's to know, so `Merge::working(id)` answers it, and
  the shell asks before it queues a cancel.
- **`outstanding` is counted on the engine thread**: a request is
  outstanding from the `try_send` that handed it over until its answer is
  taken off the inbox, not until the worker sends the answer. So it reaches
  zero only once the engine holds every answer, which is what step 4 waits
  for. `Unheard` answers no request and does not count. The shell keeps
  what each request is owed, in order, rather than a count, for the next
  correction.
- **A worker gone is answered for, not waited for.** §3 said a closed
  queue "cannot happen in a release build", where `panic = "abort"` takes
  the process with a worker that panics. It can, when the binary shuts its
  runtime down before step 8, and then every answer the worker owed is
  lost. The first build
  only set a flag. The order it had sent stayed `Sent` with REST pending, so
  an account lost after it deferred its `account_blind` for ever and
  `runlog check` found the order `unwatched`, and step 4 waited out its
  whole deadline for answers nobody could send. The shell now sees the
  worker go when the worker's end of `orders` closes (not the inbox's own
  close, which item 8's rechecks would hold off), takes what the worker
  left on the inbox, logs the loss once as an error, and answers each
  request still owed `Unknown { Interrupted }`: an order in doubt, a cancel
  of unknown effect. It cannot tell a request the worker took from one
  still queued, so none is called certain. A worker never spawned leaves
  the queue open and is not seen gone, which is still what step 4's
  deadline is for.
- **The worker counts the answers that waited** (`WorkerStats::waited`),
  each one an answer or `Unheard` that found the inbox full and waited for
  the next poll. It is what lets a test see an answer wait rather than be
  dropped, and what shows an engine slow to poll.
- **An input with no stamp of its own takes the latest stamp heard.**
  `Polled::Closed` has none, nor does an answer the shell makes for a gone
  worker. The first build stepped both at the engine's `now`, which does not
  move on an idle wake, so an `account lost` could be dated before records
  drained ahead of it in the same poll. Both now take the later of `now`
  and the latest stamp the shell has stepped.
- **`Check` has no way in yet.** §1's sender table gives item 8's `Control`
  a sender on the inbox for rechecks, and the shell has none: `Answer` is
  the crate's own, and nothing on `LiveVenue` steps a `Check`. Item 8 adds
  the way in when it adds rechecks (§6), and the worker's end of `orders`,
  not the inbox, is already what says the worker has gone.
- **Equal stamps put the REST answer first.** The two-way merge needs a rule
  for a tie, and the permutation test shows the order across the two
  sources changes only the order of lines.
- **The timer is the worker's, on tokio's clock.** Every timer is
  `ORDER_TIMEOUT` long, so they come due in the order they were set and one
  queue holds them; each is set when its `Placed` goes on the inbox and
  stamped from the injected clock when it fires. Its length is fixed in
  production; a test shortens it only behind the `fixtures` feature, as a
  test shortens the trade client's order timeout only behind `fake-venue`.
- **The shutdown's two waits are code, `quant-live::drain`**, rather than
  only prose for item 8, because the shell's tests need them and the binary
  will call them. Their deadlines are measured on the host's monotonic
  clock: they pace a shutdown and decide nothing a replay must reproduce.
- **The account side of the end-to-end tests is the real listener's**:
  `fake_venue::Listening::feeding` wires the tee with its mark and hands
  back the `AccountFeed`, as a live run wires it. The REST side is
  `quant-live::fixtures::rest`, a scripted server shared by both crates'
  tests, which acknowledges by default and matches nothing, as §5 asks.

The binary's (item 8, 8a), each where the design was silent or the code
showed it otherwise:

- **`Check` has no way in, still, and 8a does not need one.** Nothing in 8a
  rechecks during a run: the `Checked` is minted at startup, a run is an
  afternoon, and an order refused `-1021` is a rejection with the venue's code
  on its line and harmless (§3, `Unchecked`). So the input stays as §6 left
  it, for whichever later item rechecks the clock or refetches
  `exchangeInfo`, and adds the way in with the recheck.
- **The baseline gate reads the balances; the lines are written after every
  gate.** §6 duty 1 lists "the start `balances` line" among the gates. The
  balances are fetched as the seventh gate, and `started`, with the run tag,
  and `balances` are written only once all seven pass, through a latch that
  stops at a failed append. A journal that cannot hold them is the baseline
  gate's refusal: no baseline, no trading.
- **A run that records only writes nothing to the journal, and opens no
  account stream.** The design was silent. A `started` line from a run that
  trades nothing would leave a session without a `stopped` line, which the
  last-session gate (duty 6) would then refuse the next run over, for a run
  that never sent an order. With no venue wired there is nothing to hear on
  the account stream, and a key the checks refused may be unable to log on.
  One case does leave such a session: `started` written and `balances` not.
  The next run then refuses until a person has looked, which is the
  conservative reading of a journal that failed mid-start.
- **The venue is wired, and every credential read, before the first line.**
  Review found two steps that could fail after `started` and `balances`
  were written: wiring the venue, and reading the account listener's
  credentials. Either would have left a session with no `stopped` line for
  a run that sent nothing, and every later run would have refused at the
  last-session gate, against the code's own comment. The listener's
  credentials are now read in the baseline gate beside the closing
  client's, and a venue that cannot be wired is the instrument's refusal;
  both record only and leave the journal as it was. The wiring failure
  cannot be reached today, since `exchangeInfo` refuses an empty asset name
  and its definition is always Binance spot, so only the credentials have a
  test.
- **The last-session gate also refuses a journal with lines and no
  `started` line**, whose last session cannot be read at all.
- **The size check is a gate, once, at the best bid.** §2 asked the binary to
  refuse a size whose exit falls under the minimum notional "at the current
  mark". Before the capture there is no book, so the mark is the best bid of
  a REST depth snapshot, the price a market exit sells at; the exit is the
  entry less the standard tier's taker fee in the base, truncated as the
  simulator truncates it, rounded down to the lot. A size that is not a whole
  number of lots fails it too, where `MaCrossover::new` would otherwise
  refuse it after the journal had been opened. It is not repeated per order:
  a market that falls during the run can still put an exit under the
  minimum, and the venue refuses that exit with its code on the line. The
  margin §2 asks for beyond one lot, the move expected between entry and
  exit, is the operator's to choose and is not checked.
- **The trade probe is the run's own size.** `venue-check` probes at M5's
  0.001; the live binary sends the same probe (`trade::probe`, moved into the
  library so the two cannot drift) at the size it will trade, so that a
  filter the run's orders would meet is met at the gate.
- **The clock is read once, through the run's clock**, by `trade::read_clock`,
  `venue-check`'s routine moved into the library: the offset the checks pass
  with is the one every order and logon is stamped with.
- **The run tag is the wall clock's milliseconds in base 36**, eight or nine
  letters and digits, so that a journal begun a millisecond after a lost one
  is under a different tag (§5).
- **`stopped` and the closing checkpoint are stamped at the engine's last
  instant**, `EngineStats::last_ts`, which runs to the last report the drain
  booked, so no line of the session is stamped after them; and they are
  `session::closing`, which `paper` writes too, as it always did.
- **The environment defaults to Spot Testnet**, and every endpoint follows
  it, so the run that spends no money is the one with no flag. The data root
  defaults to `data/<env>`, so a testnet market never lands beside the real
  one, and is given with `--root` rather than bare, so that an argument the
  binary does not take is refused without being printed back.
- **An endpoint override takes a loopback host only.** Overrides exist for a
  local stand-in. A host that is not loopback could be the other
  environment's, and review found that `--rest https://api.binance.com` on
  the default testnet would place real orders under a banner saying no money
  was at risk, as `--env live` with the testnet's market stream would trade
  real money on prices that are not the market's. So `--env` is the only way
  to choose a venue.
- **A refused gate exits 3, and a failed journal exits 1.** A refused gate
  is a run that did what it was told, recording, and said loudly why it did
  not trade; a supervisor that restarted it would meet the same refusal, so
  it is not a failure. It first exited 0, and review found two things wrong
  with that: a harness could not tell a run that traded from one that only
  recorded without reading its output, and a run whose strict writer had
  failed, withholding every order and writing no closing line, also exited 0
  with a verdict line that hid it. So 0 is a run that traded and stopped
  clean, 1 a run that could not go on (the journal's failure included), 2 an
  argument it does not take, and 3 a refused gate.
- **The account replay check reads the REST answers back off the journal**,
  as duty 7 says it must, and needs a stamp after which the stream was not
  heard: the session's `stopped` line, stamped at the engine's last instant
  (above). It replays the `Unheard` timer from each `fills_missed` raise,
  which no line distinguishes from a hole's, safely: a hole's raise makes the
  timer's a no-op. An acknowledged cancel writes no line and is not replayed
  (`docs/run-log.md` §10).
- **The account capture is a library function**, `capture::record_account`,
  which `fake_venue`'s `Listening` now runs too, so the tests record through
  the code a live run records through. It came with one test, because none of
  the existing ones saw the tee's mark go missing from the wiring.

---

## 6. The risk

**The sample is too small to measure what it is measuring.** Criterion C asks
whether the model is optimistic, and a dozen fills cannot resolve one basis
point. This is conceded rather than solved: C reports its own resolution, and a
run that cannot see the bound says so instead of claiming a pass. The alternative
— more orders, more capital, longer — buys resolution with the thing M8 is
deliberately minimising.

**The first failure will probably be a filter literal, and that is a good
outcome.** `tick_size` and `min_notional` are hard-coded in several places and
the venue publishes the real ones. Criterion B is designed to catch it, and
catching it with $5 at risk is the entire point of the milestone's size.

**A counterparty is not a neutral oracle.** Criterion A treats Binance's
`myTrades` as ground truth. It is better evidence than anything we have had, and
it is still one party's record of a shared event — if it and our record disagree,
the conclusion is "one of us is wrong", not "we are wrong". The raw capture of
the user-data stream is what makes that arguable rather than assumed.

**Three of the seven lifecycle states need the market to cooperate.** Accepted,
filled-in-full, cancelled, filter-rejected and funds-rejected are deterministic
at minimum size — we can provoke each on demand. A **partial fill** needs an
order larger than the touch, and a **cancel that loses the race** needs the
market to reach our price in the round trip. At 0.0005 BTC against a touch of
tens of BTC neither happens by asking politely. Both are reachable by placing a
limit order inside the spread and sizing it against the visible level, which is
the one place M8 deliberately trades at a size chosen for the test rather than
for the strategy — and it is bounded by being a single order, once.

**Real money changes what a bug costs and not much else.** Every mechanism this
run exercises has been exercised before against a simulated venue. The honest
summary of the risk is that the platform is well tested against a venue that
agrees with it, and has never once been tested against one that does not.
