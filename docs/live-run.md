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
| c | `LiveVenue`: submit, cancel, and the user-data stream | **part done**: `FeeAsset`, the translator (`user_data.rs`), the order encoder and classifier (`orders.rs`), the order transport (`TradeClient::place_order`, `cancel_order`), the instrument from the venue (#60), the engine seam (#65), the account stream's frame kind (#63), the journal's entries (item 4, `m8c-journal-live`) and item 4's readers (`m8c-journal-readers`); items 6, 7 and 8 below remain |
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
7. **`LiveVenue`.** A state machine merging REST outcomes with stream reports: one
   terminal event per order, fills deduplicated by trade id, the in-doubt state
   (§5). Behind it, `impl ExecutionVenue`. Accepted by a permutation test over
   every interleaving of the REST reply and the stream's reports, and by a test
   that losing the account stream puts every working order in doubt
   (`account_blind`): `runlog check` treats an `account` line as covered by
   those doubts and does not flag it.
8. **The `live` binary and a rehearsal.** Paper's session extracted so any venue
   can run it; a startup that records first and trades last; a refusal of any
   size whose floored exit would fall below the minimum notional at the current
   mark (§2); a live mode in `ops/` enforcing one symbol. Accepted when a run on Spot Testnet (§5) reaches
   criterion D's states 1, 2, 4 and 6, `reconcile` agrees, `quant-verify` is clean
   on both data roots, and replaying the recorded account frames through the
   translator reproduces every venue-produced journal entry.

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
refusing when there is no mark.

**The engine clock becomes forward-only.** `now = max(now, ts)`, with the clamps
counted. Two asynchronous inputs can deliver out of order; `HistoricalSource` is
monotone, so this must change no backtest number to the last digit — the same
no-op property that made `Costs::NONE` checkable. Mirrors
`quant-recorder::segment`'s never-roll-backwards rule and its `backdated_records`
counter.

Half of this shipped with slice (a). The clock advanced from venue reports on
idle wakes only, so on a market wake a later report was journalled at its own
time while the strategy's clock stayed earlier, and the journal's timestamps
could run backwards. `runlog`'s `decisions_at` stops at the first entry stamped
after the instant it is asked about, so one entry out of order hides every entry
behind it. The market-wake half landed in `m8a-live-source-idles` (#52).

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
the socket closing, the idle timeout firing, a `serverShutdown` event, a
resubscribe that fails. A subscription belongs to one connection and nothing is
replayed after a reconnect, so every reconnect resubscribes, and an order working
across the blind stretch is in doubt (below) until slice (d)'s lookup settles it.
The venue's pings every 20 seconds prove the socket, not the subscription;
`session.status` reports whether we still hold one.

**The adapter refuses before an order is journalled, not after.** By the time
`ExecutionVenue::submit` runs, `on_submitted` has written the order down and the
engine's ledger holds it, so the only way back would be a `Rejected` through
`poll`, recorded as *"the venue declined an order it had been told about"*, which
is false for an order that never left. So every refusal that starts in the
adapter (no venue filters, a full outbound queue, the account stream down, an
order in doubt, a failed clock check) is a readiness check the engine consults
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
