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

Everything else people associate with "going live" — the HMAC, the API keys, the
order formats — is mechanical. These three are the milestone.

### The defect this scoping found, before any code was written

`Engine::run` blocks on `source.next_event()`
(`crates/quant-engine/src/lib.rs`), and `venue.poll()` is called in exactly one
place: inside `step`. So **execution reports are drained only when a market event
arrives**, and `LiveSource::next_event` calls `recv()` with no timeout.

In two of the three worlds this is invisible, which is why it has survived five
milestones. `SimulatedVenue` produces outcomes only in response to `observe`,
which happens inside `step` anyway — the coupling is real and costs nothing.

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
process, one account, enforced in `ops/start-run.sh` rather than left to care.

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
venue's response. On restart the system must find the order via `openOrders`,
match it to the journal entry by client order id, and resume — or, if the venue
has no such order, record that the request never landed. It must not come back
with `MaCrossover.working` latched forever on an order nobody will ever hear
about again.

---

## 4. Slices

| | Slice | Content |
|---|---|---|
| a | The engine can receive what a venue says | wake on either input; `step_reports`; forward-only clock |
| b | Credentials, signing, and refusing to run without them | HMAC, key handling, `preflight` |
| c | `LiveVenue`: submit, cancel, and the user-data stream | the translator |
| d | The indeterminate window | `InDoubt`, `openOrders` recovery |
| e | Reconciliation against the venue | criteria A, B, C as a binary |
| f | The run, and the lifecycle checklist | criterion D, criterion E |

Slice (a) is first and is not negotiable: it is a seam change, every slice after
it depends on the guarantee, and it is the one thing that can be built and tested
with **no credentials and no money** — a fake transport is enough. Doing it last
would mean discovering during a funded run that the engine cannot hear.

---

## 5. Decided before the code

**`ClientOrderId` is the idempotency key, because it already is one.**
`docs/engine-contract.md` §3 made it exist before the request leaves, for
correlation. That property is exactly what an idempotency key needs, so it goes
to the venue as `newClientOrderId` — and a retry after a timeout cannot open a
second position, because the venue refuses the duplicate. It carries a
run-scoped prefix (`q-<run>-<id>`): the bare counter restarts at 1 after a lost
journal and would collide with orders still resting.

**One authority per fact.** `Accepted` with its venue order id, and every
`Filled`, come from the **user-data stream**. Rejections come from the REST
response, because the stream reports on orders and a rejected request was never
an order — which is precisely why `VenueOrderId` is optional in the execution
contract. `newOrderRespType=ACK`, so a fuller response cannot deliver a fill the
stream will also deliver.

**The user-data socket is recorded into the raw tier before it is parsed.**
Market data has been immutable-bytes-first since M1 on the argument that our
model of the venue may be wrong. Nothing about execution reports weakens that
argument; everything about money strengthens it. A new `FrameKind`, bytes
verbatim, container version 2 → 3 with a migration note in
`docs/data-contract.md` §6. Without it, a disagreement between our record and the
venue's can only be re-litigated from our own interpretation of what it said.

**`LiveVenue::submit` does not await.** Stamp, format, `try_send`, return. The
engine loop is synchronous and must not block on a round trip — that is the whole
of §2's fire-and-forget argument, and the first place it has ever actually
mattered. A full outbound channel is **our** failure and gets its own
`RejectReason`, distinct from a venue rejection, on the same argument that made
`RefusedBy` distinguish the seam from the risk layer.

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

**The kill switch must stop a live run, and stopping is not flattening.** M6
decided that a tripped switch refuses everything including the order that would
close a position, and named the trade-off: closing out becomes manual. That was
cheap in paper. It is a real decision with real money and it is **not** reversed
here — a switch that trades after tripping is not a switch — but the run
procedure must state plainly that a trip leaves a position open and a human
closes it.

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
