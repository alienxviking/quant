# The engine contract

What sits between a market event and an order, and why it is shaped this way.

`docs/data-contract.md` is the contract for **data at rest**. This is the
contract for **the engine seam** — the boundary a strategy sees, and the promise
that it is the same boundary in a backtest, in paper trading and in production.

Written before the engine exists, for the reason M0 wrote the data contract
before the recorder: the seam is the part that is expensive to change once
anything depends on it, and "we will tidy the interface later" is how a backtest
ends up describing a system that was never run.

---

## 1. The property everything else serves

> **A strategy must not be able to tell which sources it is wired to.**

Backtest is `HistoricalSource + SimulatedVenue`. Paper is
`LiveSource + SimulatedVenue` — see §9, there is no separate paper venue. Live
is `LiveSource + LiveVenue` **[M8]** — and the mark is not decoration:
`LiveVenue` exists (`quant-live`, M8's slice (c)) and runs end to end in tests
against local servers, but no binary wires it yet and no order has gone
through it to the venue, so the third wiring names what this seam is built
for rather than something that has traded. One strategy
binary, three worlds, no code changes between them.

If a strategy *can* tell, the backtest is measuring a system that will never
exist. Every decision below is downstream of that one sentence, and where a
decision is inconvenient, this is why it was made anyway.

---

## 2. Submission is fire-and-forget

```rust
fn submit(&mut self, request: OrderRequest) -> ClientOrderId;   // returns immediately
```

**Not** `-> Result<Fill>`. The synchronous version is the obvious API and it is
the single most damaging thing that could be put in this file.

A live venue cannot answer without a network round trip. A synchronous return
would therefore either block the engine on the wire, or lie. Meanwhile a
simulated venue *can* answer instantly — so a strategy written against a
synchronous API gets, in backtest, information it will never have in production:
the outcome of its own order, at the moment it places it. Strategies that look
profitable on that are measuring a machine that does not exist.

So submission returns only an identifier, and outcomes come back the same way
market data does — as events, later, into the same loop. In a backtest the
simulated venue produces them synchronously with the market events that caused
them; live they arrive on a channel. **The strategy's code is identical, and it
is forced to handle the asynchrony that real trading has.**

The cost is real: a strategy must track its own outstanding orders. That cost is
the point. It exists in production whether or not the backtest charges for it.

---

## 3. Two identifiers, and why the venue's is not enough

| | Assigned by | Assigned when | May be absent |
|---|---|---|---|
| `ClientOrderId` | us | **before** the request leaves | never |
| `VenueOrderId` | the venue | on acceptance | **yes** |

A rejected order may never receive a venue id at all — the venue rejected the
request, so there was never an order to name. If the venue's identifier were the
only one, a rejection could not be correlated with the submission that caused
it, and the strategy would be left with an outstanding order it can never
resolve. So every request carries an identifier we minted, and every execution
event quotes it back.

The venue's identifier is recorded when it arrives, because it is what a human
reconciling against an exchange statement will search for. So is the venue's id
for each **trade** (`VenueTradeId`), because one order can fill in several, and
`myTrades` names a row by its trade. Its own type rather than a second
`VenueOrderId`: the order id and the trade id are both integers on one
`myTrades` row, and a swap must not compile.

**Both reach the record and never the strategy.** Until M8 slice (c) the
acceptance event carried the venue's order id, which the live translator filled
and the simulator left empty, so a strategy could tell the two apart by reading
it. The ids now travel *beside* the event, to the observer alone (§11), and the
field is gone.

---

## 4. What a strategy is allowed to see

A strategy sees, per event:

- the [`MarketEvent`](../crates/quant-core/src/event.rs) itself, including `Gap`;
- a **read-only reconstructed book**, maintained by the engine;
- the engine clock;
- its own execution events — the event alone, never what a venue says beside it,
  and only for orders the engine is working (§11);
- its **position, cash and equity**, maintained by the engine rather than by the
  strategy — same argument as the book, and it is the accounting a restart has to
  restore (§9), so there is exactly one of it. `equity` is `None` when there is a
  position and no price to mark it at.

It does **not** see the venue, the event source, wall-clock time, or any other
strategy's orders. Nor anything only a real venue knows: its ids for orders and
trades, the code it cited when it declined, why it was not ready, a cancel it
did not carry out, a lost account stream. Those are the record's (§11).

**What a strategy is told is closed by the compiler, not by this paragraph.**
Every field of every `ExecutionEvent` variant must be of a type a simulator
produces exactly as a live venue does (`quant-core/src/execution.rs`,
`Modelled`), with no `Option<T>` let through, because an optional field is how
the venue's order id got in. A field added to an event does not build until its
type is argued into that list.

**The engine maintains the book, not the strategy.** Book reconstruction is
subtle enough to have taken a milestone; every strategy re-deriving it would be
every strategy re-deriving it *differently*. Because an invalidated book is
cleared rather than flagged (§8 of the data contract), a strategy that reads
`best_bid()` after a gap gets `None` rather than a stale price — so "refuse to
trade across a gap" holds by construction rather than by discipline.

**Wall-clock time is not reachable.** Invariant 4: components take a clock. In a
backtest the clock is driven by `local_recv_ts` and by nothing else; a strategy
that could call the system clock would behave differently in the three worlds,
which is the property in §1.

---

## 5. Risk is a chokepoint, not a call

The strategy holds no reference to a venue. It submits through a context handle,
and that handle routes through the risk layer on the way out. There is no path
from a strategy to a venue that does not pass through it.

Empty until M6 — but *present* from M3, because retrofitting a chokepoint means
auditing every call site that grew up around its absence.

Since M8 slice (c) the handle asks four things, in this order, and an order any
of them refuses never reaches the venue: the **seam** (is it well formed?), the
**record's latch** (has the record already failed to hold a submission?), the
**venue's readiness** (can it take an order now?), and **risk**. Then the record
is told, and may still withhold it. The order is argued in §11; the part that
belongs here is that readiness comes *before* risk, so an order a venue could
not take spends none of the day's order count and cannot trip the switch, and
that every one of these refusals reaches the strategy the same way, as a
`Rejected` after the callback.

---

## 6. Ordering and the clock

The engine dispatches on `local_recv_ts` and only on `local_recv_ts` (invariant
2). Execution events generated by a simulated venue are stamped with the engine
clock at the moment they are produced, so they interleave with market data in
the order a live system would have seen them.

A backtest advances the clock **only** when an event arrives. There is no
between-events time, which means a strategy cannot act on a schedule the data
does not justify.

Live there are two inputs, and both move the clock: a market event, and anything
the venue says — an order report or a notice — stamped when its bytes arrived.
A venue input stamped later than the clock carries it forward before anything
else on that wake reads it, so a strategy is never told of something dated after
the time it reads, and the record stays in time order. Nothing moves it back; a
market event stamped before the clock is counted as a clamp. A wake on which the
venue said only notices calls no strategy method at all (§11).

---

## 7. Acceptance criteria for M3

The engine seam is done when:

- [x] One strategy binary runs against `HistoricalSource + SimulatedVenue` and
      produces an **equity curve**. *(`backtest`, over the acceptance week.)*
- [x] The equity curve is **unimpressive**. *(BTCUSDT $100 → $97.53 over 8 days,
      −2.47%, 4.40 max drawdown, 209 round trips — and that is **before any
      costs exist**. ETHUSDT $100 → $99.18.)*
- [x] The `Strategy` trait has no method, and its context no accessor, that
      reveals which source or venue it is attached to. *(Checked by running the
      same strategy value against a filling venue and an accept-only one, and
      comparing the resulting state.)*
- [x] A strategy cannot reach a venue except through the risk layer. *(Checked
      with a refuse-everything layer: the venue is never **told**, not merely
      declining.)*
- [x] A strategy that reads prices across a `Gap` gets `None`, not a stale
      quote.
- [x] No wall-clock call anywhere in the engine or strategy path: the backtest
      is deterministic, and running it twice produces byte-identical results.

**M3 is complete.**

### Why "unimpressive" is a criterion

A moving-average crossover that looks profitable on its first run means the
harness is lying. The usual culprits are all invisible in the output:

- dispatching on `exchange_ts` (lookahead — invariant 2),
- filling at a price the book never offered,
- filling instantly at a size the touch could not absorb,
- ignoring fees entirely (M4's job, absent here by design),
- trading across a gap as though the market had stood still.

Every one of those inflates the curve. So a *good-looking* first curve is
evidence of a bug, and a bad-looking one is evidence the plumbing is honest.
The strategy is deliberately naive so that the result is attributable to the
harness rather than to the idea.

M4 adds fees, slippage and latency and the curve is expected to get **worse**.
If it improves, something in M4 is wrong.

### What the first run actually showed

Every number cross-checked, which is what made the result believable rather than
merely disappointing:

| | |
|---|---|
| Samples | 10,080 — exactly 7 days of minutes |
| Accounted for | 9,810 with a mid + 270 blind = 10,080 |
| Signals to fills | 418 crossings → 418 orders → 418 fills |
| Paired | 209 entries + 209 exits, so it never got stuck holding |
| Realized P&L | equals the cash change exactly; final equity equals cash, because it ended flat |
| Book walk | 2 fills consumed more than one level, at $76 a position |
| Gaps | 19, and **zero** orders refused for want of a market |

That last row is the one worth dwelling on. The strategy contains no mention of
gaps at all. A gap clears the book, so there is no mid, so no sample is taken, so
the indicator does not advance and no crossing can fire. "Do not trade across a
gap" is structural here — three separate decisions (cleared not flagged, sample
on a clock, mid from the book) compose into it without anyone enforcing it.

### What M3 explicitly does not do

- **No fees, slippage or latency.** M4 — now done; see §8.
- **No risk limits.** M6. The chokepoint exists and passes everything.
- **No live or paper venue.** M5 and M8. The seam is built so they slot in.

---

## 8. Acceptance criteria for M4

Costs are done when:

- [x] Cost models set to zero **reproduce the M3 numbers to the last digit**.
      *(`Costs::NONE`: $97.5291254, realized −2.4708746, drawdown 4.3996746.)*
- [x] Fees are **monotone**: for rates `a < b`, the result under `b` is never
      better. *(Tested at the unit level and end to end.)*
- [x] Fees are visible **separately from the price result**, so "profitable
      before costs and not after" can be read off the output rather than
      inferred.
- [x] The venue and the portfolio **agree** on what was charged — two
      independent tallies of one number.
- [x] The output distinguishes **cannot be modelled** from **you set it to
      zero**.

**M4 is complete.**

### The answer

BTCUSDT, the acceptance week, 209 round trips:

| costs | final equity | fees | drawdown | gross P&L |
|---|---|---|---|---|
| free (= M3) | 97.53 | 0 | 4.40 | −2.47 |
| 1 bps | 94.25 | 3.28 | 7.00 | −2.47 |
| 7.5 bps | 72.94 | 24.59 | 27.30 | −2.47 |
| **10 bps (Binance spot)** | **64.74** | **32.79** | **35.45** | −2.47 |
| latency 50 ms | 97.57 | 0 | 4.37 | −2.43 |
| latency 500 ms | 97.54 | 0 | 4.41 | −2.46 |
| `--realistic` | 64.79 | 32.79 | 35.41 | −2.43 |

The strategy loses 2.5% on price and **33% on commission**. 418 fills at ten
basis points on a $76 position is 43% of position value in a week, so gross
returns would have to beat that to break even. That settles this strategy class
at this turnover, which is what M4 was for.

### Latency is a variance, not a cost

At 50 ms the result got slightly **better**, and that is not a bug.

Fees subtract a known amount. Latency does something else entirely: it moves the
fill to a *later book*, and over a 60-second sampling horizon the sign of that
move is a coin flip — 50 ms is one twelve-hundredth of a bar. So "latency must
never improve results" would have been a **wrong** criterion, and the tests
deliberately do not assert a direction. What they assert is that it changes
something: a latency model that altered no fill would be a field, not a model.

Latency becomes a systematic cost only for a strategy fast enough that the
market's move during the round trip is correlated with the reason it traded.
This strategy is nowhere near that, and the model says so rather than inventing
a number.

### What is still not modelled, and why not here

- **Queue position.** Our order was never in the recorded book, so there is no
  way to know what was ahead of it. The simulator's answer is to fill a resting
  order only on a trade-through, which understates passive fills.
- **Market impact.** Nobody in the recording reacted to us, because we were not
  there. Not recoverable from recorded data at any price.

This used to end: *"Both are why **M5 exists**: paper trading against the live
venue is the only instrument that can measure them."* **That was wrong**, and it
is corrected here rather than quietly deleted because the mistake is the
instructive part. §9 recorded the correction on 2026-09-02; this sentence
survived eighteen days further up the same file, through a documentation pass
whose own commit message (`f211430`) says it fixed the claim here, while
`docs/paper-run.md` went on to tell readers the correction was recorded in this
document. It was — once, a section later, beneath the version that contradicted
it. That is the silent-miss failure this project has twice recorded for scripted
edits to source, arriving in prose, and the remedy is the same one: after an
edit, grep for the thing that should now be gone.

A paper venue's fills are *simulated*. Our orders are still not in the book and
nobody is still reacting to them, so a fortnight of paper trading measures
neither of these two — it is the same recorded book, arriving live. §9's *What
paper trading cannot measure* carries the full correction and what M5 is worth
instead.

**And this used to end "both are why M8 exists".** That is a second correction to
the same two sentences, made while scoping M8 in `docs/live-run.md` §2. Real
orders *resting* in the real book are indeed the only instrument that can measure
these — but M8 as scoped sends `OrderKind::Market` exclusively at the venue
minimum, and an order that never rests has no queue position while $5 moves
nothing. M8 measures the **adapter**. Microstructure needs a passive strategy and
real size, and is a later milestone that does not yet have a number.

---

## 9. Acceptance criteria for M5 — paper trading

The seam is proven in a second world when:

- [x] The **same strategy binary** runs against `LiveSource + SimulatedVenue` for
      **two weeks**, unattended, with no code change from the backtest wiring.
      *(met: 2026-09-18T14:46:54Z → 2026-10-02T14:47:08Z, two symbols, zero
      supervisor restarts until the deliberate one at the deadline.)*
- [x] **Paper P&L matches a backtest over the data captured during the same
      window.** *(met, exactly, on both symbols: BTCUSDT 824 fills and cash
      31.64743842 on each side; ETHUSDT 822 and 97.65757574. Realized and fees
      identical to the satoshi. Not a tolerance — equality.)*
- [x] Every fill is journalled, and P&L recomputed from the journal alone agrees
      with the engine's running portfolio. *(met: 166 checkpoints per symbol,
      `reconcile` AGREES on both, plus 110 clean in-run reconciliations over the
      fortnight.)*
- [x] The run survives restarts: position and cash are recovered from the
      journal, not lost. *(met, and not by design: the supervisor's deadline ran
      46 s past its child's, so it started a second 60-second attempt after the
      paper binary had finished. That attempt resumed from the journal — 824
      fills already booked, starting cash taken from the recompute — and the
      exact match survived it. "A restart forfeits the exact match" below was
      the fear; what actually happened is that a restart producing no fills
      forfeits nothing.)*

### Why none of these was ticked until the run ended

`docs/paper-run.md` has the same four, in the same order, and says only the
first, third and fourth can be judged *during* the run. They were judged, every
six hours for a fortnight, and all 110 passes were clean. That was evidence and
it was deliberately not a tick.

Three of the four are statements about **two weeks**. A criterion about an
unattended fortnight is not met at hour thirty, any more than M1's seventh
criterion was met on day two — that one was spent in full rather than shortened
for convenience, and it is the precedent this one followed. So a box here was
ticked at judging, by the commands in `docs/paper-run.md` run against the
finished artifacts; a clean six-hourly pass was evidence that the run was still
worth finishing, not a partial pass.

Worth keeping now that it has been exercised: the tick came from re-running the
judging pipeline *twice*. The first pass used a build carrying a part-ordering
bug found mid-run; the numbers were re-derived from raw on the fixed build before
anything was ticked. Raw being the source of truth and the normalized tier being
disposable is what made that cost about ten minutes rather than the fortnight.

The two documents carry the same four criteria in the same order on purpose. One
is the procedure and this one is the bar, and if they were free to describe
different criteria the run could pass the procedure and miss the bar with nobody
noticing.

### Why the criterion is live-versus-replay, not an arithmetic check

"P&L reconciles against an independent recompute" checks arithmetic against
itself. It would pass on a system whose *live path* and *replay path* disagreed
about what the market did, which is the failure that would invalidate every
result this platform ever produces.

So the criterion is sharper: run the strategy live on a paper venue, capture the
raw stream while doing it, then **backtest the identical strategy over that
capture and require the two to agree**. If they diverge, one of the two paths is
lying and we find out which. It is the live analogue of the *data* contract's
§8 raw-versus-Parquet agreement — §8 of this file is fees — and it is the
strongest statement available without real money.

Exact agreement is the target and is achievable, because both sides consume the
same events with the same `local_recv_ts` and the same `ingest_seq` — see the
tee below. Any divergence is a bug, not a tolerance.

### A restart forfeits the exact match, and the reason is state, not reading

Criteria 2 and 4 pull against each other, and this was not understood when they
were written. Every document in this repository has framed a mid-run supervisor
restart as a **reader** problem: two sessions land on one symbol-day, and until
M2.e that made `normalize --write` refuse the day rather than merge it. That
half is closed — such a day is now a sequence of parts, concatenated in index
order. What makes that index order trustworthy is established when the parts are
*written*, not when they are read: a later session is refused rather than
reordered unless the venue's own update-id span in its Parquet footer starts
after every published part's ends, because overlapping spans mean the two
recordings were concurrent and there is no ordering of them that is the truth.
The half that is not closed is **state**, and it is the half that decides the
criterion.

`JournalEntry` had five variants when this was written — a start, fills,
checkpoints, a kill-switch trip and a stop. M7.5 took it to fourteen, adding the
order lifecycle (`Submitted`, `Refused`, `Accepted`, `Rejected`,
`CancelRequested`, `Cancelled`, `Orphaned`), blindness (`Blind`) and the
strategy's own notes (`Note`) — behind an exhaustive `match` with no `_` arm, so
a fifteenth cannot silently fail to move money. Schema 3 (M8 slice (c)) took it
to twenty-four, with what a live venue says on its own (`Placed`,
`NeverLanded`, `Doubt`, `CancelRefused`, `Foreign`, `Account`, `Anomaly`,
`Stray`), the market's return (`Sighted`) and the account's `Balances`; each was
decided at every exhaustive site, and none moves money (`docs/run-log.md` §8).

What has **not** changed is the sentence this paragraph exists for: there is
still no entry from which in-memory strategy state could be rebuilt, and
`Engine::resuming` (`crates/quant-engine/src/lib.rs`) still replaces the
**portfolio** and nothing else. A `Note` is a record of a decision, not a
checkpoint to resume from — `docs/run-log.md` §2 refuses that in writing. So a restarted paper process comes
back up with the right cash and the right position — which is exactly what
criterion 4 asks for, and it is worth having — and with **empty `MaCrossover`
windows and a zeroed equity sampler**. When this was written it came back with
a zeroed daily risk tally too, because `RiskEngine::recover` carried the
switch across and nothing else; since `m8c-journal-readers` the tally is
recounted from the journal (`journal::recover_risk`, §10), so the risk layer
comes back as far as the journal holds it. §10 lists where that stops: an order
in flight at the cut, a schema-1 journal's order count, a withheld order's
count, and the refusal total.

The judging backtest has no such boundary. It runs the indicator and the sampler
continuously across the same instant, so after a restart the two
sides are not the same system in the same state: the crossings differ, therefore
the fill counts differ, therefore criterion 2 fails — **for a reason that is not
live-versus-replay**, which is the only thing it was asked to test. And it cannot
be repaired afterwards. The windows the live process would have held are not
recorded anywhere, so no amount of re-reading the capture reconstructs them.

**The fallback, if the fortnight does restart, is to compare per segment between
restarts rather than end to end.** Each stretch of run between two restarts is a
continuous paper session; a backtest over exactly that window is a fair
comparison; and the claim the milestone can then make is "every segment agrees
exactly" rather than "the run agrees exactly". That is genuinely weaker — it
says nothing about the boundary itself, which is where the divergence would be —
and whatever reports the run has to say so in those words rather than quoting the
segments as though they were the run. The strong form needs a fortnight with no
restart, which is what this one has had so far.

Carrying strategy state across a restart is the real fix and it is not this
milestone's. It needs a new journal entry for what the strategy held, which is a
schema change — and rule 2 of `docs/paper-run.md` freezes the journal schema
until the run ends. So it belongs with the run log, the first thing due when the
freeze lifts.

### Slices, and where this stands

| | Slice | Status |
|---|---|---|
| a | `TeeSink`: one ingress, two consumers | **done** |
| b | `LiveSource`: the venue socket as an `EventSource` | **done** |
| c | The journal, and a reconciliation that can fail | **done** |
| d | The `paper` binary: `record()` extracted so it can feed the tee | **done** |
| e | Ops harness and rehearsal | **done** |
| — | The fortnight itself | **running since 2026-09-18, ends 2026-10-02** |

The procedure is `docs/paper-run.md`. Everything except wall clock is built and
rehearsed against the live venue.

**M5's fortnight ran 2026-09-18T14:46:54Z → 2026-10-02T14:47:08Z and passed.**
Two symbols on the Mac, one paper process each, pinned to the tag `m5-run-start`
(`aff848d`). 97,937,822 frames captured with `missing 0` and zero findings; both
symbols replay with 0 chain breaks and book invariants holding at every tick; the
Parquet tier reproduces the raw stream event for event; and the comparison came
out exact on both symbols.

Same shape M1 had — code complete 2026-08-11, acceptance run passed 2026-08-28,
seventeen days apart and nothing rotted, because the harness was rehearsed and
the reasoning was written down. Here it was thirty days, and in the middle of it
two whole slices (M2.e and M7) were built in a worktree without touching the
run — one of which is the only reason the fortnight is judgeable, since the
harness restarted the recorders in the final minute and put two sessions on the
last day.

### There is no `PaperVenue`

`CLAUDE.md`'s diagram *listed* `SimulatedVenue`, `PaperVenue` and `LiveVenue` as
three implementations. Building M5 established that the middle one does not need
to exist, and the diagram has since been corrected — this section is why.

A paper venue fills orders against a reconstructed book at prices the book
showed. That is exactly and entirely what `SimulatedVenue` does. What separates a
backtest from paper trading is the **source** and the **durability**, not the
matching — so the paper wiring is `LiveSource + SimulatedVenue`, and the
three-venue row is really two: simulated (M3) and live (M8).

Writing a second fill model to satisfy a diagram would have given the project two
things that must agree forever and no way to notice when they stopped. That is
the mistake M2.c, M4 and M5.b were each about avoiding, arriving from a fourth
direction.

### The reconciliation has to be able to fail

The first version of `reconcile` checked that `cash - starting == realized -
fees` and exited 0. That check is **vacuous**: the recompute derives all three
from the same fill lines, so the identity holds by construction. It was
decoration wearing the costume of evidence.

A real check needs two *independent* computations of one quantity. So the engine
writes `Checkpoint` entries stating what it believes, and the recompute has to
arrive at the same numbers from the journal alone. Verified against hand-built
journals: a wrong cash figure fails, a fill acted on but never written down fails
and is named as such, and a journal with **no** checkpoint exits 2 rather than 0
— because "nobody disagreed" and "two independent answers matched" are different
statements, and only one of them is evidence.

That is the same pattern as the venue and the portfolio each totalling fees, and
`quant-verify` and `quant-normalize` each counting frames. Each time the value is
not the second number but that a disagreement becomes visible.

### What paper trading cannot measure

An earlier draft of this file claimed M5 was the instrument for queue position
and market impact. **That was wrong**, and the correction matters because it
changes what M5 is worth.

A paper venue uses *simulated* fills. Our orders are still not in the book, and
nobody is still reacting to them. The two things M4 could not model remain
unmodelled and unmeasured after M5. **Real orders and real money are necessary
and not sufficient**: M8 as scoped sends market orders at the venue minimum,
which never rest and move nothing, so it measures the adapter rather than the
microstructure — see `docs/live-run.md` §2. What M5 proves is that the engine
runs continuously against
a live socket, that the architectural claim holds (one binary, two wirings), and
that live and replay agree.

### One ingress, two consumers

The paper run records raw **and** trades from the same stream, and they must be
the same stream — not two subscriptions, not a tail of the file being written.
`Ingress<S>` is generic over a sink precisely so a `TeeSink` can hand each record
to both the capture writer and the engine, with the identical timestamp and
sequence number. That identity is what makes the agreement criterion checkable
rather than approximate.

The capture is the **primary** sink and the engine the secondary. If the engine
falls behind, its record is dropped and the capture is untouched: market data is
irreplaceable and a paper fill is not. The engine then discovers the drop the
same way the offline verifier discovers a recorder drop — a hole in `ingest_seq`
— and responds the same way, by treating it as a gap and clearing the book. Same
evidence, same remedy, in a second place.

---

## 10. Acceptance criteria for M6 — the risk engine

The chokepoint is filled in when:

- [x] Limits **provably veto a misbehaving strategy, under test.** Two are in
      the suite: one that buys 1000 units on every event, one that submits
      forever.
- [x] A vetoed order **never reaches the venue** — not declined by it, never
      told to it.
- [x] The refusal reaches the strategy as an execution event, so it can tell it
      was stopped.
- [x] A tripped kill switch **survives a restart**.
- [x] Limits that do not bind **change nothing**: a permitted run is identical
      to one with no risk layer at all.

**M6 is complete.**

### Why risk keeps its own tally

`RiskEngine` tracks position and realized P&L itself rather than reading
`Portfolio`. Deliberate duplication — the one kind this project accepts.

A limit computed from the accounting can only be as correct as the accounting. If
the portfolio has a bug, the limits go wrong in the same direction and stop
protecting anything *precisely when something is already wrong*. An independent
tally means a portfolio bug cannot silently disable a limit.

Fourth time this pattern has paid: the venue and the portfolio each total fees,
`quant-verify` and `quant-normalize` each count frames, the journal and the engine
each claim a P&L. Each time the value is not the second number but that a
disagreement becomes visible.

The tally is deliberately *simpler* than the portfolio's. A risk limit needs to be
obviously right rather than exactly right, and forty lines readable in one sitting
is worth more here than agreement to the satoshi.

### The decisions inside it

**A money limit refuses when there is no price.** You cannot size what you cannot
price, and "assume the last price" makes a limit widest exactly when the market is
least understood. But an order with *no* money limit configured passes through a
gap — a limit nobody set should not have an effect.

**The position limit checks the position the order would create**, not the one we
have. The latter lets every order through right up to the one that mattered.
Exposure is absolute, so a short counts: a limit that only looked at longs would
permit an unlimited short, which is the more dangerous direction.

**The position limit never blocks a reduction of a position.** Risk judges an
order against its own tally, built from every fill it has been told about, and
after a restart from every fill the journal holds (below). An order that leaves
the position on the same side, or flat, and strictly smaller is exempt from the
limit, because a risk layer that trapped a position it considered too large
would be the most dangerous thing in the system. A flip is not a reduction: the position it opens is checked like any
other. The exemption is from that one limit. A reducing order still meets the
per-order limit (a fat finger on the way out is still a fat finger, and an exit can
be split), the no-price refusal and the daily limits, and a tripped switch refuses
even a flattening order, as below. "Never" is about the position risk has been
told of. An orphaned fill moves real exposure that neither the portfolio nor
the tally holds, so live the limit can misjudge a position an orphan changed;
that is why a single orphan fails criterion A and `runlog check`, and why an
order in flight at a restart is slice (d)'s to recover (criterion E) rather
than an orphan to live with.

**Fees count against the daily loss budget.** A limit ignoring them would be
reached late by exactly the amount a high-turnover strategy pays — which §8 showed
is most of the damage.

**Loss and order-count limits *trip* rather than refuse**, because a strategy in a
loop does not stop being declined once. A refused order does not consume the daily
count, or the count would measure our refusals rather than the strategy's
activity.

**A tripped switch survives a restart, and a new day does not clear it.** A kill
switch that forgets is not a kill switch: the supervisor exists to restart a dead
process, so a switch held only in memory would be re-armed by the machinery meant
to keep things running. Whether to resume tomorrow is a decision, not a timeout.
The day never rolls backwards either, so an NTP step cannot hand a stopped
strategy a fresh budget.

**A restart recounts the tally from the journal, and the switch with it.**
`journal::recover_risk` starts from `RiskEngine::recover(limits, last_trip)`
and folds the journal through the risk layer the way the earlier process
counted it: a `submitted` line is an order the check passed, recounted with
`RiskEngine::recount(at, true)`, which rolls the day and counts it; a risk
refusal is one the check ran on and turned away, `recount(at, false)`, which
rolls the day and counts nothing; a `filled` line goes to `on_fill`, which
moves the holding and the day's realized P&L, fees included; and every other
line does nothing, listed by name in an exhaustive `match`. Until
`m8c-journal-readers` a restart brought back the switch and nothing else, so an
addition to a real position was checked as if from flat, a real exit could be
refused as if it opened the opposite position, and the day's loss budget and
order count started fresh: an NTP step could not hand a stopped strategy a
fresh budget, and a restart could. A new UTC day in the file still resets the
counters and never the switch, because the recount rolls the day exactly where
the live check did. The limits are the restarted process's own; the counts
are facts about the day.

The recount is as complete as the journal and no more, and four things fall
outside it. An order in flight at the cut: in paper its fill never comes,
because the new `SimulatedVenue` never heard of the order, and live it arrives
as an orphan, which risk is never told of; the uninterrupted tally would hold
it, and recovering it is criterion E's lookup in slice (d)
(`docs/live-run.md` §3). A schema-1 journal, the fortnight's, has no
`submitted` lines, so its order count recounts to zero. An order a strict
writer withheld after its first failed append did spend a count in the
earlier process, and the file does not hold it. And `refusals()`, a reporting
total rather than a limit, starts again at zero.

**A tripped switch refuses everything, including a flattening order.** A real
trade-off, written down: closing out becomes a manual act, which is the right
friction for something that has already gone wrong.

### Why M6 lands before M5's run

M6's own criterion is a test-suite one, so it does not need the fortnight. But the
paper run is the first time this system runs unattended with a strategy submitting
orders, and a run that exercises the risk layer for two weeks is worth strictly
more than one that does not. Running first and adding risk after would want a
*second* long run to see the limits behave live.

Same reasoning that made M1's acceptance run worth more because M1.e's metrics
existed before it started.

---

## 11. The seam a live venue needs — M8 slice (c)

A live venue is the first that can be unready, the first that knows things the
strategy must not, and the first whose last fills can arrive after the market
has ended. Each of those needed the seam to change, and each change had to leave
the simulator, and so every backtest, exactly where it was. `docs/live-run.md` §5
records the decisions; this is the shape they produced, in `quant-engine`'s
`lib.rs`, `strategy.rs` and `venue.rs` and `quant-core`'s `report.rs`.

### Asked before anything is written down

`ExecutionVenue::ready(&self, &OrderRequest) -> Result<(), Unready>` is
consulted in `Context::submit` after the seam check and the record's latch, and
before risk. By the time `submit` runs the order is journalled and in the
engine's ledger, so the only way back would be a `Rejected`, which the journal
records as the venue declining an order it had been told about — false for one
that never left. Asked first, the refusal is written as a refusal,
`RefusedBy::Venue(Unready)`, and the strategy hears `RejectReason::Venue` and
not why.

**Required, not defaulted.** A defaulted method is what a forwarding wrapper
inherits without a word (M7.5.e's `take_notes` in `Recorded`), and the
inherited answer here would be "ready" around a venue that is not. The simulator
answers always ready; its filters stay judged on receipt, with a `Rejected`, as
a real venue judges them. `Unready` is closed now because it is serialized
inside `refused` lines, and includes `Diverged`, which only `LiveVenue`
produces: its merge machine latches it on an unattributable fill or a
stranger's trade on our symbol, and only a person clears it.

### A submission the record could not hold is never sent

`RunObserver::on_submitted` answers `WriteAhead::Proceed` or `Withhold`. On
`Withhold` the order is not sent, is refused as `RefusedBy::Record`, and every
later submission in the process is refused the same way before the venue is
asked — a record that failed to hold one line cannot be trusted with the next.
The withheld order has already passed risk and spent one of the day's count;
that can happen once per process. `JournalWriter::new` always answers `Proceed`,
which keeps paper's never-fatal journalling exactly as it was.
`JournalWriter::strict`, the live run's, answers `Withhold` from its first
failed append on, and writes nothing more, because a line appended after a torn
one would leave the journal unreadable.

### Reports and notices

`poll` hands back `VenueReport`s. An `Order(OrderReport)` is an event with what
only a real venue knows typed beside it per variant: the venue's order id on an
acceptance, the order and trade ids on a fill, what the venue cited or why the
request never landed on a rejection, how an order ended. A `Notice` is a fact
with no event at all: a REST acknowledgement, an outcome that became unknown or
known again, a cancel the venue did not carry out, a report about an order this
run did not send, the account stream lost or regained, a report that could not
be read.

The record hears the detail (`on_accepted`, `on_rejected`, `on_cancelled`,
`on_orphaned`, and `on_fill` through one `Booked`) and every notice
(`on_notice`, required). The strategy is told `OrderReport::event()`, which is
where venue detail stops, and nothing of a notice. A notice moves the clock like
any report, so a stream lost during a silent market is dated when it was heard.
`take_notes` runs only on a wake that called the strategy, so counting its calls
cannot reveal a talkative venue.

`poll`'s contract, in the trait's docs: append and never block; arrival order,
stamped by the clock that stamps `local_recv_ts`; at most one terminal report
per order and none before its last fill; `Order` only for this run's orders;
never drop a report silently.

### Only an order the engine is working reaches the strategy

A live venue reports on the account, not on the process. A report about an order
the engine is not working — never minted, or already ended — is withheld: a
fill goes to `on_orphaned` (counted, not booked), anything else to `on_stray`
(required), and the strategy is told nothing. `EngineStats::withheld` counts
them. Against the simulator it is always zero, by construction: an order enters
the ledger before the simulator is told of it, and the simulator reports only
on what it was given and nothing after an order's end.

### Settle

`Engine::settle()` is one wake on which only the venue speaks, after `run`.
Whatever arrives is booked, written down and tallied by risk, and no strategy
method is called: the market the strategy trades on has ended, a backtest never
delivers a report still in flight at the end of its data, and a strategy acting
during the drain would leave decisions no replay has. It returns what it
delivered and the orders still working (`Engine::working`). The live binary
loops on it with a deadline of its own, pausing `IDLE_TICK` when nothing came,
before the closing checkpoint and `Stopped`. Neither `backtest` nor `paper`
calls it, and against the simulator it changes nothing: it polls at a clock
nothing has moved, so a report still in flight stays undelivered.

### Cancel what is working

`Engine::cancel_working()` is how a live stop asks for its resting orders to
go, before it settles (the owner's decision 3, `docs/live-run.md` §5,
*LiveVenue, decided before the code*, §7). For each order the engine is
working, lowest id first, it calls `on_cancel_requested` and then the venue's
`cancel`, both at the engine's clock, and it calls no strategy method. It is
the engine's own because nothing else could do it: only `Context::cancel`
reaches the venue, and only from a strategy callback, which a stop must not
make, and a cancel sent around the engine would leave a `cancelled` line with
no request behind it. It asks; it does not end anything. The venue's report
ends an order, and `settle` hears it. Not a trait method, so no venue had to
change; `backtest` and `paper` never call it. Its test asserts one
`cancel_requested` line per working order, each before the venue hears the
cancel, and no strategy call, and was seen red with the venue told first and
with the record skipped; a second asserts that a run ending flat asks the
venue nothing.

### Acceptance

- [x] The simulator reproduces every existing test, and M5's money check —
      824 fills, cash 31.64743842 — after every commit that touched the path,
      with the whole report and a backtest journal byte-identical apart from its
      wall-clock `started` line.
- [x] A strategy's view of a lifecycle is identical whichever venue produced it:
      the same acceptance, fills, declined order, lost cancel and cancellation,
      through a venue with nothing beside its events and one that says
      everything a live venue says, give the strategy the same events, clock,
      position, cash and `take_notes` count, and only the second's record heard
      the ids and notices. The clock is the same only because the talkative
      venue's notices are stamped no later than the order reports beside them
      on the same wake. A notice moves the clock like any report (§6): one
      stamped later than everything else on its wake carries the clock, and
      what the strategy reads on that wake, forward to its own time, and one on
      a wake of its own moves the clock with no strategy call at all. That is
      by design, and it is how a stream lost during a silent market is dated
      when it was heard.

      *Between the simulator and `LiveVenue` as well, since
      `m8c-sim-declines-unannounced`.* The test drove two venues of one shape,
      and until then `SimulatedVenue` announced an order before judging its
      filters and its market, so a declined order reached a strategy as an
      acceptance and a rejection, where a live refusal is a rejection alone.
      It now announces only an order it keeps, and a test pins that both ways
      (`docs/live-run.md` §5, *LiveVenue, decided before the code*, §6).
- [x] Readiness refuses before anything is written and spends nothing of risk's
      count; a record that withholds once withholds for good; strays and orphans
      never reach the strategy; `settle` books a late fill and tells nobody.
      Every one of those tests was seen red under its break before it was
      trusted.

### What the record writes: journal schema 3

The seam's slice left the new facts without lines of their own, logging each as
an error; `m8c-journal-live` gave them lines (`docs/run-log.md` §8). Detail goes
on the line of the event it describes: the venue's ids on `accepted`, `filled`,
`cancelled` and `orphaned`, what the venue cited on `rejected`, the orders still
working on `stopped`. Each `Notice` maps to an entry of its own through one
exhaustive `match` in the writer, a request that never landed is
`never_landed` rather than `rejected`, and a stray is `stray`.

`RunObserver::on_sighted(at)` is the fourteenth hook, **required** for
`on_notice`'s reason. The engine calls it in step 2 when `Book::apply` takes a
book from not live to live, and the journal writes it as `sighted`: the
market's return, written down, which `explain` reads in place of inferring it
from any later line once a session is schema 3. Only `LiveVenue`
(`quant-live`) produces the venue's lines, so for now they are written only by
its end-to-end tests; `sighted` is in every new backtest and paper
journal, one each time a book goes from not live to live: after every gap,
and also when a book re-anchors after its own update chain broke, which writes
no `blind` line. The readers' pull request, `m8c-journal-readers`, recounts
the risk tally after a restart (§10), and reads the new lines: an `orphaned`,
`stray` or other-run `foreign` line and a doubt left raised are findings in
`runlog check`, and a line torn before a restart is said on the verdict line.

