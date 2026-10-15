# M7.5 — The run log

What the engine decided, written down where it is already dispatched. This is the
scoping call, the acceptance criteria and the slices, written before the code for
the reason `docs/data-contract.md` preceded the recorder and `docs/engine-contract.md`
preceded the engine.

---

## 1. The scoping call

**The run log is the journal, widened from money to decisions — written ahead of
anything that can act on them, plus a check that the record can detect its own
incompleteness.** It is not a new artifact.

Two re-scopes, both of the M1.d and M2 kind: the inherited wording did not
survive contact with the code, and the reasons are worth keeping.

### Re-scope 1 — there is no second file

"The run log" names a new artifact. It is not one. `JournalEntry` already holds
five variants, and four of the five engine-side losses — seam refusal, risk
refusal, venue acceptance, cancellation — are *already* `ExecutionEvent` values:
constructed, counted in `EngineStats`, and dropped at the end of the loop body.

The fortnight makes the gap exact. **1,648 execution events reached the engine;
824 were journalled.** Exactly half of what the engine saw is unrecorded, and
nothing has to be invented for it. It has to be written down where it already
passes through.

**The decisive argument against a second file is ordering, not tidiness.** The
engine clock *is* the event stream, so a `submitted`, its `refused`, and a
`filled` belonging to a different order routinely carry the **same**
`local_recv_ts`, exactly. Split across two files, a reader has to merge on equal
keys, and the only honest tiebreak is the order they were written — which one
appender preserves and two do not. A record whose central question is *what
happened before what* cannot be split without destroying the one property it
exists to have. M2.d paid for this already: `ingest_seq` is the ordering key of
the four-way merge *because* a four-way merge needs one.

Three supporting arguments. The fill is needed in both roles — recovery and
decision stream — so two files means either duplicating it (two things that must
agree forever) or shipping a decision stream that cannot be read on its own. The
torn-tail rule holds for one appender and gives two tails for two, so a crash
could leave them describing different prefixes of one run with nothing to say
which. And "written down before it can have an effect" is a *sequencing*
guarantee; sequencing a submission in file B against its fill in file A would
need an fsync of both.

The separate-durability case is real and is answered by measurement rather than
argument. Measured on this machine, 300 trials against the real 194-byte `filled`
line: **`sync_data` costs 3.7 ms per durable append; flush-only costs 0.008 ms** —
a factor of 450, and not a number to carry over from Linux intuition, because
`File::sync_data` on Apple platforms is `F_FULLFSYNC`. At this milestone's volume
— about 3,500 entries per symbol-fortnight against today's 994 — that is **13
seconds of device flush spread over fourteen days**, a journal growing from 181 KB
to roughly 560 KB, and both symbols together ~1.1 MB against 4.5 GB of raw
capture. **0.02%.** There is nothing here to protect the recovery record from.

The cost, stated rather than hidden: adding variants breaks pre-M7.5 readers,
because `journal::read` treats an unknown tag on a non-last line as corruption
and there is no version field to refuse at. That is M1.d1's container 1→2 problem
without M1.d1's header to refuse in. Nothing runs an old binary now, so the
one-time cost is acceptable — and `schema` goes on `Started` in slice (a) so the
*next* change can say what is actually wrong.

### Re-scope 2 — `runlog diff` is a debugger, not the criterion

M7 sketched it as the sharpening of M5: from "the totals match" to "every
decision matches, and here is the first that did not." That is a real
improvement and it should be built. It must not be the bar, and the reason is
the question M5.c teaches — **before trusting a check, ask what input would make
it fail.**

Ask it. The live side is `LiveSource + SimulatedVenue`; the replay side is
`HistoricalSource + SimulatedVenue` over the Parquet re-derivation. The tee
guarantees both consume identical records with identical `local_recv_ts` and
`ingest_seq`; `normalize --check` guarantees Parquet reproduces raw event for
event; and downstream of the event stream **both sides are the same engine, the
same venue, the same strategy, deterministically.**

So on a clean single-session run from a frozen build, the diff passes *because
nothing in it could have made it fail*. It tests the **source pair** at higher
resolution. It tests nothing about the engine, and a green diff quoted as
evidence that the engine is correct would be M5.c's vacuous identity at a larger
radius.

Its three real failure modes — a tee drop, a restart, and code drift between the
run and the judging — were all checkable before this milestone, by comparing two
log lines, counting `started` entries, and reading the tag. What the diff adds is
**localisation when something does fail**, which is worth a slice and is not an
acceptance criterion.

There is a second trap in it: a diff is only as sharp as the narrowest column
recorded. A run log holding only fills would compare 824 against 824 and exit 0.
**A criterion that a less complete artifact passes just as easily is not
measuring completeness.**

### The sorting rule

One rule decides the contents, and it is M1.d's and M7's applied a third time:
**record the decision, not the input.**

The mid at 04:00 is re-derivable from raw and `explain --at` already prints it. A
*suppression* lives in `MaCrossover`'s memory and is gone at process exit. So
crossings are recorded and the ~19,300 non-crossing samples are not — which is
also what keeps the volume three orders of magnitude below the capture, and what
makes one line per engine event a refutation rather than a tier: 50.2M × 3.7 ms
is **51 hours of fsync**.

---

## 2. What it is not

**Not a second file.** The ordering argument above is the one not to trade, and at
~3,500 entries per symbol-fortnight there is no write-rate problem to separate. A
second file is also a second thing that must agree forever with the first — did
both survive the crash, does this fill have its submission, do they describe the
same prefix — which is the pattern this project has refused four times (no
`PaperVenue`, no venue trait from one implementation, no duplicated ops harness,
no second fill model). It becomes right only if decisions approach the event
rate; the threshold is in §5 with a number on it.

**Not an event stream, and not replayable.** "Record decisions the way market data
is recorded" is half true and the false half matters. A submission is an
*intent*, and the whole of `docs/engine-contract.md` §2 argues that intent and
outcome are different kinds of thing. A strategy's decision *not* to act is not
an event in any sense — nothing happens, there is no object to stream, which is
exactly why no engine-side hook can observe it however it is shaped. And the
record cannot be fed back into anything: the engine needs `MarketEvent`s to move
a book and a decision record holds none. It is **checkable by replay, not
replayable.** The replayable artifact is raw, and keeping it the only one is a
tier rule.

**Not Parquet.** Parquet writes its footer at close, so a `SIGKILL` loses the file
rather than the last line — and the torn-tail rule is the entire reason this
format is JSON Lines. Publishing by rename would also make the live record
invisible until the process ends, which is the opposite of what an operator needs
at 3am. A later derivation to Parquet belongs in `normalize` and is deferred, not
refused.

**Not a strategy-state checkpoint you can resume from.** The record will contain
the fast and slow averages, which makes it look like something a restart could
rebuild from. Reconstructing in-memory state from it would create a second source
of truth for that state — the refused pattern, arriving through the door this
milestone opens. Refused now, in writing.

**Not a widening of what the strategy can see.** `RejectReason` keeps its two
values; which limit bound goes to the file only. `Context` gains nothing that
touches the outside world — the engine *pulls* notes rather than the strategy
pushing them, which is `Recorded<S>`'s own argument arriving where it was going.
A strategy must still be unable to tell which pair it is wired to, and the test
that runs one strategy value against a filling venue and an accept-only one is
re-run with a record attached.

---

## 3. The acceptance criteria

Three properties, in M4's and M7's shape. The diff is one of them and it is not
the first.

### P1 — the record detects its own incompleteness

`Ledger::mint` hands out `ClientOrderId`s densely from 1, and every mint is
followed by exactly one of three outcomes: a seam refusal, a risk refusal, or a
submission. So **a gap in the id sequence is a decision that happened and was not
written down**, and it is detectable from the file alone.

`runlog check` must report zero holes on a healthy run, and must name the missing
id when one is removed. This is the first criterion precisely because the id is
minted whether or not the line is written — the omission leaves evidence.

Its honest limit: it does not cover entries that consume no id (`accepted`,
`cancelled`, `blind`, `note`), which are covered only by the weaker lifecycle and
strategy claims.

### P2 — the questions are answered, as one-liners, against the finished fortnight

The milestone is not done because entries exist; it is done when the questions
that motivated it can be answered. Each of these must be a `jq` one-liner over
the real run:

- why did it not trade between 03:00 and 04:00 — blind, no crossing, suppressed,
  or nothing to do?
- which of the six risk limits bound, and when?
- how many crossings produced no order, and why?

That third one had a known answer the system could not give, and M7.5.e gave it.
ETHUSDT signalled **828 crossings, 4 suppressed, 822 submitted — two produced
neither an order nor a suppression**, because the `if`/`else if` in `MaCrossover`
had no `else`. Legitimate no-ops (crossed up while already long; crossed down
while already flat), counted by nothing.

The arm exists now, and `crossings` is an identity rather than an approximation:
`entries + exits + suppressed + no_ops == crossings`, asserted and printed.
Measured on the finished fortnight it holds at 828 and 824.

Two things that fixing it turned up. Every fixture in `quant-backtest`'s tests
**rose before it fell**, so none of them could reach the arm — the indicator's
first side was always *below* and its first crossing always upward, which is an
entry. Reaching a no-op needs the side established as above at the first sample
where both averages exist, which is never itself a crossing. That is why five
milestones passed without the gap showing.

And with venue filters enforced (M4.c), ETHUSDT's 828 crossings resolve to **413
entries, 0 exits, 3 suppressed and 412 nothing-to-do** — the strategy signalling
exits it can never take, because it never gets a position to exit. The
`min_notional` defect arriving in a second, independent place.

### P3 — the diff localises, and refuses rather than passing vacuously

`runlog diff live.jsonl replay.jsonl` reports the first divergence with both
sides. It must **exit non-zero rather than 0** when there is nothing to compare,
and when the live side holds two `started` entries — because a restart forfeits
the exact match and "nobody disagreed" is not "two answers matched". That
distinction is `reconcile`'s exit 2, applied here.

**Verified capable of failing.** Every check ships with the sabotage that reddens
it, in the same commit: a deleted line, a transposed pair, a changed field, an
empty file. A green test nobody has watched go red is worth nothing, and that is
a convention here rather than an afterthought.

---

## 4. Slices

| | Slice | Content |
|---|---|---|
| a | The fold made total, the id made real, the start dated | exhaustive `match`, real `client_order_id`, `Started.at`, `schema` |
| b | The order lifecycle, write-ahead | `RunObserver`, submitted/accepted/rejected/cancelled/orphaned |
| c | Refusals, and which of the six limits bound | `Refusal { reason, bound }`, coalescing |
| d | Risk's own clock, blindness, and the tee made reachable | `Tripped` at the instant, `Blind`, `secondary_dropped` |
| e | The strategy's own words | `take_notes`, pulled not pushed |
| f | `runlog check`, `runlog diff`, and the sabotages | the three criteria — **met**, §7 |
| g | The debt with a scheduled repayment | **done** — one shared type, not two formats |

Slice (a) is load-bearing out of proportion to its size: making `journal::replay`
an exhaustive `match` with no `_` arm means a future variant **cannot silently
fail to move money**. That is a compile error instead of a paragraph, and every
slice after it depends on the guarantee.

Slice (g) is its own PR, after (f). M7's commitment reads "in the same commit as
the run log"; the run log ships over several commits, so the honest reading is
*when the run log lands*, and the content of the commitment is that the switch
and the deletion must not be separable — there must be no window where the parser
reads a format that changed. If the week runs long, this is what slips, and it
slips by a week rather than by a milestone.

---

## 5. Decided before the code

**It is M7.5, not M8.** Eight sentences across `CLAUDE.md` and the docs say "must
be fixed before M8" meaning live trading with real capital, and README's table
reads M8 as *Live, tiny capital*. Renumbering would silently invert all eight,
against this project's own rule that existing history is not rewritten.

**One line per decision, never per event or per sample.** 50.2M entries is 51
hours of fsync; 20,133 per symbol-fortnight is 2.4 MB and ~74 seconds for
arithmetic a replay re-derives in 30 seconds.

**Write-ahead, generalising the rule the journal already has.** Fills are
journalled before the strategy is told; submissions are journalled before the
wire. At M8 an order at a live venue we never wrote down is an orphan position.

**The volume boundary is a policy, and policies drift.** Everything here is
affordable because decisions arrive at ~0.003/s against a market peak of 694
msg/s. The design holds while decisions stay below roughly **250/s sustained**.
Past that an fsync inside `Engine::dispatch` stalls the engine thread, the tee drops the
engine's copy, and the record of a session damages the session — M1's literal
recorded lesson, *a metric must never be able to take down a capture*. The
consolation is that the failure reports itself: `LiveSource` finds the
`ingest_seq` hole and the engine writes its own `blind` line with the width, so
invariant 3 arrives in a new place.

---

## 6. The risk

**It answers the easy half and the operator finds out on first use.** The real
3am question is often "why did it trade" — and the strategy's column is
permanently weaker evidence than the engine's, because a note payload is opaque
and nothing can contradict it the way a `Checkpoint` contradicts a bad fold. A
strategy that writes nothing produces a record that looks complete. The hourly
claim narrows this — the notes between two claims must fold to the claim's deltas
— but both sides of that fold are written by the same strategy, so it catches a
dropped line and not a wrong belief. Conceded rather than answered.

**The record gets read as money.** A 3,500-line file where 824 lines are fills and
2,600 are decisions is a file someone will fold with the wrong filter. Three
defences rather than naming alone: no decision entry carries `fee`, and `fee` is
what every money fold keys on; no decision entry carries `cash`, `realized` or
`fills`, so `Checkpoint` stays the only claim; and `reconcile` prints the two
counts separately.

**It becomes a thing nobody uses.** The failure mode is thirteen entry types, no
reader, and `explain` still answering four of five blocks. The guard is that P2 is
demonstrated as one-liners against the finished fortnight — the questions get
answered before the milestone is called done — and that slice (f) teaches
`explain --at` to print the decisions around an instant, so the reader that
established this boundary becomes the first consumer of the thing that crosses it.


---

## 7. The criteria, measured

Against the finished fortnight, replayed through `backtest --journal` so there is
a run log over the real window at all. The live journals predate this milestone
and hold only fills and checkpoints, which is why the replay side is the one
measured — and why `runlog diff` against a live journal is a thing to run at M8
rather than now.

**P1 — the record detects its own incompleteness.** `runlog check` on
BTCUSDT: 3,845 lines, 824 decisions, 337 claims, **exit 0**, every id dense from
1 to 824. Removing the `submitted` line for id 400 names it exactly —
*"client_order_id 400 was minted and never written down"*, exit 1.

Its honest limit, demonstrated rather than asserted: deleting a `note` leaves
**no hole**, because a note consumes no id. The claim fold is what catches it —
*"the strategy claimed 1 more 'entry' notes between two claims; the file holds
0"*. Two layers, and the second is weaker on purpose: both sides of that fold
are written by the same strategy, so it catches a dropped line and not a wrong
belief.

**P2 — the questions, as one-liners.**

```bash
# why did it not trade between 20:00 and 21:00 on 2026-09-29?
explain ~/paper --at 2026-09-29T23:00:00Z --symbol BTCUSDT --window 1h
#   decided   BLIND since 2026-09-29T18:35:01Z (Disconnect), with nothing seen since

# which of the six limits bound, and when?
jq -r 'select(.type=="refused") | "\(.at)  \(.bound)  by \(.by)"' run.jsonl
#   1789744915755174000  order_notional  by risk   (5,768 of them under --max-order 50)

# how many crossings produced no order, and why?
jq -r 'select(.type=="note" and .kind=="no_op") | .detail.why' run.jsonl | sort | uniq -c
#   412 crossed down while already flat          (ETHUSDT)
```

The first question is the one that needed more than `jq`, and finding out why is
the useful part. The engine's clock *is* the event stream, so a twelve-hour
outage writes nothing while it lasts — the `blind` line sits at the moment the
stream stopped, eleven hours before the hour being asked about. A window query
finds silence, and silence reads as *quiet*. So `explain --at` carries the last
blindness forward from outside the window and reports whether anything has been
seen since. Same shape as `market_at`'s day rule: ask narrowly, widen only when
the narrow answer is the uninformative one.

**P3 — the diff localises, and refuses rather than passing vacuously.** Two
identical runs: `AGREE: 824 decisions`, exit 0. A genuinely different system
(`--fast 12`): the first differing decision with both sides, exit 1. Nothing to
compare: exit **2**. A live side holding two `started` entries: exit **2**, with
the reason — a restart forfeits the exact match before any comparison begins.

**The diff's first version was vacuous and the fortnight proved it.** It compared
decisions without their timestamps, reasoning that a reader should not assume
what M5's criterion proves. But the ids are dense from 1 and a long/flat
crossover alternates buy and sell at one size, so every line read `submitted #n
Buy 0.001` — run against two genuinely different strategies it compared 786
decisions and found **every one equal**, reporting only that one side had more.
The assumption being avoided was the thing worth testing: the tee gives both
paths identical `local_recv_ts`, so a decision and its replay disagreeing about
*when* is exactly the divergence this tool exists to find. Excluding the field
discarded the only evidence.

All six checks ship with the sabotage that reddens them, and each sabotage
reddens its own tests and nothing else.

---

## 8. Schema 3: what a live run writes (M8 slice (c))

M7.5 widened the journal from money to decisions. A live venue widens it again,
to what only a real venue knows: its own names for orders and trades, what it
cited when it declined, and facts that are not events at all. Built in
`m8c-journal-live`, from the seam `docs/engine-contract.md` §11 describes. The
reasoning for each line is in that section and in this one; `docs/live-run.md`
§5 has the run tag's and the balances'.

**The rule that keeps every old file readable:** every field added to an
existing line is defaulted on read and written only when it holds something.
So the fortnight's schema-1 journals and every schema-2 journal read unchanged,
and a line that carries none of the new fields is byte for byte the line schema 2
wrote. `read` refuses a session begun in a schema newer than its own, naming both
numbers, rather than reporting a readable file as damage at its first unfamiliar
line.

### Fields on lines that already existed

| line | gains | why |
|---|---|---|
| `started` | `run` | the tag every order is sent under; the first line's is the session's |
| `filled` | `remaining`, `venue_order_id`, `trade_id` | one line is one `myTrades` row (criterion A) |
| `accepted` | `venue_order_id` | |
| `rejected` | `cited`, `breach` | the only durable copy of a REST answer; the rule criterion B compares |
| `cancelled` | `venue_order_id`, `ended` | cancelled, expired or self-trade prevention |
| `orphaned` | `venue_order_id`, `trade_id` | the one handle on an unattributable fill |
| `stopped` | `working` | the orders still live at a stop: stopping is not flattening |

`rejected` keeps its meaning: the venue had the request and declined it. A
request that never reached the venue is not a rejection, and has a line of its
own. `filled.remaining` is written on every fill, so it is text that changes in
every new backtest and paper journal; no number does.

### Lines of their own

| line | what it records |
|---|---|
| `placed` | the venue's REST acknowledgement and its order id, which finds the order after a cancel replaced its client id (criterion E) |
| `never_landed` | a request that never became an order the venue had |
| `doubt` | an order's outcome becoming unknown, or known again, explicitly both ways |
| `cancel_refused` | a cancel the venue did not carry out, or whose fate is unknown |
| `foreign` | a report about an order this run did not send; a trade kept whole, commission asset verbatim |
| `account` | the account stream lost or regained |
| `anomaly` | a report that could not be read, or contradicts what came before it |
| `stray` | a report, other than a fill, about an order the engine is not working |
| `sighted` | the market came into view: a book went from meaning nothing to meaning something |
| `balances` | the account's two balances, at start and stop, on the wall clock (criterion A's baseline) |

The writer maps every `Notice` through one exhaustive `match` with no `_` arm, so
a fact a venue learns to report later does not build until it has a line. A
doubt raised, a stranger's trade, the account stream lost, an anomaly and a
request that never landed are logged as errors as well as written, as an orphan
always was.

### What each reader decided

Every site that matches journal entries without a `_` arm had to decide each
new line, and the compiler would not let one be skipped.

| | `replay` | `next_order_id` | `explain`'s instant | evidence the market came back |
|---|---|---|---|---|
| `placed`, `doubt`, `cancel_refused`, `account`, `anomaly`, `stray`, `never_landed` | not money | no id | its stamp | no |
| `foreign` | **its own arm, not folded**: money, and not this run's | no id: text never becomes a `ClientOrderId` | its stamp | no |
| `balances` | **its own arm, not folded**: folding the venue's claim would make criterion A compare the venue with itself | no id | **none**: the wall clock, like `started` | no |
| `sighted` | nothing | no id | its stamp | **yes**, from schema 3 |

No new line moves the id counter: every id this engine minted is already on a
`submitted` or `refused` line, and an id the venue named must never move it.
Two older lines stopped moving it too, for the same reason: `cancel_requested`,
because `Context::cancel` journals whatever id the strategy passes, and
`orphaned`, the fill twin of a `stray`. Either could name an id never minted,
and the next session would mint past it and leave the hole check holes it
could not explain.

**The market's return is read, not inferred, from schema 3.** Before it, a line
the engine wrote after `blind` was taken to mean the market came back, which
holds while a venue cannot speak without the market: true of the simulator, and
so of every journal written before. A live venue reports on idle wakes, so a fill
during an outage, and whatever the strategy decided on it, would read the outage
as over. `explain` now reads the schema of each session off its `started` line:
in a schema-3 session only `sighted` ends a blindness, and an older one is read
exactly as it always was.

### The strict writer

`JournalWriter::new` is lenient, for paper and backtests: a line it cannot write
is counted and logged, and the run goes on. `JournalWriter::strict` is the live
run's. From its first failed append it writes nothing more and answers every
submission `Withhold`, which the engine turns into `RefusedBy::Record` for that
order and every later one. Nothing more written, because a line appended after a
torn one sits in the middle of the file, where `read` refuses the whole journal.

### Checked against the real artifacts

On 2026-10-15, with the schema-3 build, against the fortnight's two journals
(schema 1): `reconcile` AGREES on both (BTCUSDT 824 fills, cash 31.64743842;
ETHUSDT 822 fills, cash 97.65757574), and `explain --check-journal` finds all
166 checkpoints agreeing in each. `runlog check` exits 2, *nothing to check*,
as it always has on them: they predate M7.5 and hold no decisions and no claims.

A `backtest --journal` over the same window writes a schema-3 journal of 3,862
lines: §7's 3,845 and one `sighted` line for each of the 17 `blind` ones. Its
money is the backtest's to the satoshi (824 fills, cash 31.64743842), `reconcile`
AGREES, `runlog check` is COMPLETE over 824 decisions and 337 claims, and all 165
checkpoints agree. The text of a new journal changes (`schema`, `remaining`,
`sighted`); no number does.
