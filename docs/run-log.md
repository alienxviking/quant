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
| f | `runlog check`, `runlog diff`, and the sabotages | the three criteria |
| g | The debt with a scheduled repayment | emitter to `.json()`, delete `health.rs` |

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
Past that an fsync inside `step` stalls the engine thread, the tee drops the
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
