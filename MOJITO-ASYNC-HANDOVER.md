
---

## UPDATE 2026-08-28 (resumed session) — A1 deployed, A2 wave running

### A1 review + deploy (DONE, all merged to main)
- 4-expert adversarial review over the A1 wave (architect / systems / safety / API).
  Consolidated consensus in '/Users/rom/.omp/agent/sessions/-workspace/2026-08-28T08-38-53-319Z_01a04785-9a87-7622-acdd-26186ec775af/local/a1-consensus.md'; per-PR fold comments posted.
- Folds implemented by 4 parallel agents (fiber, continuation, stackpool+cancel,
  affinity+seam) + a SUBSTRATE agent resolving the review's biggest finding:
  issue #101 "A2.0 substrate resume-table M:N rework" — return_to moved INTO
  ms_ctx_t (v2 168B -> v3 176B), `_ms_resume_tab` + `_ms_last_*` globals deleted,
  switch path thread-safe, 32-fiber cap gone, O(64) scans gone. TDD probes:
  40-fiber concurrency + 2-worker multithread (RED on frozen, GREEN after).
- Notable fix from the continuation fold: b2 keeps struct FIELDS register-resident,
  so cross-switch flags moved into the fiber's heap-block tail (opaque pointers).
- Continuation layer now honest: pre-switch ledger + carrier-reconciled states +
  FiberMotion conformance on Fiber (finished()) + real-carrier AOT driver t28.
- Merged via the stacked batch: PRs #102(+substrate), #97(fiber), #96(continuation),
  #98(stackpool), #95(cancel), #99(affinity), #100(seam) into a1/full; then
  PR #103 a1/full -> main. Issues #41 #49 #50 #51 #52 #53 #101 closed; EPIC #1 closed.
- Final weave: Fiber Movable, _block/_completed guards, owner/ADR-007 fences,
  gen-expected wake consumption, FiberFrame.parked + seam DriveVerdict (Parked|
  Completed), comptime toggle gates, t16 OOB fix (CELL_INTS=6), t17/t24 flipped to
  the post-#101 no-cap contract. 30 drivers + spike harness: ALL GREEN on main.

### A2 wave RUNNING (this session)
- Base a2/full off main (1ccc96c). Burst 1 = 4 agents: #67 pool (A2Pool),
  #68 queues (A2Queue), #69 injection (A2Inject), #70 steal (A2Steal).
- Burst 2 (after burst-1 merge): #71 two-phase/affinity, #72 idle sleep,
  #73 fairness, #74 bench. ONE combined 8-PR 4-expert review, folds, then merge.
