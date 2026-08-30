# mojito-async

Colorless, direct-style structured concurrency for Mojo, implemented on the
[`mojito-sys`](https://github.com/mojito-async/mojito-sys) systems substrate.

Ordinary Mojo `def` functions park and resume via stackful one-shot fibers and
an M:N scheduler. No `async def`, no `await`, no duplicated sync/async APIs:

```text
ordinary call -> operation ready?  -> return normally
                  would block      -> park fiber, scheduler runs,
                                      resume same fiber, return value
```

## Status

**A1–A6 complete.** 115+ PRs merged; the M:N scheduler, fiber park/resume,
structured scopes, sync primitives, channels, reactor-backed I/O, and the
timer lane are live. Per
[`docs/mojito-async_IMPLEMENTATION_SPEC.md`](docs/mojito-async_IMPLEMENTATION_SPEC.md):

- `mojito-sys` S0 handoff gate passed.
- The runtime is exercised by the full precommit gate suite (`precommit/gate.sh`).
