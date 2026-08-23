# mojito-async

Colorless, direct-style structured concurrency for Mojo, implemented on the
[`mojito-sys`](https://git.opsite.ca/mojito/mojito-sys) systems substrate.

Ordinary Mojo `def` functions park and resume via stackful one-shot fibers and
an M:N scheduler. No `async def`, no `await`, no duplicated sync/async APIs:

```text
ordinary call -> operation ready?  -> return normally
                  would block      -> park fiber, scheduler runs,
                                      resume same fiber, return value
```

## Status

**Pre-A0.** Implementation has not started. Per
[`docs/mojito-async_IMPLEMENTATION_SPEC.md`](docs/mojito-async_IMPLEMENTATION_SPEC.md):

- Hard prerequisite: `mojito-sys` passes its S0 handoff gate.
- First phase here will be **A0: Colorless Parking and Structured-Task
  Feasibility** (one-worker direct-style runtime; park/run/resume with correct
  join, cancellation, error, and scope behavior).
