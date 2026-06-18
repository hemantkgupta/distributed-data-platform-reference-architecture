# Learning Log

| Date | Phase | Checkpoint | Concepts | Code touched | Tests | Revision status |
|---|---|---|---|---|---|---|
| 2026-06-17 | Days 1-15: source contracts, CDC envelopes, ownership, fact grain | CP1 - source contract compatibility surface | Data contract, ownership, grain, freshness SLO, schema compatibility | `data-platform-contracts` | 3 contract tests; included in green full build on 2026-06-18 | Revision cards added |
| 2026-06-18 | Days 1-15: source contracts, CDC envelopes, ownership, fact grain | CP2 - CDC envelope and replay boundary | Log-based CDC, source offset, idempotency key, before/after image, delete semantics | `data-platform-cdc` | 6 total tests passing via `./gradlew build --console=plain` | Revision cards added |
