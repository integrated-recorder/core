# RC12 sharded archive compatibility fixture

This directory is a persisted format-v2 recording tree produced by the RC12
(`v0.1.0-rc.12`, peeled commit `d4a14d4fab908a0cb415dacd47e7f0d9bc63be8d`)
source's own `storage` and `acquire` write paths. It is a deterministic,
synthetic media fixture, not a recording copied from a user or a live service.
It includes the RC12 recording root, acquisition-context sidecar, init/media
payload objects, canonical index pages, manifest snapshot and metadata page.
The source tag predates global ArchiveID metadata, so readers must use the
stable legacy identity fallback without rewriting the original recording.

Tests copy this fixture before opening it and compare the persisted tree before
and after the current-code migration / projection path; they must not rewrite
or normalize this source fixture.
