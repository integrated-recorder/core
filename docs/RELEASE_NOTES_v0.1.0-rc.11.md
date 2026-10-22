# Integrated Recorder v0.1.0-rc.11

This is a prerelease. Use the latest installer from the official `main` branch
when installing or upgrading.

## Upgrade and storage reliability

- Fixes application-generation migration when a Docker image changes. A new
  image-bundled application gets its own exact version/commit identity; an
  existing signed installed release continues to require its normal signature,
  digest, and compatibility verification.
- Preserves adapter and Storage Provider set pins while migrating the active
  application generation.
- Makes interrupted installer upgrades recover safely: failures before durable
  activation can restore the previous Compose deployment, while an activated
  candidate remains pending for forward retry instead of starting an old Host
  over newer durable state.
- Improves bounded Storage Provider startup diagnostics so operators can
  distinguish validation and process-start stages without exposing raw private
  paths or credentials.
- Adds isolated Linux Docker regression coverage for persisted application
  state, provider pins, archive objects, retries, and container recreation.

## Known limitation

The underlying cause of the previously reported production `RC9 → RC10`
`bundled storage.local artifact failed validation` incident remains
unconfirmed. The diagnostic improvements and generation migration fix do not
claim that this separate historical failure was reproduced or resolved.

The official RC9-to-RC11 Linux Docker acceptance is run after publication and
is reported separately from the release build workflow.
