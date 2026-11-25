# API authorization matrix

This is the complete product API authorization inventory. The product API table is mechanically checked against registered handlers by `TestEveryRegisteredAPIHasAuthorizationClassification`; unmatched authenticated actions map to `permission.unclassified` and are denied. Runtime Host owns the separate update/plugin/storage/setup endpoints listed below. `HEAD` follows the corresponding `GET` rule.

## Public endpoints

| Method | Path | Access |
|---|---|---|
| `GET`, `HEAD` | `/healthz`, `/api/auth/session`, `/api/setup`, `/api/setup/status` | Public, bounded status only |
| `GET` | `/api/global-identity/session` | Public session status only |
| `POST` | `/api/global-identity/login/options`, `/api/global-identity/login/finish`, `/api/global-identity/logout` | Global Identity Passkey ceremony routes; ceremony cookie/CSRF checks apply |
| `POST` | `/api/auth/login`, `/api/auth/bootstrap` | Public credential operation; bootstrap is origin-checked, bounded, and one-time |
| `GET`, `HEAD` | allowlisted SPA routes and `/static/*` | Public web assets |

## Control Plane product API

| Method | Path | Required permission |
|---|---|---|
| `POST` | `/api/auth/logout` | `account.sessions.self` |
| `GET` | `/api/user` | `account.profile.self` |
| `PATCH` | `/api/user` | `account.profile.self` |
| `GET` | `/api/user/authorization` | `account.profile.self` |
| `POST` | `/api/user/password` | `account.password.self` |
| `GET` | `/api/user/preferences` | `user.preferences.self` |
| `PUT` | `/api/user/preferences` | `user.preferences.self` |
| `GET` | `/api/user/sessions` | `account.sessions.self` |
| `DELETE` | `/api/user/sessions/{sessionID}` | `account.sessions.self` |
| `POST` | `/api/user/sessions/revoke-others` | `account.sessions.self` |
| `GET` | `/api/users` | `user.read` |
| `POST` | `/api/users` | `user.create` |
| `PATCH` | `/api/users/{userID}/roles` | `user.manage` |
| `POST` | `/api/users/{userID}/disable` | `user.manage` |
| `POST` | `/api/users/{userID}/enable` | `user.manage` |
| `DELETE` | `/api/users/{userID}` | `user.manage` |
| `GET` | `/api/roles` | `role.read` |
| `POST` | `/api/roles` | `role.manage` |
| `PUT` | `/api/roles/{roleID}` | `role.manage` |
| `PATCH` | `/api/roles/{roleID}` | `role.manage` |
| `DELETE` | `/api/roles/{roleID}` | `role.manage` |
| `GET` | `/api/global-identity/status`, `/api/global-identity/history` | `system.read` |
| `POST` | `/api/global-identity/history/import` | `system.settings.manage` |
| `GET` | `/api/global-identity/registry` | `system.read` |
| `POST` | `/api/global-identity/registry/import` | `system.settings.manage` |
| `POST` | `/api/global-identity/nodes/delegate`, `/api/global-identity/nodes/{nodeID}/revoke` | `system.settings.manage` |
| `GET` | `/api/node-identity/status` | `system.read` |
| `POST` | `/api/node-identity/create`, `/api/node-identity/enrollment/challenge`, `/api/node-identity/advertisement/sign` | `system.settings.manage` |
| `POST` | `/api/global-identity/registration/options`, `/api/global-identity/registration/finish`, `/api/global-identity/credentials/{credentialID}/revoke` | `system.settings.manage` |
| `GET` | `/api/owner-authority/subjects`, `/api/owner-authority/subjects/{subjectID}`, `/api/owner-authority/subjects/{subjectID}/events` | `system.read` |
| `POST` | `/api/owner-authority/events` | `system.settings.manage` and CSRF; signed event is independently verified against the current identity history |
| `GET` | `/api/peer-sessions` | `recording.read` |
| `POST`, `DELETE` | `/api/peer-sessions`, `/api/peer-sessions/{nodeID}` | `system.settings.manage` and CSRF |
| `GET` | `/api/replica-leases` | `recording.read`; returns a state summary without signatures or grant chains |
| `POST` | `/api/replica-tombstones` | `recording.delete`; HTTPS, OwnerSubject `archive.delete` and Node `archive.replicate` capability; persists a tombstone and retries encrypted replica cleanup plus signed provider reports; leaves the canonical source archive untouched, and reports do not prove secure overwrite |
| `POST` | `/api/replica-imports` | `recording.export`; requires HTTPS and revalidates the current OwnerSubject capability, exact peer session, and durable import lease |
| `GET` | `/api/peer-archives/capabilities`, `/api/peer-archives/{nodeID}/head`, `/api/peer-archives/{nodeID}/snapshots/{snapshotID}/manifest`, `/api/peer-archives/{nodeID}/snapshots/{snapshotID}/key-envelope`, `/api/peer-archives/{nodeID}/snapshots/{snapshotID}/objects/{objectID}` | `recording.read`; peer requests separately revalidate Node Key and OwnerSubject capability |
| `POST` | `/api/setup/storage-test` | `system.settings.manage` |
| `POST` | `/api/recordings` | `recording.create` |
| `GET` | `/api/recordings` | `recording.read` |
| `GET` | `/api/recordings/{id}` | `recording.read` |
| `DELETE` | `/api/recordings/{id}` | `recording.delete` |
| `GET` | `/api/recordings/{id}/diagnostics/storage` | `recording.read` |
| `POST` | `/api/recordings/{id}/stop` | `recording.stop` |
| `POST` | `/api/recordings/{id}/complete` | `recording.stop` |
| `POST` | `/api/recordings/{id}/seal` | `recording.stop` |
| `GET` | `/api/recordings/{id}/lifecycle` | `recording.read` |
| `GET` | `/api/recordings/{id}/acquisition-flow` | `recording.read` |
| `GET` | `/api/recordings/{id}/play/master.m3u8` | `recording.read` |
| `GET` | `/api/recordings/{id}/play/tracks/{track}/playlist.m3u8` | `recording.read` |
| `GET` | `/api/recordings/{id}/play/live/master.m3u8` | `recording.read` |
| `GET` | `/api/recordings/{id}/play/live/tracks/{track}/playlist.m3u8` | `recording.read` |
| `GET` | `/api/recordings/{id}/play/segments/{segmentID}` | `recording.read` |
| `GET` | `/api/recordings/{id}/tags` | `recording.read` |
| `PUT` | `/api/recordings/{id}/tags` | `recording.annotate` |
| `GET` | `/api/recordings/{id}/archive/index` | `recording.read` |
| `GET` | `/api/recordings/{id}/archive/snapshot` | `recording.read` |
| `GET` | `/api/recordings/{id}/integrity` | `recording.read` |
| `POST` | `/api/recordings/{id}/integrity/verify` | `recording.integrity.run` |
| `GET` | `/api/integrity/jobs/{job_id}` | `recording.read` |
| `POST` | `/api/integrity/jobs/{job_id}/cancel` | `recording.integrity.run` |
| `POST` | `/api/recordings/{id}/exports` | `recording.export` |
| `GET` | `/api/recordings/{id}/exports` | `recording.read` |
| `GET` | `/api/exports/{job_id}` | `recording.read` |
| `GET` | `/api/exports/{job_id}/download` | `recording.export` |
| `DELETE` | `/api/exports/{job_id}` | `recording.export` |
| `GET` | `/api/recordings/{id}/thumbnail` | `recording.read` |
| `POST` | `/api/recordings/{id}/thumbnail/regenerate` | `recording.preview.manage` |
| `GET` | `/api/recordings/{id}/previews` | `recording.read` |
| `GET` | `/api/recordings/{id}/previews/{ordinal}` | `recording.read` |
| `POST` | `/api/recordings/{id}/previews` | `recording.preview.manage` |
| `GET` | `/api/v2/recordings` | `recording.read` |
| `GET` | `/api/dashboard` | `recording.read` |
| `GET` | `/api/search` | `recording.read` |
| `GET` | `/api/system/storage` | `storage.read` |
| `GET` | `/api/storage/pools` | `storage.read` |
| `GET` | `/api/storage/pools/{pool_id}/metrics` | `storage.read` |
| `GET` | `/api/system/info` | `system.read` |
| `GET` | `/api/resolve-workflows` | `recording.workflow.manage` |
| `GET` | `/api/resolve-workflows/{id}` | `recording.workflow.manage` |
| `DELETE` | `/api/resolve-workflows/{id}` | `recording.workflow.manage` |
| `POST` | `/api/resolve-workflows/{id}/continue` | `recording.workflow.manage` |
| `GET` | `/api/workflow-history` | `recording.workflow.manage` |
| `GET` | `/api/workflow-history/{id}` | `recording.workflow.manage` |
| `GET` | `/api/adapters` | `recording.create` |
| `GET` | `/api/adapters/{id}` | `recording.create` |
| `GET` | `/api/adapters/{id}/icon` | `plugin.read` |
| `GET` | `/api/adapters/{id}/schema` | `recording.create` |
| `GET` | `/api/adapters/{id}/config` | `plugin.configure` |
| `PUT` | `/api/adapters/{id}/config` | `plugin.configure` |
| `GET` | `/api/adapters/{id}/resources` | `recording.create` |
| `GET` | `/api/adapters/{id}/resources/search` | `recording.create` |
| `POST` | `/api/adapters/{id}/restart` | `plugin.manage` |
| `POST` | `/api/adapters/{id}/enable` | `plugin.manage` |
| `POST` | `/api/adapters/{id}/disable` | `plugin.manage` |
| `POST` | `/api/watches` | `watch.manage` |
| `GET` | `/api/watches` | `watch.read` |
| `GET` | `/api/watches/{id}` | `watch.read` |
| `PUT` | `/api/watches/{id}` | `watch.manage` |
| `DELETE` | `/api/watches/{id}` | `watch.manage` |
| `POST` | `/api/watches/{id}/enable` | `watch.manage` |
| `POST` | `/api/watches/{id}/disable` | `watch.manage` |
| `POST` | `/api/watches/{id}/check` | `watch.check` |
| `GET` | `/api/watches/{id}/recordings` | `watch.read` |
| `GET` | `/api/watches/{id}/events` | `watch.read` |
| `GET` | `/api/recordings/{id}/events` | `recording.read` |
| `GET` | `/api/recordings/{id}/timed-events` | `recording.read` |
| `GET` | `/api/recordings/{id}/timed-events/export` | `recording.export` |
| `GET` | `/api/recordings/{id}/metadata` | `recording.read` |
| `GET` | `/api/audit` | `audit.read` |
| `GET` | `/api/notifications` | `system.diagnostics.read` |
| `POST` | `/api/notifications/sync` | `system.diagnostics.read` |
| `POST` | `/api/notifications/{id}/read` | `system.diagnostics.read` |
| `POST` | `/api/notifications/read-all` | `system.diagnostics.read` |
| `GET` | `/api/logs` | `system.diagnostics.read` |
| `GET` | `/api/settings` | `system.read` |
| `PUT` | `/api/settings` | `system.settings.manage` |
| `GET` | `/api/retention/candidates` | `recording.read` |
| `POST` | `/api/retention/run` | `recording.delete` |
| `GET` | `/api/runtime/update` | `system.update.manage` |
| `POST` | `/api/runtime/update/check` | `system.update.manage` |
| `POST` | `/api/runtime/update/stage` | `system.update.manage` |
| `POST` | `/api/runtime/update/activate` | `system.update.manage` |
| `POST` | `/api/runtime/update/rollback` | `system.update.manage` |
| `POST` | `/api/runtime/adapters/reconcile` | `plugin.manage` |

## Runtime Host API

| Method | Path | Required permission |
|---|---|---|
| `GET` | `/api/runtime/update` | `system.update.manage` |
| `POST` | `/api/runtime/update/check` | `system.update.manage` |
| `POST` | `/api/runtime/update/stage` | `system.update.manage` |
| `POST` | `/api/runtime/update/activate` | `system.update.manage` |
| `POST` | `/api/runtime/update/rollback` | `system.update.manage` |
| `POST` | `/api/runtime/adapters/reconcile` | `plugin.manage` |
| `GET` | `/api/runtime/plugins` | `plugin.read` |
| `POST` | `/api/runtime/plugins/refresh` | `plugin.manage` |
| `POST` | `/api/runtime/plugins/{pluginID}/install` | `plugin.manage` |
| `POST` | `/api/runtime/plugins/{pluginID}/update` | `plugin.manage` |
| `DELETE` | `/api/runtime/plugins/{pluginID}` | `plugin.manage` |
| `GET` | `/api/runtime/storage/provider` | `storage.read` |
| `GET` | `/api/runtime/storage/providers/{providerID}/config` | `storage.read` |
| `PUT` | `/api/runtime/storage/providers/{providerID}/config` | `storage.manage` |
| `POST` | `/api/runtime/storage/providers/{providerID}/probe` | `storage.manage` |
| `POST` | `/api/runtime/storage/providers/{providerID}/activate` | `storage.manage` |
| `GET` | `/api/runtime/storage/instances` | `storage.read` |
| `POST` | `/api/runtime/storage/instances` | `storage.manage` |
| `GET` | `/api/runtime/storage/instances/{instanceID}/config` | `storage.read` |
| `PUT` | `/api/runtime/storage/instances/{instanceID}/config` | `storage.manage` |
| `POST` | `/api/runtime/storage/instances/{instanceID}/probe` | `storage.manage` |
| `POST` | `/api/runtime/storage/instances/{instanceID}/activate` | `storage.manage` |
| `POST` | `/api/setup/begin` | `system.settings.manage` |
| `POST` | `/api/setup/complete` | `system.settings.manage` |
| `POST` | `/api/setup/storage-test` | `system.settings.manage` |

## Self-scoped and root-protected behavior

- `GET/PATCH /api/user` and `GET /api/user/authorization` are scoped to the authenticated stable `user_id`; `PATCH` changes only that user's login after current-password verification. Password, session, and preference routes are likewise self-scoped.
- Self profile/password/session/preferences permissions are implicit for every active authenticated user. They do not grant user administration.
- Owner-root checks apply to role assignment or user enable/disable/delete operations that affect Owner authority. Only an existing Owner may grant or remove the Owner role, and the final active Owner is protected. Built-in role definitions cannot be edited or deleted.
- Role and user mutation URLs use stable IDs, not mutable login names.
- `GET /api/setup` and `/api/setup/status` are public projections; setup mutations require `system.settings.manage` and CSRF validation. Initial `/api/auth/bootstrap` remains the one-time public claim operation.
- API endpoints not listed above are not granted permission implicitly.
