# Password AutoFill acceptance

Credential forms use native HTML username and password controls and read submitted values from the form's actual DOM controls. Browser suggestions depend on the password manager, saved credentials, origin, and deployment security settings. Verify on a trusted HTTPS origin; a missing suggestion on plain HTTP alone does not indicate a form defect.

The first account chooses its own login identifier and receives the Owner role. The login is not forced to `owner`. Setup resume accepts the same editable username/password pair. Account rename is a separate authenticated operation and requires the current password.

## iOS Safari manual acceptance

### Existing credential

1. Open `/login` in iOS Safari.
2. Focus either credential field and check that Password AutoFill offers the saved site credential.
3. Select it and confirm that both username and password populate.
4. Submit without retyping the password and confirm authentication succeeds.

### New installation

1. Open administrator setup and enter the login identifier you want to use.
2. If Safari offers a generated password, select it and confirm it also fills the confirmation field.
3. Complete setup, sign out, and reopen `/login`.
4. Confirm Safari offers the saved username/password pair and sign-in succeeds.

### Setup resume

1. Open the resume screen for an installation whose setup is incomplete.
2. Confirm Safari offers the saved username and current password.
3. Sign in and confirm setup resumes.

### Password change and login rename

1. Open account security while signed in.
2. Confirm the current login is visible as read-only context before password fields.
3. Change the login in its separate form using the current password; sign in again with the new login.
4. Change the password using the current password and the new-password fields; sign in again with the new password.

The application does not merge credentials across origins. `localhost`, LAN IP addresses, Tailscale names, and reverse-proxy DNS names can be distinct credential origins.

## Automated Safari/WebKit code-and-engine acceptance

The focused Playwright WebKit suite checks credential markup, required controls, native DOM-value-to-`FormData` submission, UTF-8 password policy behavior, setup and resume, account rename, password change, and secondary-user creation. This is Safari/WebKit code-and-engine acceptance. It is not equivalent to testing iCloud Passwords or QuickType on physical iPhone hardware; real-device AutoFill remains a separate manual check.

## English quick check

The first administrator chooses a login. Login, setup, setup resume, account rename, password change, and user creation use standard username/password autocomplete values and submit the values present in the native controls. Confirm AutoFill on a trusted HTTPS origin. Automated Playwright WebKit coverage validates the form contract but does not simulate physical iOS Passwords/QuickType UI.
