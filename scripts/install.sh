#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if ! command -v python3 >/dev/null 2>&1; then
	printf '%s\n' 'Integrated Recorder installer requires Python 3.9 or newer.' >&2
	exit 2
fi
if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 9) else 1)'; then
	printf '%s\n' 'Integrated Recorder installer requires Python 3.9 or newer.' >&2
	exit 2
fi

exec python3 "$script_dir/install.py" "$@"
