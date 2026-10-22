package storageproto

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
)

// DiagnosticError identifies a storage-provider failure without including
// executable paths, configuration, credentials, or child-process output in
// its message. Wrapped causes remain available to internal errors.Is/As
// callers, while normal Runtime Host logs contain only the stable stage/code.
type DiagnosticError struct {
	stage string
	code  string

	causes []error
}

var diagnosticToken = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// NewDiagnosticError creates a safe, machine-readable diagnostic. Stage and
// code must be fixed internal identifiers; invalid values are replaced with
// "unknown" so callers cannot accidentally put paths or user data in logs.
func NewDiagnosticError(stage, code string, causes ...error) *DiagnosticError {
	if !diagnosticToken.MatchString(stage) {
		stage = "unknown"
	}
	if !diagnosticToken.MatchString(code) {
		code = "unknown"
	}
	copyCauses := make([]error, 0, len(causes))
	for _, cause := range causes {
		if cause != nil {
			copyCauses = append(copyCauses, cause)
		}
	}
	return &DiagnosticError{stage: stage, code: code, causes: copyCauses}
}

// Stage returns the fixed internal stage identifier.
func (e *DiagnosticError) Stage() string {
	if e == nil {
		return "unknown"
	}
	return e.stage
}

// Code returns the fixed failure category.
func (e *DiagnosticError) Code() string {
	if e == nil {
		return "unknown"
	}
	return e.code
}

func (e *DiagnosticError) Error() string {
	if e == nil {
		return "storage provider initialization failed: stage=unknown code=unknown"
	}
	return fmt.Sprintf("storage provider initialization failed: stage=%s code=%s", e.stage, e.code)
}

// Unwrap preserves errors.Is/As checks without exposing the wrapped error text
// through Error().
func (e *DiagnosticError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return append([]error(nil), e.causes...)
}

// DiagnosticCode returns the outermost stable stage/code when one is present.
// Callers should prefer retaining the original error so errors.Is/As remain
// available as well.
func DiagnosticCode(err error) (stage, code string, ok bool) {
	var diagnostic *DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic == nil {
		return "", "", false
	}
	return diagnostic.stage, diagnostic.code, true
}

// SafeErrorCode classifies common failure classes without formatting an
// arbitrary error. It is intended for bounded diagnostic labels only.
func SafeErrorCode(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		return "not_found"
	case errors.Is(err, ErrProtocol):
		return "protocol_invalid"
	default:
		return "operation_failed"
	}
}
