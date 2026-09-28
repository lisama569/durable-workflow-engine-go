package workflow

import "errors"

// Definition errors.
var (
	ErrInvalidDefinition = errors.New("workflow: invalid definition")
	ErrDefinitionChanged = errors.New("workflow: definition fingerprint does not match historical run")
)

// Run lifecycle errors.
var (
	ErrRunNotFound      = errors.New("workflow: run not found")
	ErrRunAlreadyExists = errors.New("workflow: run already exists")
	ErrRunTerminal      = errors.New("workflow: run already in a terminal state")
)

// ErrIncompatible is reported when the stored state cannot be interpreted.
var ErrIncompatible = errors.New("workflow: incompatible persisted state")
