package shared

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidDefinition   = errors.New("invalid definition")
	ErrAlreadyExists       = errors.New("already exists")
	ErrNotFound            = errors.New("not found")
	ErrTypeMismatch        = errors.New("type mismatch")
	ErrConstraintViolation = errors.New("constraint violation")
)

func NewError(kind error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", kind, fmt.Sprintf(format, args...))
}
