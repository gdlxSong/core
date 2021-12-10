package placement

import "errors"

var (
	ErrMissingAddress = errors.New("missing address")
)

type Placement interface {
	Register()
}
