package errors

type Error struct {
	e   error
	msg string
}

func NewError(msg string) Error {
	return Error{
		msg: msg,
	}
}

func (e Error) Error() string {
	return e.msg
}

var (
	ErrNotFound = NewError("Not found")
)
