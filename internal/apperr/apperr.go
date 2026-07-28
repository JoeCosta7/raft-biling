package apperr

type CommandError struct {
	Kind    string
	Message string
	Field   string
	Details map[string]interface{}
}

func (e *CommandError) Error() string { return e.Message }

const (
	KindValidation = "validation"
	KindConflict   = "conflict"
	KindNotFound   = "not_found"
	KindStorage    = "storage"
)
