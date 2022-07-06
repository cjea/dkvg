package parse

import (
	"dkvg/pkg/model"
	"fmt"
	"strings"
)

func ParseSync(raw string) (*model.Cmd, error) {
	if raw != "sync" {
		return nil, fmt.Errorf("sync does not take args in '%s': %w", raw, model.ErrBadCommand)
	}
	return &model.Cmd{
		Type: model.CmdSync,
		Data: "",
	}, nil
}

// ParseGetRaw parses a raw string as a lookup key.
// It strips all leading and trailing whitespace.
// TODO(cjea): support quoting keys for string literals.
func ParseGetRaw(raw string) (*model.Cmd, error) {
	key := strings.TrimSpace(raw)
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty: %w", model.ErrBadCommand)
	}
	return &model.Cmd{
		Type: model.CmdGet,
		Data: key,
	}, nil
}

// ParseSetRaw parses a raw string of the form "key=val" into a model.Cmd.
// It strips all leading and trailing whitespace.
// TODO(cjea): support quoting both keys and vals for string literals.
func ParseSetRaw(raw string) (*model.Cmd, error) {
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf(
			"malformed set command (must contain one '='): %w",
			model.ErrBadCommand,
		)
	}

	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	if len(key) == 0 || len(val) == 0 {
		return nil, fmt.Errorf("keys and vals must not be empty: %w", model.ErrBadCommand)
	}

	return &model.Cmd{
		Type: model.CmdSet,
		Data: model.Pair{Left: key, Right: val},
	}, nil
}


func Parse(raw string) (*model.Cmd, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, model.PrefixSet) {
		return ParseSetRaw(strings.TrimPrefix(raw, model.PrefixSet))
	} else if strings.HasPrefix(raw, model.PrefixGet) {
		return ParseGetRaw(strings.TrimPrefix(raw, model.PrefixGet))
	} else if strings.HasPrefix(raw, model.PrefixSync) {
		return ParseSync(raw)
	}


	return nil, fmt.Errorf("'%s': %w", raw, model.ErrBadCommand)
}
