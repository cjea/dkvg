package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

const PROMPT = "#> "

var (
	ErrNotFound   = errors.New("not foundd")
	ErrBadCommand = errors.New("unrecognized command")
)

type CmdType int

const (
	CmdUnknown CmdType = iota
	CmdQuit
	CmdGet
	CmdSet
)

var kvStore = map[string]string{}

func setVal(store map[string]string, key string, val string) error {
	kvStore[key] = val
	return nil
}

func getKey(store map[string]string, key string) (string, error) {
	val, ok := kvStore[key]
	if !ok {
		return "", fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	return val, nil
}

func Prompt() {
	fmt.Printf(PROMPT)
}

type Cmd struct {
	Type CmdType
	Data interface{}
}

type Pair struct {
	Left  string
	Right string
}

// ParseSet parses a raw string of the form "key=val" into a Cmd.
// It strips all leading and trailing whitespace.
// TODO(cjea): support quoting both keys and vals for string literals.
func ParseSet(raw string) (*Cmd, error) {
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf(
			"malformed set command (must contain one '=': %w",
			ErrBadCommand,
		)
	}

	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	if len(key) == 0 || len(val) == 0 {
		return nil, fmt.Errorf("keys and vals must not be empty: %w", ErrBadCommand)
	}

	return &Cmd{
		Type: CmdSet,
		Data: Pair{key, val},
	}, nil
}

func KvSet(pair Pair) error {
	kvStore[pair.Left] = pair.Right
	return nil
}

// ParseGet parses a raw string as a lookup key.
// It strips all leading and trailing whitespace.
// TODO(cjea): support quoting keys for string literals.
func ParseGet(raw string) (*Cmd, error) {
	key := strings.TrimSpace(raw)
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty: %w", ErrBadCommand)
	}
	return &Cmd{
		Type: CmdGet,
		Data: key,
	}, nil
}

func KvGet(key string) (string, error) {
	val, ok := kvStore[key]
	if !ok {
		return "", ErrNotFound
	}
	return val, nil
}

func Dispatch(cmd *Cmd) error {
	switch cmd.Type {
	case CmdQuit:
		fmt.Fprintf(os.Stderr, "Goodbye\n")
		os.Exit(0)
	case CmdSet:
		pair, ok := cmd.Data.(Pair)
		if !ok {
			// invariant: system bug
			must(fmt.Errorf("expected a pair, got %#v", cmd.Data))
		}

		KvSet(pair)
		fmt.Printf("OK\n")
	case CmdGet:
		k, ok := cmd.Data.(string)
		if !ok {
			// invariant: system bug
			must(fmt.Errorf("expected a string, got %#v", cmd.Data))
		}

		val, err := KvGet(k)
		if err == nil {
			fmt.Printf("%s\n", val)
		} else if errors.Is(err, ErrNotFound) {
			fmt.Printf("NULL\n")
		} else {
			return err
		}
	default:
		return ErrBadCommand
	}
	return nil
}

func Parse(raw string) (*Cmd, error) {
	raw = strings.TrimSpace(raw)
	if raw == "quit" || raw == "q" {
		return &Cmd{Type: CmdQuit}, nil
	} else if strings.HasPrefix(raw, "set ") {
		return ParseSet(raw[4:])
	} else if strings.HasPrefix(raw, "get ") {
		return ParseGet(raw[4:])
	}

	return nil, fmt.Errorf("'%s': %w", raw, ErrBadCommand)
}

func handleInput(raw string) {
	cmd, err := Parse(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot parse command: %v\n", err)
		return
	}
	if err = Dispatch(cmd); err != nil {
		fmt.Fprintf(os.Stderr, "cannot run command: %v", err)
	}
}

func repl() {
	scanner := bufio.NewScanner(os.Stdin)
	Prompt()
	for scanner.Scan() {
		handleInput(scanner.Text())
		Prompt()
	}
}

func main() {
	fmt.Printf("Hello, world!\n")
	repl()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
