package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const OutputFile = "store.json"

const (
	PROMPT   = "#> "
	SetOK    = "OK"
	EmptyVal = "NULL"
)

const (
	PrefixSet = "set "
	PrefixGet = "get "
)

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

func Persist(store map[string]string) error {
	var err error
	serialized, err := json.Marshal(store)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(OutputFile, serialized, 0644)
	return err
}

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
	var err error
	kvStore[pair.Left] = pair.Right
	err = Persist(kvStore)
	return err
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

func DispatchSet(cmd *Cmd) error {
	pair, ok := cmd.Data.(Pair)
	if !ok {
		// invariant: system bug
		must(fmt.Errorf("expected a pair, got %#v", cmd.Data))
	}

	return KvSet(pair)
}

func DispatchGet(cmd *Cmd) (string, error) {
	k, ok := cmd.Data.(string)
	if !ok {
		// invariant: system bug
		must(fmt.Errorf("expected a string, got %#v", cmd.Data))
	}

	return KvGet(k)
}

func Dispatch(cmd *Cmd) error {
	var err error
	switch cmd.Type {
	case CmdQuit:
		fmt.Fprintf(os.Stderr, "Goodbye\n")
		os.Exit(0)
	case CmdSet:
		if err = DispatchSet(cmd); err != nil {
			return err
		}
		fmt.Printf("%s\n", SetOK)
	case CmdGet:
		val, err := DispatchGet(cmd)
		if err == nil {
			fmt.Printf("%s\n", val)
		} else if errors.Is(err, ErrNotFound) {
			fmt.Printf("%s\n", EmptyVal)
		} else {
			return err
		}
	default:
		return ErrBadCommand
	}

	return nil
}

func ParseRaw(raw string) (*Cmd, error) {
	raw = strings.TrimSpace(raw)
	if raw == "quit" || raw == "q" {
		return &Cmd{Type: CmdQuit}, nil
	} else if strings.HasPrefix(raw, PrefixSet) {
		return ParseSet(strings.TrimPrefix(raw, PrefixSet))
	} else if strings.HasPrefix(raw, PrefixGet) {
		return ParseGet(strings.TrimPrefix(raw, PrefixGet))
	}

	return nil, fmt.Errorf("'%s': %w", raw, ErrBadCommand)
}

func HandleRawInput(raw string) {
	var err error
	cmd, err := ParseRaw(raw)
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
		HandleRawInput(scanner.Text())
		Prompt()
	}
}

// InitKvStore looks for a file at OutputPath, and creates one if none exists.
func InitKvStore() {
	var err error
	_, err = os.Stat(OutputFile)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Initializing store at %s\n", OutputFile)
		emptyJSON := []byte{'{', '}'}
		must(ioutil.WriteFile(OutputFile, emptyJSON, 0644))
	}
	fmt.Printf("Loading store from %s\n", OutputFile)
	f, err := os.Open(OutputFile)
	must(err)
	data, err := ioutil.ReadAll(f)
	must(err)
	must(json.Unmarshal(data, &kvStore))
}

func main() {
	InitKvStore()
	repl()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
