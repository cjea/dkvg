package main

import (
	"bufio"
	"dkvg/pkg/data_pipeline"
	"dkvg/pkg/model"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync"
)

var (
	UseREPL    = false
	OutputFile = "store.json"
	SockName   = "/tmp/dkvg.sock"
)

var kvStore = map[string]interface{}{}
var kvStoreMutex = sync.RWMutex{}
var Store = model.Store{
	Store: kvStore,
	Mutex: &kvStoreMutex,
	OutputPath: OutputFile,
}
type InputHandler struct {
	model.WriteConfig
}

func (h InputHandler) HandleRawInput(raw string) {
	res := data_pipeline.Process(&Store, raw)
	switch res.Status {
	case model.StatusResultFailed:
		h.WriteErr(res.Message+"\n")
	case model.StatusGetNoFound:
		h.WriteOut(model.NullDisplay+"\n")
	case model.StatusSetSuccess:
		h.WriteOut(model.SetOK+"\n")
	default:
		h.WriteOut(res.Message+"\n")
	}
}

func stdOutWrite(s string, as ...interface{}) {
	fmt.Fprintf(os.Stdout, s, as...)
}

func stdErrWrite(s string, as ...interface{}) {
	fmt.Fprintf(os.Stderr, s, as...)
}

func REPL() {
	scanner := bufio.NewScanner(os.Stdin)
	handler := InputHandler{
		WriteConfig: model.WriteConfig{
			WriteOut: stdOutWrite,
			WriteErr:  stdErrWrite,
		},
	}
	fmt.Printf(model.PROMPT)
	for scanner.Scan() {
		handler.HandleRawInput(scanner.Text())
		fmt.Printf(model.PROMPT)
	}
}

func NewSocketWriteConfig(c net.Conn) model.WriteConfig {
	return model.WriteConfig{
		WriteOut: func(s string, args ...interface{}) {
			c.Write([]byte(fmt.Sprintf(s, args...)))
		},
		WriteErr: func(s string, args ...interface{}) {
			c.Write([]byte(fmt.Sprintf(s, args...)))
		},
	}
}

func HandleNetworkReceived(c net.Conn) {
	buf := make([]byte, 1<<9)
	handler := InputHandler{WriteConfig: NewSocketWriteConfig(c)}
	for {
		handler.WriteConfig.WriteErr(model.PROMPT)
		nr, err := c.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Fprintf(os.Stderr, "failed to read network buffer: %v\n", err)
			}
			break
		}
		handler.HandleRawInput(string(buf[0:nr]))
	}
}

func ListenUnixSocket() {
	var err error
	l, err := net.Listen("unix", SockName)
	must(err)
	fmt.Printf("Listening on %s\n", SockName)
	for {
		fd, err := l.Accept()
		fmt.Fprintf(os.Stderr, "Accepted new connection\n")
		must(err)
		go HandleNetworkReceived(fd)
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

func Usage(ec int) {
	fmt.Printf("Usage: $0 [ --output path/to/output.json ] [ --sock /path/to/sock.sock ] [ --repl ] [ --help ]\n")
	os.Exit(ec)
}

func ParseArgs(args []string) {
	l := len(args)
	for i := 0; i < l; i++ {
		arg := args[i]
		switch arg {
		case "--help", "-h", "help":
			Usage(0)
		case "--repl":
			UseREPL = true
		case "--output", "-o":
			OutputFile = args[i+1]
			fmt.Printf("Setting output file: %s\n", OutputFile)
			i++
		case "--sock", "-s":
			SockName = args[i+1]
			fmt.Printf("Setting socket name: %s\n", SockName)
			i++
		default:
			panic("unrecognized arg: " + arg)
		}
	}
}

func main() {
	ParseArgs(os.Args[1:])
	InitKvStore()
	if UseREPL {
		REPL()
	} else {
		ListenUnixSocket()
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
