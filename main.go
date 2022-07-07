package main

import (
	"bufio"
	"dkvg/pkg/config"
	"dkvg/pkg/data_pipeline"
	"dkvg/pkg/model"
	"dkvg/pkg/wal"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

var Store *model.Store
type InputHandler struct {
	model.WriteConfig
	Store *model.Store
	WAL *wal.WAL
}

func (h InputHandler) HandleRawInput(raw string) {
	 debugStore := func() string {
		buf, err := json.Marshal(h.Store.Store)
		must(err)
		return fmt.Sprintf("VERSION %d\n%s\n", h.WAL.GlobalVersion, string(buf))
	 }
	res := data_pipeline.Process(h.Store, raw, h.WAL)
	switch res.Status {
	case model.StatusResultFailed:
		h.WriteErr(res.Message+"\n"+fmt.Sprintf("\n***STORE\n%s\n", debugStore()))
	case model.StatusGetNoFound:
		h.WriteOut(model.NullDisplay+"\n"+fmt.Sprintf("\n***STORE\n%s\n", debugStore()))
	case model.StatusSetSuccess:
		h.WriteOut(model.SetOK+"\n"+debugStore())
	case model.StatusSyncSuccess:
		h.WriteOut(debugStore()+"\n")
	default:
		h.WriteOut(res.Message+"\n"+(fmt.Sprintf("\n***STORE\n%s\n", debugStore())))
	}
}

func stdOutWrite(s string, as ...interface{}) {
	fmt.Fprintf(os.Stdout, s, as...)
}

func stdErrWrite(s string, as ...interface{}) {
	fmt.Fprintf(os.Stderr, s, as...)
}

func REPL(cfg *config.Config, s *model.Store, w *wal.WAL) {
	scanner := bufio.NewScanner(os.Stdin)
	handler := InputHandler{
		Store: s,
		WAL: w,
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

func HandleNetworkReceived(c net.Conn, store *model.Store, w *wal.WAL) {
	buf := make([]byte, 1<<9)
	handler := InputHandler{WriteConfig: NewSocketWriteConfig(c), Store: store, WAL: w}
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

func ListenUnixSocket(cfg *config.Config, s *model.Store, w *wal.WAL) {
	var err error
	l, err := net.Listen("unix", cfg.SockName)
	must(err)
	fmt.Printf("Listening on %s\n", cfg.SockName)

	for {
		fd, err := l.Accept()
		fmt.Fprintf(os.Stderr, "Accepted new connection\n")
		must(err)
		go HandleNetworkReceived(fd, s, w)
	}
}

func main2() {
	var err error
	w, err := wal.ParseWAL("wal.log")
	must(err)
	c := &model.Cmd{
		Type: model.CmdSet,
		Data: model.Pair{Left: "foo", Right: "the foo-iest"},
	}
	c2 := &model.Cmd{
		Type: model.CmdSet,
		Data: model.Pair{Left: "bar", Right: "such bar-ness"},
	}
	// bytes := wal.SerializeCmdForWAL(c)
	// fmt.Printf("entry bytes: %v\n", bytes)
	must(w.Append(c))
	must(w.Append(c2))
	fmt.Printf("WAL after update: %#v\n", w)
}

func main() {
	cfg := config.NewDefaultConfig()
	cfg.ParseArgs(os.Args[1:])
	s := &model.Store{
		Mutex: &sync.RWMutex{},
		OutputPath: cfg.OutputFile,
		Store: nil,
	}

	w, err := wal.ParseWAL(cfg.WALPath)
	must(err)

	must(wal.BuildStore(nil, w, s))
	fmt.Printf("Initial store: %#v\n", s.Store)

	if cfg.UseREPL {
		REPL(cfg, s, w)
	} else {
		ListenUnixSocket(cfg, s, w)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
