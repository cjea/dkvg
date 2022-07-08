package main

import (
	"bufio"
	"dkvg/pkg/config"
	"dkvg/pkg/data_pipeline"
	"dkvg/pkg/model"
	"dkvg/pkg/shot"
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
		return fmt.Sprintf("VERSION %d\n%s\n", h.Store.GlobalVersion, string(buf))
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
	case model.StatusSnapshotSuccess:
		h.WriteOut("Persisted snapshot"+"\n"+debugStore()+"\n")
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

// func main() {
// 	r, err := os.Open("snapshot/1657249158_store.snapshot")
// 	must(err)
// 	raw, err := ioutil.ReadAll(r)
// 	must(err)
// 	version, data, err := shot.ParseSnapshot(raw)
// 	must(err)
// 	fmt.Printf("parsed version=%v\ndata=%v\n", version, data)
// }

func main() {
	run()
}

func run() {
	cfg := config.NewDefaultConfig()
	cfg.ParseArgs(os.Args[1:])
	s := &model.Store{
		Store: nil,
		GlobalVersion: 0,
		Mutex: &sync.RWMutex{},
		OutputPath: cfg.OutputFile,
	}

	h, err := wal.NewestSnapshot()
	must(err)
	if h != nil {
		v, data, err := shot.ReadSnapshot(h.FullPath)
		must(err)
		s.GlobalVersion = v
		s.Store = data
	}
	w, err := wal.ParseWAL(cfg.WALPath)
	must(err)

	must(wal.BuildStore(w, s))

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
