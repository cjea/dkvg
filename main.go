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
	handler := InputHandler{
		WAL: w,
		Store: store,
		WriteConfig: NewSocketWriteConfig(c),
	}
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

func WriteWAL(w io.Writer) error {
	r, err := os.Open("wal.log")
	must(err)
	buf := make([]byte, 1<<16)
	for {
		nbytes, err := r.Read(buf)
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
		w.Write(buf[0:nbytes])
	}
}

var RequestTypeCatchup byte = 'C'
func validateCatchupRequest (p []byte) error {
	if p[0] != RequestTypeCatchup {
		return fmt.Errorf("the endpoint only accepts catchup requests")
	}
	return nil
}

func HandleCatchupRequest(wc io.ReadWriteCloser, p []byte) {
	var err error
	if err = validateCatchupRequest(p); err != nil {
		wc.Write([]byte(err.Error()))
		wc.Close()
	}
	must(WriteWAL(wc))
}

func ListenCatchupRequests() {
	l, err := net.ListenTCP(
		"tcp",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1025},
	)
	must(err)

	for {
		conn, err := l.AcceptTCP()
		must(err)
		fmt.Fprintf(os.Stderr, "Accepted new connection\n")
		var buf []byte = make([]byte, 1<<8)
		_, err = conn.Read(buf)
		must(err)
		go HandleCatchupRequest(conn, buf)
	}
}

func main() {
	// send 'C' via `nc localhost 1025` to stream the WAL.
	ListenCatchupRequests()
}


func main2() {
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
