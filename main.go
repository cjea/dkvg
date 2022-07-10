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
	"strconv"
	"strings"
	"sync"
)

var Store *model.Store

type InputHandler struct {
	model.WriteConfig
	Store *model.Store
	WAL   *wal.WAL
}

func REPL(cfg *config.Config, s *model.Store, w *wal.WAL) {
	scanner := bufio.NewScanner(os.Stdin)
	handler := InputHandler{
		Store: s,
		WAL:   w,
		WriteConfig: model.WriteConfig{
			WriteOut: func(s string, as ...interface{}) {
				fmt.Fprintf(os.Stdout, s, as...)
			},
			WriteErr: func(s string, as ...interface{}) {
				fmt.Fprintf(os.Stderr, s, as...)
			},
		},
	}
	handler.WriteOut(model.PROMPT)
	for scanner.Scan() {
		handler.ExecuteUserCommand(scanner.Text())
		handler.WriteOut(model.PROMPT)
	}
}

func (h InputHandler) ExecuteUserCommand(raw string) {
	debugStore := func() string {
		buf, err := json.Marshal(h.Store.Store)
		must(err)
		return fmt.Sprintf("VERSION %d\n%s\n", h.Store.GlobalVersion, string(buf))
	}
	res := data_pipeline.Process(h.Store, raw, h.WAL)
	switch res.Status {
	case model.StatusResultFailed:
		h.WriteErr(res.Message + "\n" + fmt.Sprintf("\n***STORE\n%s\n", debugStore()))
	case model.StatusGetNoFound:
		h.WriteOut(model.NullDisplay + "\n" + fmt.Sprintf("\n***STORE\n%s\n", debugStore()))
	case model.StatusSetSuccess:
		h.WriteOut(model.SetOK + "\n" + debugStore())
	case model.StatusSnapshotSuccess:
		h.WriteOut("Persisted snapshot" + "\n" + debugStore() + "\n")
	default:
		h.WriteOut(res.Message + "\n" + (fmt.Sprintf("\n***STORE\n%s\n", debugStore())))
	}
}

// NewSocketWriteConfig sends all writes to the connection. Maybe in the future
// it should be optional to specify custom write functions instead.
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

func HandleUserCommand(c net.Conn, store *model.Store, w *wal.WAL) {
	buf := make([]byte, 1<<9)
	handler := InputHandler{
		WAL:         w,
		Store:       store,
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
		handler.ExecuteUserCommand(string(buf[0:nr]))
	}
}

func AcceptUserCommand(cfg *config.Config, s *model.Store, w *wal.WAL) {
	var err error
	l, err := net.Listen("unix", cfg.SockName)
	must(err)
	fmt.Printf("Listening on %s\n", cfg.SockName)

	for {
		fd, err := l.Accept()
		fmt.Fprintf(os.Stderr, "Accepted new user command\n")
		must(err)
		go HandleUserCommand(fd, s, w)
	}
}

func WriteWAL(w io.Writer, version uint64) error {
	fmt.Printf("TODO: handle the version in WriteWAL (would be %d)\n", version)
	r, err := os.Open("wal.log")
	if err != nil {
		return err
	}
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

func validateCatchupRequest(p []byte) (uint64, error) {
	usage := func() (uint64, error) {
		return 0, fmt.Errorf("invalid catchup request. 'C<VERSION>'")
	}

	if p[0] != RequestTypeCatchup {
		return usage()
	}
	isDigit := func(b byte) bool {
		switch b {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return true
		}
		return false
	}
	sb := strings.Builder{}
	for _, d := range p[1:] {
		if !isDigit(d) {
			break
		}
		sb.WriteByte(d)
	}
	if sb.Len() < 1 {
		return usage()
	}
	i, err := strconv.Atoi(sb.String())
	if err != nil {
		return 0, err
	}
	return uint64(i), nil
}

func HandleCatchupRequest(wc io.ReadWriteCloser, p []byte) {
	var err error
	var version uint64
	if version, err = validateCatchupRequest(p); err != nil {
		wc.Write([]byte(err.Error()))
		wc.Close()
		return
	}
	if err = WriteWAL(wc, version); err != nil {
		fmt.Printf("%s\n", err.Error())
		wc.Write([]byte("unexpected error"))
	}
	wc.Close()
}

func AcceptCatchupRequests() {
	l, err := net.ListenTCP(
		"tcp",
		&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1025},
	)
	must(err)

	for {
		conn, err := l.AcceptTCP()
		must(err)
		fmt.Fprintf(os.Stderr, "Accepted new catchup request\n")
		var buf []byte = make([]byte, 1<<8)
		_, err = conn.Read(buf)
		must(err)
		go HandleCatchupRequest(conn, buf)
	}
}

func main() {
	// send 'C' via `nc localhost 1025` to stream the WAL.
	go AcceptCatchupRequests()
	run()
}

func main2() {
}

func run() {
	cfg := config.NewDefaultConfig()
	cfg.ParseArgs(os.Args[1:])
	s := &model.Store{
		Store:         nil,
		GlobalVersion: 0,
		Mutex:         &sync.RWMutex{},
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
		AcceptUserCommand(cfg, s, w)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
