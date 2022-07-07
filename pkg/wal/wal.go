package wal

import (
	"dkvg/pkg/model"
	"dkvg/pkg/runcmd"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"unsafe"
)

type WALPath string

type WAL struct {
	// GlobalVersion tracks the highest numbered Cmd in the WAL.
	GlobalVersion uint64
	Cmds []*model.Cmd
	Path string
	FileHandle *os.File
	Mutex *sync.RWMutex
}

type SnapshotHandle struct {
	path string
	cache map[string]interface{}
}

func SerializeCmdForWAL(c *model.Cmd) []byte {
	p, ok := c.Data.(model.Pair)
	if !ok {
		panic(fmt.Sprintf("can't serialize commands of this type: %#v", c))
	}
	keyLen := uint16(len(p.Left))
	valLen := uint16(len(p.Right))
	keyLenBytes := *(*[2]byte)(unsafe.Pointer(&keyLen))
	valLenBytes := *(*[2]byte)(unsafe.Pointer(&valLen))

	// 2 for key length and 2 for val length. Calling code should append the
	// global version.
	fullSize := 4 + len(p.Left) + len(p.Right)
	res := make([]byte, fullSize)
	parts := [][]byte{
		keyLenBytes[:],
		valLenBytes[:],
		[]byte(p.Left),
		[]byte(p.Right),
	}
	i := 0
	for _, sl := range parts {
		i += copy(res[i:], sl)
	}
	return res
}

func (h *SnapshotHandle) Parse() (map[string]interface{}, error) {
	if h.cache != nil {
		return h.cache, nil
	}

	var err error
	r, err := os.Open(h.path)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	m := map[string]interface{}{}
	err = json.Unmarshal(bytes, &m)
	if err != nil {
		return nil, err
	}
	h.cache = m
	return h.cache, nil
}

func GetCurrentWAL() *WAL {
	panic("not implemented: GetCurrentWAL")
}

func Snapshot (*model.Store, *WAL, WALPath) SnapshotHandle {
	panic("not implemented: Snapshot")
}

func NewWAL(path string) (*WAL, error) {
	fmt.Printf("Initializing new WAL into '%s'\n", path)

	var err error
	prelude := model.WALMagicNumber
	preludeBytes := *(*[4]byte)(unsafe.Pointer(&prelude))
	versionBytes := []byte{0,0,0,0,0,0,0,0}
	rw, err := os.OpenFile(string(path), os.O_RDWR|os.O_CREATE, 0644)
	_, err = rw.Write(preludeBytes[:])
	if err != nil {
		return nil, err
	}
	_, err = rw.Write(versionBytes[:])
	if err != nil {
		return nil, err
	}
	return &WAL{
		Cmds: []*model.Cmd{},
		GlobalVersion: 0,
		Path: path,
		FileHandle: rw,
		Mutex: &sync.RWMutex{},
	}, nil
}

func ParseWAL (path string) (*WAL, error) {
	fmt.Printf("Parsing WAL at '%s'\n", path)
	var err error
	_, err = os.Stat(string(path))
	if err != nil {
		return NewWAL(path)
	}
	rw, err := os.OpenFile(string(path), os.O_RDWR, 0644)
	bytes, err := ioutil.ReadAll(rw)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("WAL is empty")
	}
	checksum := *(*int32)(unsafe.Pointer(&bytes[0]))
	if checksum != model.WALMagicNumber {
		return nil, fmt.Errorf("WAL is corrupt")
	}
	version := *(*uint64)(unsafe.Pointer(&bytes[4]))
	wal := WAL{
		Cmds: []*model.Cmd{},
		GlobalVersion: version,
		Path: path,
		Mutex: &sync.RWMutex{},
		FileHandle: rw,
	}
	// Add 4 for the magic number, 8 for the uint64 version.
	idx := 4 + 8
	for idx < len(bytes) {
		keyLen := *(*uint16)(unsafe.Pointer(&bytes[idx]))
		idx = idx + 2
		valLen := *(*uint16)(unsafe.Pointer(&bytes[idx]))
		idx = idx + 2
		lastKeyByte := idx + int(keyLen) - 1
		lastValByte := lastKeyByte + int(valLen)
		key := strings.Builder{}
		for ; idx <= lastKeyByte; idx += 1 {
			key.WriteByte(bytes[idx])
		}
		val := strings.Builder{}
		for ; idx <= lastValByte; idx += 1 {
			val.WriteByte(bytes[idx])
		}
		cmd := &model.Cmd{
			Type: model.CmdSet,
			Data: model.Pair{Left: key.String(), Right: val.String()},
		}
		wal.Cmds = append(wal.Cmds, cmd)
		globalVersion := *(*uint64)(unsafe.Pointer(&bytes[idx]))
		idx = idx + 8
		if globalVersion <= wal.GlobalVersion {
			return nil, fmt.Errorf(
				"global version in '%s' must not decrease from %#v to %v (index=%v)",
				path, wal, globalVersion, idx,
			)
		}
		wal.GlobalVersion = globalVersion
	}
	return &wal, nil
}

func (w *WAL) Append(c *model.Cmd) error {
	var err error
	bytes := SerializeCmdForWAL(c)
	entryLen := len(bytes) + 8
	v := w.GlobalVersion + 1
	globalVersion := *(*[8]byte)(unsafe.Pointer(&v))

	w.Mutex.Lock()
	defer w.Mutex.Unlock()

	entry := make([]byte, entryLen)
	copy(entry, bytes)
	copy(entry[entryLen - 8:], globalVersion[:])
	if _, err = w.FileHandle.Seek(0, 2); err != nil {
		return err
	}
	_, err = os.Stat(w.FileHandle.Name())
	if err != nil {
		return err
	}
	if _, err := w.FileHandle.Write(entry); err != nil {
		return err
	}
	w.Cmds = append(w.Cmds, c)
	w.GlobalVersion = v
	return nil
}

func BuildStore(h *SnapshotHandle, wal *WAL, s *model.Store) error {
	init := map[string]interface{}{}
	if h != nil {
		data, err := h.Parse()
		if err != nil {
			return err
		}
		init = data
	}
	s.Store = init
	wal.Mutex.RLock()
	defer wal.Mutex.RUnlock()

	for _, cm := range wal.Cmds {
		err := runcmd.KvSet(cm, s)
		if err != nil {
			return fmt.Errorf(
				"command in WAL failed: cmd = %#v\n, sote = %#v", cm, s.Store,
			)
		}

	}
	return nil
}
