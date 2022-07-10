package wal

import (
	"dkvg/pkg/model"
	"dkvg/pkg/runcmd"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

type WALPath string

type WAL struct {
	Cmds       []*model.WALCmd
	Path       string
	FileHandle *os.File
	Mutex      *sync.RWMutex
	offset     uint64
}

func (w *WAL) SetOffset(o uint64) {
	w.offset = o
}

func (w *WAL) GlobalVersion() uint64 {
	if len(w.Cmds) == 0 {
		return 0
	}
	return w.Cmds[len(w.Cmds)-1].GlobalVersion
}

func (w *WAL) NextGlobalVersion() uint64 {
	ret := w.GlobalVersion() + 1
	if w.IsEmpty() {
		ret += w.offset
	}
	return ret
}

type SnapshotHandle struct {
	FullPath string
	FileInfo fs.FileInfo
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
	parts := [][]byte{
		keyLenBytes[:],
		valLenBytes[:],
		[]byte(p.Left),
		[]byte(p.Right),
	}

	// 2 for key length and 2 for val length. Not responsible for appending the
	// global version to the end of the record.
	fullSize := 4 + len(p.Left) + len(p.Right)
	res := make([]byte, fullSize)
	i := 0
	for _, part := range parts {
		i += copy(res[i:], part)
	}
	return res
}

type byModTime []fs.FileInfo

func (s byModTime) Len() int {
	return len(s)
}
func (s byModTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byModTime) Less(i, j int) bool {
	return s[i].ModTime().Before(s[j].ModTime())
}

// NewestSnapshot only returns an error for exceptions. If there are simply
// no snapshots in the snapshot/ directory, then this func returns (nil, nil).
func NewestSnapshot() (*SnapshotHandle, error) {
	infos, err := ListAllSnapshots()
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, nil
	}
	info := infos[len(infos)-1]
	h := &SnapshotHandle{
		FullPath: fmt.Sprintf("snapshot/%s", info.Name()),
		FileInfo: info,
	}

	return h, nil
}

// ListAllSnapshots returns all *.snapshot files in the snapshot directory,
// sorted by mod-time. Last element is the newest file.
func ListAllSnapshots() ([]fs.FileInfo, error) {
	var err error
	entries, err := os.ReadDir("snapshot")
	if err != nil {
		return nil, err
	}
	snapshots := make(byModTime, 0)
	for _, entry := range entries {
		n := entry.Name()
		if !strings.HasSuffix(n, ".snapshot") {
			continue
		}
		info, err := os.Stat("snapshot/" + n)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, info)
	}
	sort.Sort(snapshots)

	return snapshots, nil
}

func GetCurrentWAL() (*WAL, error) {
	return ParseWAL("wal.log")
}

func NewWAL(path string) (*WAL, error) {
	fmt.Printf("Initializing new WAL into '%s'\n", path)

	var err error
	prelude := model.WALMagicNumber
	preludeBytes := *(*[4]byte)(unsafe.Pointer(&prelude))
	versionBytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
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
		Cmds:       []*model.WALCmd{},
		Path:       path,
		FileHandle: rw,
		Mutex:      &sync.RWMutex{},
	}, nil
}

func ParseWAL(path string) (*WAL, error) {
	fmt.Printf("Parsing WAL at '%s'\n", path)
	var err error
	_, err = os.Stat(string(path))
	if err != nil {
		return NewWAL(path)
	}
	rw, err := os.OpenFile(string(path), os.O_RDWR, 0644)
	serializedWAL, err := ioutil.ReadAll(rw)
	if err != nil {
		return nil, err
	}
	if len(serializedWAL) == 0 {
		return nil, fmt.Errorf("WAL is empty")
	}
	checksum := *(*int32)(unsafe.Pointer(&serializedWAL[0]))
	if checksum != model.WALMagicNumber {
		return nil, fmt.Errorf("WAL is corrupt")
	}
	wal := WAL{
		Cmds:       []*model.WALCmd{},
		Path:       path,
		Mutex:      &sync.RWMutex{},
		FileHandle: rw,
		offset:     0,
	}
	highestVersion := uint64(0)
	// Add 4 for the magic number, 8 for the uint64 version.
	idx := 4 + 8
	for idx < len(serializedWAL) {
		keyLen := *(*uint16)(unsafe.Pointer(&serializedWAL[idx]))
		idx = idx + 2
		valLen := *(*uint16)(unsafe.Pointer(&serializedWAL[idx]))
		idx = idx + 2
		lastKeyByte := idx + int(keyLen) - 1
		lastValByte := lastKeyByte + int(valLen)
		key := strings.Builder{}
		for ; idx <= lastKeyByte; idx += 1 {
			key.WriteByte(serializedWAL[idx])
		}
		val := strings.Builder{}
		for ; idx <= lastValByte; idx += 1 {
			val.WriteByte(serializedWAL[idx])
		}
		cmd := &model.Cmd{
			Type: model.CmdSet,
			Data: model.Pair{Left: key.String(), Right: val.String()},
		}

		globalVersion := *(*uint64)(unsafe.Pointer(&serializedWAL[idx]))
		idx = idx + 8
		if globalVersion <= highestVersion {
			return nil, fmt.Errorf(
				"global version in '%s' must not decrease from %#v to %v (index=%v)",
				path, wal, globalVersion, idx,
			)
		}
		wal.Cmds = append(wal.Cmds, &model.WALCmd{Cmd: cmd, GlobalVersion: globalVersion})
		highestVersion = globalVersion
	}
	return &wal, nil
}

func (w *WAL) Append(c *model.Cmd) error {
	var err error
	bytes := SerializeCmdForWAL(c)
	entryLen := len(bytes) + 8
	v := w.NextGlobalVersion()
	fmt.Printf("about to append command version=%d\n", v)
	globalVersion := *(*[8]byte)(unsafe.Pointer(&v))

	w.Mutex.Lock()
	defer w.Mutex.Unlock()

	entry := make([]byte, entryLen)
	copy(entry, bytes)
	copy(entry[entryLen-8:], globalVersion[:])
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
	w.Cmds = append(w.Cmds, &model.WALCmd{Cmd: c, GlobalVersion: v})
	return nil
}

func (w *WAL) IsEmpty() bool {
	return len(w.Cmds) == 0
}

func BuildStore(wal *WAL, s *model.Store) error {
	wal.Mutex.RLock()
	defer wal.Mutex.RUnlock()

	if wal.IsEmpty() {
		fmt.Printf("Setting initial global version = %d\n", s.GlobalVersion)
		wal.SetOffset(s.GlobalVersion)
	}

	for _, cm := range wal.Cmds {
		if cm.GlobalVersion <= s.GlobalVersion {
			fmt.Printf(
				"Skipping command %#v\n , which is less than store version %d\n",
				cm.GlobalVersion, s.GlobalVersion,
			)
			continue
		}
		err := runcmd.KvSet(cm.Cmd, s)
		if err != nil {
			return fmt.Errorf(
				"command in WAL failed: cmd = %#v\n, sote = %#v", cm, s.Store,
			)
		}
		s.GlobalVersion = cm.GlobalVersion
	}
	return nil
}
