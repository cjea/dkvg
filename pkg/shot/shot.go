package shot

import (
	"dkvg/pkg/model"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
	"unsafe"
)

func SerializeStore(s *model.Store) ([]byte, error) {
	var err error
	var data []byte
	data, err = json.Marshal(s.Store)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(data) + 8)
	versionBytes := *(*[8]byte)(unsafe.Pointer(&s.GlobalVersion))
	copy(out, versionBytes[:])
	copy(out[8:], data)
	return out, nil
}

func ParseSnapshot(raw []byte) (uint64, map[string]interface{}, error) {
	var err error
	out := map[string]interface{}{}
	version := *(*uint64)(unsafe.Pointer(&raw[0]))
	data := raw[8:]
	err = json.Unmarshal(data, &out)
	if err != nil {
		return 0, nil, fmt.Errorf("snapshot is corrupt: %s", err.Error())
	}
	return version, out, nil
}

// Snapshot records the given database, along with the current global WAL.
func Snapshot(s *model.Store) error {
	var err error
	id := time.Now().Unix()
	storeshot := fmt.Sprintf("snapshot/%d_store.snapshot", id)
	logshot := fmt.Sprintf("snapshot/%d_wal.log", id)

	bytes, err := SerializeStore(s)
	if err != nil {
		return err
	}
	ioutil.WriteFile(storeshot, bytes, 0444)
	r, err := os.Open("wal.log")
	if err != nil {
		return err
	}
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(logshot, buf, 0444)
	if err != nil {
		return err
	}
	return nil
}

func ReadSnapshot(fullPath string) (uint64, map[string]interface{}, error) {
	fail := func(err error) (uint64, map[string]interface{}, error) {
		return 0, nil, fmt.Errorf("%s -- %s", fullPath, err.Error())
	}

	r, err := os.Open(fullPath)
	if err != nil {
		return fail(err)
	}
	raw, err := ioutil.ReadAll(r)
	if err != nil {
		return fail(err)
	}
	return ParseSnapshot(raw)
}
