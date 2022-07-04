package store

import (
	"dkvg/pkg/config"
	"dkvg/pkg/model"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

// InitKvStore looks for a file at OutputPath, and creates one if none exists.
func InitKvStore(cfg *config.Config) *model.Store {
	var kvStore = map[string]interface{}{}
	var kvStoreMutex = sync.RWMutex{}
	var err error
	_, err = os.Stat(cfg.OutputFile)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Initializing store at %s\n", cfg.OutputFile)
		emptyJSON := []byte{'{', '}'}
		must(ioutil.WriteFile(cfg.OutputFile, emptyJSON, 0644))
	}
	fmt.Printf("Loading store from %s\n", cfg.OutputFile)
	f, err := os.Open(cfg.OutputFile)
	must(err)
	data, err := ioutil.ReadAll(f)
	must(err)
	must(json.Unmarshal(data, &kvStore))
	return &model.Store{
		Store: kvStore,
		Mutex: &kvStoreMutex,
		OutputPath: cfg.OutputFile,
	}
}


func must(err error) {
	if err != nil {
		panic(err)
	}
}
