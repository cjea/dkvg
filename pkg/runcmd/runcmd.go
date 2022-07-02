package runcmd

import (
	"dkvg/pkg/model"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
)

func Persist(store *model.Store, outputPath string) error {
	var err error
	serialized, err := json.Marshal(store)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(outputPath, serialized, 0644)
	return err
}


func KvSet(pair model.Pair, store *model.Store) error {
	var err error
	store.Mutex.Lock()
	defer store.Mutex.Unlock()
	store.Store[pair.Left] = pair.Right
	err = Persist(store, store.OutputPath)
	return err
}

func KvGet(key string, store *model.Store) (string, error) {
	store.Mutex.RLock()
	defer store.Mutex.RUnlock()
	val, ok := store.Store[key]
	if !ok {
		return "", model.ErrNotFound
	}
	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("non string type: %v", val)
	}
	return str, nil
}

func DispatchSet(cmd *model.Cmd, store *model.Store) error {
	pair, ok := cmd.Data.(model.Pair)
	if !ok {
		return fmt.Errorf("invariant: expected a pair, got %#v", cmd.Data)
	}

	return KvSet(pair, store)
}

func DispatchGet(cmd *model.Cmd, store *model.Store) (string, error) {
	k, ok := cmd.Data.(string)
	if !ok {
		// invariant: system bug
		return "", fmt.Errorf("expected a string, got %#v", cmd.Data)
	}

	return KvGet(k, store)
}

func RunCmd(cmd *model.Cmd, store *model.Store) (model.Result, error) {
	var err error
	fail := func(err error) (model.Result, error) { return model.Result{}, err }

	switch cmd.Type {
	case model.CmdSet:
		if err = DispatchSet(cmd, store); err != nil {
			return fail(err)
		}
		return model.Result{Status: model.StatusSetSuccess }, nil
	case model.CmdGet:
		val, err := DispatchGet(cmd, store)
		if err == nil {
			return model.Result{Status: model.StatusGetSuccess, Message: val}, nil
		} else if errors.Is(err, model.ErrNotFound) {
			return model.Result{Status: model.StatusGetNoFound}, nil
		} else {
			return fail(err)
		}
	default:
		return fail(model.ErrBadCommand)
	}
}
