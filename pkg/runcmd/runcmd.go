package runcmd

import (
	"dkvg/pkg/model"
	"dkvg/pkg/shot"
	"errors"
	"fmt"
)

type OrderedAppender interface {
	Append(*model.Cmd) error
	GlobalVersion() uint64
}

func AppendCmd(cmd *model.Cmd, a OrderedAppender) error {
	var err error
	err = a.Append(cmd)
	return err
}

func KvSet(cmd *model.Cmd, store *model.Store) error {
	store.Mutex.Lock()
	defer store.Mutex.Unlock()

	pair, ok := cmd.Data.(model.Pair)
	if !ok {
		return fmt.Errorf("invariant: expected a pair, got %#v", cmd.Data)
	}

	store.Store[pair.Left] = pair.Right
	return nil
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

func DispatchSet(cmd *model.Cmd, store *model.Store, a OrderedAppender) error {
	var err error
	err = AppendCmd(cmd, a)
	if err != nil {
		return err
	}
	store.GlobalVersion = a.GlobalVersion()
	return KvSet(cmd, store)
}

func DispatchGet(cmd *model.Cmd, store *model.Store) (string, error) {
	k, ok := cmd.Data.(string)
	if !ok {
		// invariant: system bug
		return "", fmt.Errorf("expected a string, got %#v", cmd.Data)
	}

	return KvGet(k, store)
}

func DispatchSnapshot(store *model.Store) error {
	return shot.Snapshot(store)
}

func RunCmd(cmd *model.Cmd, store *model.Store, a OrderedAppender) (model.Result, error) {
	var err error
	fail := func(err error) (model.Result, error) { return model.Result{}, err }

	switch cmd.Type {
	case model.CmdSet:
		if err = DispatchSet(cmd, store, a); err != nil {
			return fail(err)
		}
		return model.Result{Status: model.StatusSetSuccess}, nil
	case model.CmdGet:
		val, err := DispatchGet(cmd, store)
		if err == nil {
			return model.Result{Status: model.StatusGetSuccess, Message: val}, nil
		} else if errors.Is(err, model.ErrNotFound) {
			return model.Result{Status: model.StatusGetNoFound}, nil
		} else {
			return fail(err)
		}
	case model.CmdSnapshot:
		if err = DispatchSnapshot(store); err != nil {
			return fail(err)
		}
		return model.Result{Status: model.StatusSnapshotSuccess}, nil
	default:
		return fail(model.ErrBadCommand)
	}
}
