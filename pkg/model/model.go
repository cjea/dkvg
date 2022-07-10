package model

import (
	"errors"
	"sync"
)

const (
	PrefixSet      = "set "
	PrefixGet      = "get "
	PrefixSnapshot = "snapshot"
)

var (
	ErrNotFound   = errors.New("not foundd")
	ErrBadCommand = errors.New("unrecognized command (are you missing arguments?)")
)

const (
	StatusResultFailed = iota + 2
	StatusSetSuccess
	StatusGetSuccess
	StatusGetNoFound
	StatusSnapshotSuccess
)

const WALMagicNumber = 0x33AA33AA

type Result struct {
	Status  byte
	Message string
}

const (
	GlobalVersionKey = "_GLOBAL_VERSION"
)

type Store struct {
	Store         map[string]interface{}
	Mutex         *sync.RWMutex
	GlobalVersion uint64
}

type CmdType int

const (
	CmdUnknown CmdType = iota
	CmdGet
	CmdSet
	CmdSnapshot
)

type Cmd struct {
	Type CmdType
	Data interface{}
}

type WALCmd struct {
	*Cmd
	GlobalVersion uint64
}

type Pair struct {
	Left  string
	Right string
}

type WriteConfig struct {
	WriteOut func(string, ...interface{})
	WriteErr func(string, ...interface{})
}

const (
	PROMPT      = "#> "
	SetOK       = "OK"
	NullDisplay = "NULL"
)
