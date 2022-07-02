package model

import (
	"errors"
	"sync"
)

const (
	PrefixSet = "set "
	PrefixGet = "get "
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
)

type Result struct {Status byte; Message string}

type Store struct {
	Store map[string]interface{}
	Mutex *sync.RWMutex
	OutputPath string
}

type CmdType int

const (
	CmdUnknown CmdType = iota
	CmdGet
	CmdSet
)

type Cmd struct {
	Type CmdType
	Data interface{}
}

type Pair struct {
	Left  string
	Right string
}

type WriteConfig struct {
	WriteOut func(string, ...interface{})
	WriteErr  func(string, ...interface{})
}

const (
	PROMPT   = "#> "
	SetOK    = "OK"
	NullDisplay = "NULL"
)
