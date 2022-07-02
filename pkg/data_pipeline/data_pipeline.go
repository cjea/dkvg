package data_pipeline

import (
	"dkvg/pkg/model"
	"dkvg/pkg/parse"
	"dkvg/pkg/runcmd"
)

const (
	FAIL_STATUS = iota + 2
)

func Process(store *model.Store, input string) model.Result {
	var err error
	fail := func(err error) model.Result {
		return model.Result{Status: model.StatusResultFailed, Message: err.Error()}
	}

	cmd, err := parse.Parse(input)
	if err != nil {
		return fail(err)
	}
	res, err := runcmd.RunCmd(cmd, store)
	if err != nil {
		return fail(err)
	}
	return res
}
