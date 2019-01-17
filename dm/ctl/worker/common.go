package worker

import (
	"context"

	"github.com/juju/errors"
	"github.com/pingcap/dm/checker"
	"github.com/pingcap/dm/dm/config"
)

func checkSubTask(ctx context.Context, task string) error {
	// precheck task
	cfg := config.NewSubTaskConfig()
	err := cfg.Decode(task)
	if err != nil {
		return errors.Annotatef(err, "decode task %s", task)
	}

	// poor man's precheck
	// TODO: improve process and display
	err = checker.CheckSyncConfig(ctx, []*config.SubTaskConfig{cfg})
	return errors.Trace(err)
}
