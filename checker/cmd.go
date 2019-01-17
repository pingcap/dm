package checker

import (
	"github.com/juju/errors"
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"golang.org/x/net/context"
)

// CheckSyncConfig checks synchronization configuration
func CheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig) error {
	c := NewChecker(cfgs)

	err := c.Init()
	if err != nil {
		return errors.Annotate(err, "fail to initial checker")
	}
	defer c.Close()

	pr := make(chan pb.ProcessResult, 1)
	c.Process(ctx, pr)
	for len(pr) > 0 {
		r := <-pr
		// we only want first error
		if len(r.Errors) > 0 {
			return errors.Errorf("fail to check synchronization configuration with type %v:\n %v\n detail: %v", r.Errors[0].Type, r.Errors[0].Msg, string(r.Detail))
		}
	}

	return nil
}
