package syncer

import (
	"fmt"
	"sort"

	"github.com/siddontang/go-mysql/mysql"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/utils"
)

// ExecErrorContext records a failed exec SQL information
type ExecErrorContext struct {
	err  error
	pos  mysql.Position
	jobs string
}

// Error implements SubTaskUnit.Error
func (s *Syncer) Error() interface{} {
	s.execErrors.Lock()
	defer s.execErrors.Unlock()

	sort.Slice(s.execErrors.errors, func(i, j int) bool {
		return utils.CompareBinlogPos(s.execErrors.errors[i].pos, s.execErrors.errors[j].pos, 0) == -1
	})

	errors := make([]*pb.SyncSQLError, 0, len(s.execErrors.errors))
	for _, ctx := range s.execErrors.errors {
		errors = append(errors, &pb.SyncSQLError{
			Msg:                  ctx.err.Error(),
			FailedBinlogPosition: fmt.Sprintf("%s:%d", ctx.pos.Name, ctx.pos.Pos),
			ErrorSQL:             ctx.jobs,
		})
	}

	return &pb.SyncError{Errors: errors}
}
