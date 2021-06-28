package syncer

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// ExprFilterGroup groups many related fields about expression filter.
type ExprFilterGroup struct {
	Configs        map[string][]*config.ExpressionFilter // tableName -> raw config
	GenExprFunc    func(db, table, expr string) (*config.Expression, error)
	InsertExprs    map[string][]*config.Expression // tableName -> expr
	UpdateOldExprs map[string][]*config.Expression // tableName -> expr
	UpdateNewExprs map[string][]*config.Expression // tableName -> expr
	DeleteExprs    map[string][]*config.Expression // tableName -> expr

	hasInsertFilter    map[string]struct{} // set(tableName)
	hasUpdateOldFilter map[string]struct{} // set(tableName)
	hasUpdateNewFilter map[string]struct{} // set(tableName)
	hasDeleteFilter    map[string]struct{} // set(tableName)
}

// NewExprFilterGroup creates an ExprFilterGroup.
func NewExprFilterGroup(exprConfig []*config.ExpressionFilter,
	genFunc func(db, table, expr string) (*config.Expression, error),
) *ExprFilterGroup {
	ret := &ExprFilterGroup{
		Configs:            map[string][]*config.ExpressionFilter{},
		GenExprFunc:        genFunc,
		InsertExprs:        map[string][]*config.Expression{},
		UpdateOldExprs:     map[string][]*config.Expression{},
		UpdateNewExprs:     map[string][]*config.Expression{},
		DeleteExprs:        map[string][]*config.Expression{},
		hasInsertFilter:    map[string]struct{}{},
		hasUpdateOldFilter: map[string]struct{}{},
		hasUpdateNewFilter: map[string]struct{}{},
		hasDeleteFilter:    map[string]struct{}{},
	}
	for _, c := range exprConfig {
		tableName := dbutil.TableName(c.Schema, c.Table)
		ret.Configs[tableName] = append(ret.Configs[tableName], c)

		if c.InsertValueExpr != "" {
			ret.hasInsertFilter[tableName] = struct{}{}
		}
		if c.UpdateOldValueExpr != "" {
			ret.hasUpdateOldFilter[tableName] = struct{}{}
		}
		if c.UpdateNewValueExpr != "" {
			ret.hasUpdateNewFilter[tableName] = struct{}{}
		}
		if c.DeleteValueExpr != "" {
			ret.hasDeleteFilter[tableName] = struct{}{}
		}
	}
	return ret
}

// GetInsertExprs returns the expression filters for given table to filter INSERT events.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetInsertExprs(db, table string) ([]*config.Expression, error) {
	key := dbutil.TableName(db, table)

	if ret, ok := g.InsertExprs[key]; ok {
		return ret, nil
	}
	if _, ok := g.hasInsertFilter[key]; !ok {
		return nil, nil
	}

	for _, c := range g.Configs[key] {
		if c.InsertValueExpr != "" {
			expr, err2 := g.GenExprFunc(db, table, c.InsertValueExpr)
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.InsertExprs[key] = append(g.InsertExprs[key], expr)
		}
	}
	return g.InsertExprs[key], nil
}

// GetUpdateExprs returns two lists of expression filters for given table, to filter UPDATE events by old values and new
// values respectively. The two lists should have same length, and the corresponding expressions is AND logic.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetUpdateExprs(db, table string) ([]*config.Expression, []*config.Expression, error) {
	key := dbutil.TableName(db, table)

	retOld, ok1 := g.UpdateOldExprs[key]
	retNew, ok2 := g.UpdateNewExprs[key]
	if ok1 || ok2 {
		return retOld, retNew, nil
	}

	if _, ok := g.hasUpdateOldFilter[key]; ok {
		for _, c := range g.Configs[key] {
			if c.UpdateOldValueExpr != "" {
				expr, err := g.GenExprFunc(db, table, c.UpdateOldValueExpr)
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.UpdateOldExprs[key] = append(g.UpdateOldExprs[key], expr)
			} else {
				g.UpdateOldExprs[key] = append(g.UpdateOldExprs[key], &config.Expression{
					Expression: expression.NewOne(),
				})
			}
		}
	}

	if _, ok := g.hasUpdateNewFilter[key]; ok {
		for _, c := range g.Configs[key] {
			if c.UpdateNewValueExpr != "" {
				expr, err := g.GenExprFunc(db, table, c.UpdateNewValueExpr)
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.UpdateNewExprs[key] = append(g.UpdateNewExprs[key], expr)
			} else {
				g.UpdateNewExprs[key] = append(g.UpdateNewExprs[key], &config.Expression{
					Expression: expression.NewOne(),
				})
			}
		}
	}

	return g.UpdateOldExprs[key], g.UpdateNewExprs[key], nil
}

// GetDeleteExprs returns the expression filters for given table to filter DELETE events.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetDeleteExprs(db, table string) ([]*config.Expression, error) {
	key := dbutil.TableName(db, table)

	if ret, ok := g.DeleteExprs[key]; ok {
		return ret, nil
	}
	if _, ok := g.hasDeleteFilter[key]; !ok {
		return nil, nil
	}

	for _, c := range g.Configs[key] {
		if c.DeleteValueExpr != "" {
			expr, err2 := g.GenExprFunc(db, table, c.DeleteValueExpr)
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.DeleteExprs[key] = append(g.DeleteExprs[key], expr)
		}
	}
	return g.DeleteExprs[key], nil
}

// ResetExprs deletes the expressions generated before. This should be called after table structure changed.
func (g *ExprFilterGroup) ResetExprs(db, table string) {
	key := dbutil.TableName(db, table)
	delete(g.InsertExprs, key)
	delete(g.UpdateOldExprs, key)
	delete(g.UpdateNewExprs, key)
	delete(g.DeleteExprs, key)
}

// SkipDMLByExpression returns true when given row matches the expr, which means this row should be skipped.
func SkipDMLByExpression(row []interface{}, expr *config.Expression, upstreamCols []*model.ColumnInfo) (bool, error) {
	log.L().Debug("will evaluate the expression", zap.Stringer("expression", expr), zap.Any("raw row", row))
	datums, err := utils.AdjustBinaryProtocolForDatum(row, upstreamCols)
	if err != nil {
		return false, err
	}
	r := chunk.MutRowFromDatums(datums).ToRow()

	d, err := expr.Eval(r)
	if err != nil {
		return false, err
	}
	return d.GetInt64() == 1, nil
}
