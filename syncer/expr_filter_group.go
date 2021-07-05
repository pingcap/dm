package syncer

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/chunk"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

// ExprFilterGroup groups many related fields about expression filter.
type ExprFilterGroup struct {
	configs        map[string][]*config.ExpressionFilter // tableName -> raw config
	insertExprs    map[string][]expression.Expression    // tableName -> expr
	updateOldExprs map[string][]expression.Expression    // tableName -> expr
	updateNewExprs map[string][]expression.Expression    // tableName -> expr
	deleteExprs    map[string][]expression.Expression    // tableName -> expr

	hasInsertFilter    map[string]struct{} // set(tableName)
	hasUpdateOldFilter map[string]struct{} // set(tableName)
	hasUpdateNewFilter map[string]struct{} // set(tableName)
	hasDeleteFilter    map[string]struct{} // set(tableName)
}

// NewExprFilterGroup creates an ExprFilterGroup.
func NewExprFilterGroup(exprConfig []*config.ExpressionFilter) *ExprFilterGroup {
	ret := &ExprFilterGroup{
		configs:            map[string][]*config.ExpressionFilter{},
		insertExprs:        map[string][]expression.Expression{},
		updateOldExprs:     map[string][]expression.Expression{},
		updateNewExprs:     map[string][]expression.Expression{},
		deleteExprs:        map[string][]expression.Expression{},
		hasInsertFilter:    map[string]struct{}{},
		hasUpdateOldFilter: map[string]struct{}{},
		hasUpdateNewFilter: map[string]struct{}{},
		hasDeleteFilter:    map[string]struct{}{},
	}
	for _, c := range exprConfig {
		tableName := dbutil.TableName(c.Schema, c.Table)
		ret.configs[tableName] = append(ret.configs[tableName], c)

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
func (g *ExprFilterGroup) GetInsertExprs(db, table string, ti *model.TableInfo) ([]expression.Expression, error) {
	key := dbutil.TableName(db, table)

	if ret, ok := g.insertExprs[key]; ok {
		return ret, nil
	}
	if _, ok := g.hasInsertFilter[key]; !ok {
		return nil, nil
	}

	for _, c := range g.configs[key] {
		if c.InsertValueExpr != "" {
			expr, err2 := getSimpleExprOfTable(c.InsertValueExpr, ti)
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.insertExprs[key] = append(g.insertExprs[key], expr)
		}
	}
	return g.insertExprs[key], nil
}

// GetUpdateExprs returns two lists of expression filters for given table, to filter UPDATE events by old values and new
// values respectively. The two lists should have same length, and the corresponding expressions is AND logic.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetUpdateExprs(db, table string, ti *model.TableInfo) ([]expression.Expression, []expression.Expression, error) {
	key := dbutil.TableName(db, table)

	retOld, ok1 := g.updateOldExprs[key]
	retNew, ok2 := g.updateNewExprs[key]
	if ok1 || ok2 {
		return retOld, retNew, nil
	}

	if _, ok := g.hasUpdateOldFilter[key]; ok {
		for _, c := range g.configs[key] {
			if c.UpdateOldValueExpr != "" {
				expr, err := getSimpleExprOfTable(c.UpdateOldValueExpr, ti)
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.updateOldExprs[key] = append(g.updateOldExprs[key], expr)
			} else {
				g.updateOldExprs[key] = append(g.updateOldExprs[key], expression.NewOne())
			}
		}
	}

	if _, ok := g.hasUpdateNewFilter[key]; ok {
		for _, c := range g.configs[key] {
			if c.UpdateNewValueExpr != "" {
				expr, err := getSimpleExprOfTable(c.UpdateNewValueExpr, ti)
				if err != nil {
					// TODO: terror
					return nil, nil, err
				}
				g.updateNewExprs[key] = append(g.updateNewExprs[key], expr)
			} else {
				g.updateNewExprs[key] = append(g.updateNewExprs[key], expression.NewOne())
			}
		}
	}

	return g.updateOldExprs[key], g.updateNewExprs[key], nil
}

// GetDeleteExprs returns the expression filters for given table to filter DELETE events.
// This function will lazy calculate expressions if not initialized.
func (g *ExprFilterGroup) GetDeleteExprs(db, table string, ti *model.TableInfo) ([]expression.Expression, error) {
	key := dbutil.TableName(db, table)

	if ret, ok := g.deleteExprs[key]; ok {
		return ret, nil
	}
	if _, ok := g.hasDeleteFilter[key]; !ok {
		return nil, nil
	}

	for _, c := range g.configs[key] {
		if c.DeleteValueExpr != "" {
			expr, err2 := getSimpleExprOfTable(c.DeleteValueExpr, ti)
			if err2 != nil {
				// TODO: terror
				return nil, err2
			}
			g.deleteExprs[key] = append(g.deleteExprs[key], expr)
		}
	}
	return g.deleteExprs[key], nil
}

// ResetExprs deletes the expressions generated before. This should be called after table structure changed.
func (g *ExprFilterGroup) ResetExprs(db, table string) {
	key := dbutil.TableName(db, table)
	delete(g.insertExprs, key)
	delete(g.updateOldExprs, key)
	delete(g.updateNewExprs, key)
	delete(g.deleteExprs, key)
}

// SkipDMLByExpression returns true when given row matches the expr, which means this row should be skipped.
func SkipDMLByExpression(row []interface{}, expr expression.Expression, upstreamCols []*model.ColumnInfo) (bool, error) {
	// TODO: add metrics
	log.L().Debug("will evaluate the expression", zap.Stringer("expression", expr), zap.Any("raw row", row))
	data, err := utils.AdjustBinaryProtocolForDatum(row, upstreamCols)
	if err != nil {
		return false, err
	}
	r := chunk.MutRowFromDatums(data).ToRow()

	d, err := expr.Eval(r)
	if err != nil {
		return false, err
	}
	return d.GetInt64() == 1, nil
}

// getSimpleExprOfTable returns an expression of given `expr` string, using the table structure that is tracked before.
func getSimpleExprOfTable(expr string, ti *model.TableInfo) (expression.Expression, error) {
	// TODO: use upstream timezone?
	e, err := expression.ParseSimpleExprWithTableInfo(utils.UTCSession, expr, ti)
	if err != nil {
		// if expression contains an unknown column, we return an expression that skips nothing
		if core.ErrUnknownColumn.Equal(err) {
			log.L().Warn("meet unknown column when generating expression, return a FALSE expression instead",
				zap.String("expression", expr),
				zap.Error(err))
			e = expression.NewZero()
		} else {
			return nil, err
		}
	}

	return e, nil
}
