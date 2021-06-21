package config

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/session"
)

// TODO: move related struct to tidb-tools

// ExpressionFilter represents a filter that will be applied on row changes.
type ExpressionFilter struct {
	Schema             string `yaml:"schema" toml:"schema" json:"schema"`
	Table              string `yaml:"table" toml:"table" json:"table"`
	InsertValueExpr    string `yaml:"insert-value-expr" toml:"insert-value-expr" json:"insert-value-expr"`
	UpdateOldValueExpr string `yaml:"update-old-value-expr" toml:"update-old-value-expr" json:"update-old-value-expr"`
	UpdateNewValueExpr string `yaml:"update-new-value-expr" toml:"update-new-value-expr" json:"update-new-value-expr"`
	DeleteValueExpr    string `yaml:"delete-value-expr" toml:"delete-value-expr" json:"delete-value-expr"`
}

type Expression struct {
	expression.Expression
	TableInfo *model.TableInfo
	Session   session.Session
}
