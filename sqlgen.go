// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import "reflect"

type opType int8

const (
	opType_rawQuery opType = iota + 1
	opType_limit
	opType_top
	opType_cols
	opType_omit
	opType_table
	opType_unlockTable
	opType_id
	opType_where
	opType_and
	opType_or
	opType_in
	opType_in_or
	opType_between
	opType_between_or
	opType_orderby
)

type sqlClause struct {
	op     opType
	clause string
	params []interface{}
}

type sqlClauseList []sqlClause

func (list sqlClauseList) Len() int {
	return len(list)
}

func (list sqlClauseList) Less(i, j int) bool {
	return list[i].op < list[j].op
}

func (list sqlClauseList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

//SqlGenerator that generate standard sql statement
type SqlGenerator interface {
	GenSelect(table *TableMetadata, sqls sqlClauseList) (string, []interface{})
	//Generates insert sql
	GenInsert(value reflect.Value, table *TableMetadata, sqls sqlClauseList) (string, []interface{})
}
