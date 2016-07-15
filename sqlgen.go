// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

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
	//Generats update sql
	GenUpdate(value reflect.Value, table *TableMetadata, sqls sqlClauseList) (string, []interface{})
	//Generats delete sql
	GenDelete(table *TableMetadata, sqls sqlClauseList) (string, []interface{})
}

type BaseGenerator struct {
	bufPool  *sync.Pool
	wrapFunc func(string) string
}

func newBaseGenerator() *BaseGenerator {
	g := BaseGenerator{bufPool: &sync.Pool{}}
	g.bufPool.New = func() interface{} { return &bytes.Buffer{} }
	return &g
}

func (b *BaseGenerator) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	b.bufPool.Put(buf)
}

func (b *BaseGenerator) getBuf() *bytes.Buffer {
	return b.bufPool.Get().(*bytes.Buffer)
}

//Generates select SQL statement
func (m *BaseGenerator) GenSelect(table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	var args []interface{}
	var colNames string
	var omitCols []string
	sqls = append(sqls, sqlClause{op: opType_table, clause: m.wrapColumn(table.Name)})
	sort.Sort(sqls)
	isPaging := false
	hasWhere := false
	var pagingParam []interface{}
	buf.WriteString("select ")
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_top:
			// buf.WriteString(fmt.Sprintf("top %v ", s.params...))
			isPaging = true
			pagingParam = []interface{}{0, 1}
		case opType_cols:
			colNames = s.clause
		case opType_omit:
			omitCols = strings.Split(strings.ToLower(s.clause), ",")
		case opType_table:
			buf.WriteString("%s")
			buf.WriteString(fmt.Sprintf(" from %v", s.clause))

		case opType_unlockTable:
			buf.WriteString(" with(nolock) ")
		case opType_id:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s=?", table.IdColumn.name))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s=?", table.IdColumn.name))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_where:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s", s.clause))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_and:
			buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			args = append(args, s.params...)
		case opType_or:
			buf.WriteString(fmt.Sprintf(" or (%s)", s.clause))
			args = append(args, s.params...)
		case opType_in:
			if len(s.params) > 0 {
				if hasWhere {
					buf.WriteString(fmt.Sprintf(" and %s in (%s)", s.clause, m.makeInArgs(s.params)))
				} else {
					buf.WriteString(fmt.Sprintf(" where %s in (%s)", s.clause, m.makeInArgs(s.params)))
					hasWhere = true
				}
			}
		case opType_in_or:
			if len(s.params) > 0 {
				buf.WriteString(fmt.Sprintf(" or(%s in (%s))", s.clause, m.makeInArgs(s.params)))
			}
		case opType_between:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s between ? and ?", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s between ? and ?", s.clause))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_between_or:
			buf.WriteString(fmt.Sprintf(" or (%s between ? and ?)", s.clause))
			args = append(args, s.params...)
		case opType_limit:
			isPaging = true
			pagingParam = s.params
		case opType_orderby:
			buf.WriteString(" order by ")
			buf.WriteString(s.clause)
		default:
			break
		}
	}
	if isPaging {
		buf.WriteString(fmt.Sprintf(" limit %v,%v", pagingParam[0], pagingParam[1]))
	}
	if len(colNames) <= 0 {
		cols := make([]string, 0, len(table.Columns))
		table.Columns.Foreach(func(colKey string, col *columnMetadata) {
			if col.rwType&io_type_ro == io_type_ro {
				if len(omitCols) > 0 {
					for i := range omitCols {
						if colKey == omitCols[i] {
							return
						}
					}
				}
				cols = append(cols, m.wrapColumn(col.name))
			}
		})
		colNames = strings.Join(cols, ",")
	}
	return fmt.Sprintf(buf.String(), colNames), args
}

func (m *BaseGenerator) makeInArgs(params []interface{}) string {
	element := reflect.Indirect(reflect.ValueOf(params[0]))
	isNumber := false
	switch element.Type().Kind() {
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8,
		reflect.Float32, reflect.Float64:
		isNumber = true
	default:
		isNumber = false
	}
	var buf bytes.Buffer
	for _, arg := range params {
		if isNumber {
			buf.WriteString(fmt.Sprintf("%v,", arg))
		} else {
			buf.WriteString(fmt.Sprintf("'%v',", arg))
		}
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

func (m *BaseGenerator) wrapColumn(colName string) string {
	if m.wrapFunc != nil {
		return m.wrapFunc(colName)
	}
	return fmt.Sprintf(`%s`, colName)
}

func (b *BaseGenerator) getValue(colMeta *columnMetadata, value reflect.Value) interface{} {
	field := value.FieldByIndex(colMeta.fieldIndex)
	if field.Type().Kind() == reflect.Ptr {
		field = field.Elem()
	}
	result := field.Interface()

	switch colMeta.goType.Kind() {
	case reflect.Struct, reflect.Slice:
		if colMeta.specialType == specialType_time {
			return result
		}
		data, _ := json.MarshalIndent(result, "", "")
		var buf bytes.Buffer
		json.Compact(&buf, data)
		return buf.String()
	default:
		return result
	}
}

//Generates insert SQL statement
func (m *BaseGenerator) GenInsert(value reflect.Value, table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	args := make([]interface{}, 0, len(table.Columns))
	var colNames []string
	include := true
Loop:
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_cols:
			colNames = strings.Split(strings.ToLower(s.clause), ",")
			break Loop
		case opType_omit:
			colNames = strings.Split(strings.ToLower(s.clause), ",")
			include = false
		}
	}
	buf.WriteString("insert into ")
	buf.WriteString(m.wrapColumn(table.Name))
	buf.WriteString("(")
	table.Columns.Foreach(func(col string, meta *columnMetadata) {
		if meta.isAutoId || meta.rwType&io_type_wo != io_type_wo {
			return
		}
		if len(colNames) <= 0 {
			buf.WriteString(m.wrapColumn(meta.name))
			buf.WriteString(",")
			args = append(args, m.getValue(meta, value))
			return
		}
		for _, name := range colNames {
			if name == col && include {
				buf.WriteString(m.wrapColumn(meta.name))
				buf.WriteString(",")
				args = append(args, m.getValue(meta, value))
				return
			}
			if name != col && !include {
				buf.WriteString(m.wrapColumn(meta.name))
				buf.WriteString(",")
				args = append(args, m.getValue(meta, value))
				return
			}
		}
	})
	buf.Truncate(buf.Len() - 1)
	buf.WriteString(fmt.Sprintf(") values(%s)", strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")))
	return buf.String(), args
}

//Generates insert SQL statement
func (m *BaseGenerator) GenUpdate(value reflect.Value, table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	sqlWhere := m.getBuf()
	defer m.putBuf(buf)
	defer m.putBuf(sqlWhere)
	args := make([]interface{}, 0, len(table.Columns))
	whereArgs := make([]interface{}, 0)
	var colNames []string
	include := true
	hasWhere := false
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_cols:
			colNames = strings.Split(strings.ToLower(s.clause), ",")
		case opType_omit:
			colNames = strings.Split(strings.ToLower(s.clause), ",")
			include = false
		case opType_id:
			if hasWhere {
				sqlWhere.WriteString(fmt.Sprintf(" and %s=?", table.IdColumn.name))
			} else {
				sqlWhere.WriteString(fmt.Sprintf(" where %s=?", table.IdColumn.name))
				hasWhere = true
			}
			whereArgs = append(whereArgs, s.params...)
		case opType_where:
			if hasWhere {
				sqlWhere.WriteString(fmt.Sprintf(" and %s", s.clause))
			} else {
				sqlWhere.WriteString(fmt.Sprintf(" where %s", s.clause))
				hasWhere = true
			}
			whereArgs = append(whereArgs, s.params...)
		case opType_and:
			sqlWhere.WriteString(fmt.Sprintf(" and %s", s.clause))
			whereArgs = append(whereArgs, s.params...)
		case opType_or:
			sqlWhere.WriteString(fmt.Sprintf(" or (%s)", s.clause))
			whereArgs = append(whereArgs, s.params...)
		case opType_in:
			if len(s.params) > 0 {
				if hasWhere {
					sqlWhere.WriteString(fmt.Sprintf(" and %s in (%s)", s.clause, m.makeInArgs(s.params)))
				} else {
					sqlWhere.WriteString(fmt.Sprintf(" where %s in (%s)", s.clause, m.makeInArgs(s.params)))
					hasWhere = true
				}
			}
		case opType_in_or:
			if len(s.params) > 0 {
				sqlWhere.WriteString(fmt.Sprintf(" or(%s in (%s))", s.clause, m.makeInArgs(s.params)))
			}
		case opType_between:
			if hasWhere {
				sqlWhere.WriteString(fmt.Sprintf(" and %s between ? and ?", s.clause))
			} else {
				sqlWhere.WriteString(fmt.Sprintf(" where %s between ? and ?", s.clause))
				hasWhere = true
			}
			whereArgs = append(whereArgs, s.params...)
		case opType_between_or:
			sqlWhere.WriteString(fmt.Sprintf(" or (%s between ? and ?)", s.clause))
			whereArgs = append(whereArgs, s.params...)
		}
	}
	buf.WriteString("update ")
	buf.WriteString(m.wrapColumn(table.Name))
	buf.WriteString(" set ")
	table.Columns.Foreach(func(col string, meta *columnMetadata) {
		if meta.isAutoId || meta.rwType&io_type_wo != io_type_wo {
			return
		}
		if len(colNames) <= 0 {
			buf.WriteString(m.wrapColumn(meta.name))
			buf.WriteString("=?,")
			args = append(args, m.getValue(meta, value))
			return
		}
		for _, name := range colNames {
			if name == col && include {
				buf.WriteString(m.wrapColumn(meta.name))
				buf.WriteString("=?,")
				args = append(args, m.getValue(meta, value))
				return
			}
			if name != col && !include {
				buf.WriteString(m.wrapColumn(meta.name))
				buf.WriteString("=?,")
				args = append(args, m.getValue(meta, value))
				return
			}
		}
	})
	buf.Truncate(buf.Len() - 1)
	if sqlWhere.Len() > 0 {
		buf.Write(sqlWhere.Bytes())
		args = append(args, whereArgs...)
	}
	return buf.String(), args
}

func (m *BaseGenerator) GenDelete(table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	args := make([]interface{}, 0, len(table.Columns))
	hasWhere := false
	buf.WriteString("delete from ")
	buf.WriteString(m.wrapColumn(table.Name))
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_id:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s=?", table.IdColumn.name))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s=?", table.IdColumn.name))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_where:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s", s.clause))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_and:
			buf.WriteString(fmt.Sprintf(" and %s", s.clause))
			args = append(args, s.params...)
		case opType_or:
			buf.WriteString(fmt.Sprintf(" or (%s)", s.clause))
			args = append(args, s.params...)
		case opType_in:
			if len(s.params) > 0 {
				if hasWhere {
					buf.WriteString(fmt.Sprintf(" and %s in (%s)", s.clause, m.makeInArgs(s.params)))
				} else {
					buf.WriteString(fmt.Sprintf(" where %s in (%s)", s.clause, m.makeInArgs(s.params)))
					hasWhere = true
				}
			}
		case opType_in_or:
			if len(s.params) > 0 {
				buf.WriteString(fmt.Sprintf(" or(%s in (%s))", s.clause, m.makeInArgs(s.params)))
			}
		case opType_between:
			if hasWhere {
				buf.WriteString(fmt.Sprintf(" and %s between ? and ?", s.clause))
			} else {
				buf.WriteString(fmt.Sprintf(" where %s between ? and ?", s.clause))
				hasWhere = true
			}
			args = append(args, s.params...)
		case opType_between_or:
			buf.WriteString(fmt.Sprintf(" or (%s between ? and ?)", s.clause))
			args = append(args, s.params...)
		}
	}
	return buf.String(), args
}
