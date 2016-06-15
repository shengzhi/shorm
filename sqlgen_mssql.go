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

type MSSqlGenerator struct {
	bufPool *sync.Pool
}

func NewMSSqlGenerator() *MSSqlGenerator {
	g := MSSqlGenerator{bufPool: &sync.Pool{}}
	g.bufPool.New = func() interface{} { return &bytes.Buffer{} }
	return &g
}

func (m *MSSqlGenerator) putBuf(buf *bytes.Buffer) {
	buf.Reset()
	m.bufPool.Put(buf)
}

func (m *MSSqlGenerator) getBuf() *bytes.Buffer {
	return m.bufPool.Get().(*bytes.Buffer)
}

//Generates select SQL statement
func (m *MSSqlGenerator) GenSelect(table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	var args []interface{}
	var colNames string
	var omitCols []string
	sqls = append(sqls, sqlClause{op: opType_table, clause: m.wrapColumn(table.Name)})
	sort.Sort(sqls)
	isPaging := false
	pagingOrder := table.IdColumn.name
	hasWhere := false
	var pagingParam []interface{}
	buf.WriteString("select ")
	for _, s := range sqls {
		switch s.op {
		case opType_rawQuery:
			return s.clause, s.params
		case opType_top:
			buf.WriteString(fmt.Sprintf("top %v ", s.params...))
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
			buf.WriteString("ROW_NUMBER() OVER (order by %s) as row,")
			isPaging = true
			pagingParam = s.params
		case opType_orderby:
			if isPaging {
				pagingOrder = s.clause
			} else {
				buf.WriteString(" order by ")
				buf.WriteString(s.clause)
			}
		default:
			break
		}
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
	if isPaging {
		sqlStr := fmt.Sprintf("select top %[3]v * from (%[1]s) t where t.row > %[2]v",
			fmt.Sprintf(buf.String(), pagingOrder, colNames), pagingParam[0], pagingParam[1])
		return sqlStr, args
	}
	return fmt.Sprintf(buf.String(), colNames), args
}

func (m *MSSqlGenerator) makeInArgs(params []interface{}) string {
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

func (m *MSSqlGenerator) wrapColumn(colName string) string {
	return fmt.Sprintf("[%s]", colName)
}

//Generates insert SQL statement
func (m *MSSqlGenerator) GenInsert(value reflect.Value, table *TableMetadata, sqls sqlClauseList) (string, []interface{}) {
	buf := m.getBuf()
	defer m.putBuf(buf)
	args := make([]interface{}, 0, len(table.Columns))
	var colNames []string
	include := true
Loop:
	for _, s := range sqls {
		switch s.op {
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

func (m *MSSqlGenerator) getValue(colMeta *columnMetadata, value reflect.Value) interface{} {
	field := value.FieldByIndex(colMeta.fieldIndex)
	if field.Type().Kind() == reflect.Ptr {
		field = field.Elem()
	}
	result := field.Interface()

	switch colMeta.goType.Kind() {
	case reflect.Struct:
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
