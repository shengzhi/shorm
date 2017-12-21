// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shorm

import (
	"reflect"
	"strings"
)

// SqlWhere sql where 子句
type SqlWhere sqlClauseList

// Where equals < where col=? >
func (s SqlWhere) Where(clause string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_where,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// And equals < and col=? >
func (s SqlWhere) And(clause string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_and,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// Or equals < or col=? >
func (s SqlWhere) Or(clause string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_or,
		clause: clause,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// In equals < and col in(?) >
func (s SqlWhere) In(colName string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_in,
		clause: colName,
	}
	if len(args) > 0 {
		if len(args) > 1 {
			subSql.params = append(subSql.params, args...)
		} else {
			arg := args[0]
			val := reflect.ValueOf(arg)
			if val.Type().Kind() == reflect.Slice {
				for i := 0; i < val.Len(); i++ {
					subSql.params = append(subSql.params, val.Index(i).Interface())
				}
			} else {
				subSql.params = append(subSql.params, arg)
			}
		}

	}
	s = append(s, subSql)
	return s
}

// OrIn equals < or col in (?) >
func (s SqlWhere) OrIn(colName string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_in_or,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// Between equals <and col between ? and ? >
func (s SqlWhere) Between(colName string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_between,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// OrBetween equals <or col between ? and ? >
func (s SqlWhere) OrBetween(colName string, args ...interface{}) SqlWhere {
	subSql := sqlClause{
		op:     opType_between_or,
		clause: colName,
	}
	if len(args) > 0 {
		subSql.params = append(subSql.params, args...)
	}
	s = append(s, subSql)
	return s
}

// Limit equals < limit skip,size >
func (s SqlWhere) Limit(skip, size int) SqlWhere {
	subSql := sqlClause{
		op: opType_limit,
	}
	subSql.params = append(subSql.params, skip, size)
	s = append(s, subSql)
	return s
}

// OrderBy equals <order by ? >
func (s SqlWhere) OrderBy(orderby ...string) SqlWhere {
	subSql := sqlClause{
		op:     opType_orderby,
		clause: strings.Join(orderby, ","),
	}
	s = append(s, subSql)
	return s
}

// Cols specify which columns will be selected or affected
func (s SqlWhere) Cols(cols string) SqlWhere {
	subSql := sqlClause{
		op:     opType_cols,
		clause: cols,
	}
	s = append(s, subSql)
	return s
}

// Omit specify which columns will not be selected or affected
func (s SqlWhere) Omit(cols string) SqlWhere {
	subSql := sqlClause{
		op:     opType_omit,
		clause: cols,
	}
	s = append(s, subSql)
	return s
}
