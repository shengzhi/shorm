// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Implements sql insert, update, delete operations

package shorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type SqlResult struct {
	Success    int
	FailedData []interface{}
}

type temp struct {
	values []interface{}
	sqls   []string
	args   []interface{}
}

//InsertSlice does not guarantee all elements in one db transaction,
//but will guarantee the elements line in same node are in db transaction
func (s *Session) InsertSlice(slicePtr interface{}) (*SqlResult, error) {
	defer s.reset()
	slice := reflect.Indirect(reflect.ValueOf(slicePtr))
	if slice.Kind() != reflect.Slice {
		return nil, fmt.Errorf("slicePtr must be a pointer to slice")
	}
	if slice.Len() <= 0 {
		return nil, nil
	}

	elementType := slice.Index(0).Type()
	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
	}
	if elementType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("element type must be struct")
	}
	table, err := getTableMeta(reflect.Indirect(slice.Index(0)).Interface())
	if err != nil {
		return nil, err
	}

	shardGroup := make(map[*DbGroup]*temp, 0)
	var shardValue int64
	var element interface{}
	var elementValue reflect.Value
	for i := 0; i < slice.Len(); i++ {
		shardValue = 0
		elementValue = slice.Index(i)
		element = elementValue.Interface()
		if table.IsShardinger {
			shardValue = element.(Shardinger).GetShardValue()
		} else {
			if table.ShardColumn != nil {
				shardField := elementValue.FieldByIndex(table.ShardColumn.fieldIndex)
				if shardField.Type().Kind() == reflect.Ptr {
					shardField = shardField.Elem()
				}
				shardValue := shardField.Interface()
				switch v := shardValue.(type) {
				case int, int32, int64, uint, uint32, uint64:
					shardValue, _ = strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
				}
			}
		}
		var group *DbGroup
		var has bool
		if shardValue > 0 {
			group, has = s.engine.cluster.findGroup(shardValue)
			if !has {
				group, _ = s.engine.cluster.DefaultGroup()
			}
		} else {
			group, _ = s.engine.cluster.DefaultGroup()
		}
		if elementValue.Type().Kind() == reflect.Ptr {
			elementValue = elementValue.Elem()
		}
		sqlstr, args := s.sqlGen.GenInsert(elementValue, table, s.clauseList)
		if v, ok := shardGroup[group]; ok {
			v.values = append(v.values, element)
			v.sqls = append(v.sqls, sqlstr)
			v.args = append(v.args, args...)
		} else {
			shardGroup[group] = &temp{
				values: []interface{}{element},
				sqls:   []string{sqlstr},
				args:   args,
			}
		}
	}

	result := &SqlResult{}
	ch_result := make(chan SqlResult)
	wait := &sync.WaitGroup{}
	for k, v := range shardGroup {
		node, _ := k.GetMaster()
		s.logger.Printf("exec insert slice againt db node %s\r\n", node.Name)

		wait.Add(1)
		go func(t *temp) {
			sqlstr := strings.Join(v.sqls, ";")
			s.logger.Printf("sql:%s\r\n args:%v\r\n", sqlstr, t.args)
			_, err = node.Db.Exec(sqlstr, t.args...)
			if err != nil {
				ch_result <- SqlResult{FailedData: t.values}
			} else {
				ch_result <- SqlResult{Success: len(t.values)}
			}
			wait.Done()
		}(v)
	}
	go func() {
		wait.Wait()
		close(ch_result)
	}()
	for r := range ch_result {
		result.Success += r.Success
		result.FailedData = append(result.FailedData, r.FailedData...)
	}
	return result, err
}

//InsertMulti equivalent to foreach to call method Insert(model interface{})
//It's not in db transaction
func (s *Session) InsertMulti(models ...interface{}) (int64, error) {
	count := int64(0)
	for i := range models {
		if n, err := s.Insert(models[i]); err != nil {
			return count, err
		} else {
			count += n
		}
	}
	return count, nil
}

//Insert data to db
func (s *Session) Insert(model interface{}) (int64, error) {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return 0, err
	}
	value := reflect.ValueOf(model)
	if value.Type().Kind() == reflect.Ptr {
		value = value.Elem()
	}
	sqlStr, args := s.sqlGen.GenInsert(value, table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)

	if !s.hasShardKey {
		if table.IsShardinger {
			s.ShardValue(model.(Shardinger).GetShardValue())
		} else {
			if table.ShardColumn == nil {
				s.group, _ = s.engine.cluster.DefaultGroup()
			} else {
				shardField := value.FieldByIndex(table.ShardColumn.fieldIndex)
				if shardField.Type().Kind() == reflect.Ptr {
					shardField = shardField.Elem()
				}
				shardValue := shardField.Interface()
				switch v := shardValue.(type) {
				case int, int32, int64, uint, uint32, uint64:
					number, _ := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
					s.ShardValue(number)
				default:
					s.group, _ = s.engine.cluster.DefaultGroup()
				}
			}
		}
	}
	node, _ := s.group.GetMaster()
	s.logger.Printf("exec insert against node %s", node.Name)
	var result sql.Result
	if result, err = node.Db.Exec(sqlStr, args...); err != nil {
		return 0, err
	}
	if autoId, err := result.LastInsertId(); err == nil {
		for _, v := range table.Columns {
			if v.isAutoId {
				value.FieldByIndex(v.fieldIndex).Set(reflect.ValueOf(autoId))
				break
			}
		}
	}
	return result.RowsAffected()
}

func (s *Session) insertWithTx(tx *sql.Tx, model interface{}) error {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return err
	}
	value := reflect.ValueOf(model)
	if value.Type().Kind() == reflect.Ptr {
		value = value.Elem()
	}
	sqlStr, args := s.sqlGen.GenInsert(value, table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(sqlStr)
	if err != nil {
		return err
	}
	var result sql.Result
	if result, err = stmt.Exec(args...); err != nil {
		return err
	}
	if autoId, err := result.LastInsertId(); err == nil {
		for _, v := range table.Columns {
			if v.isAutoId {
				value.FieldByIndex(v.fieldIndex).Set(reflect.ValueOf(autoId))
				break
			}
		}
	}
	return nil
}

func (s *Session) insertSliceWithTx(tx *sql.Tx, slicePtr interface{}) error {
	defer s.reset()
	slice := reflect.Indirect(reflect.ValueOf(slicePtr))
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("slicePtr must be a pointer to slice")
	}
	if slice.Len() <= 0 {
		return nil
	}

	elementType := slice.Index(0).Type()
	if elementType.Kind() == reflect.Ptr {
		elementType = elementType.Elem()
	}
	if elementType.Kind() != reflect.Struct {
		return fmt.Errorf("element type must be struct")
	}
	table, err := getTableMeta(reflect.Indirect(slice.Index(0)).Interface())
	if err != nil {
		return err
	}

	sqls := make([]string, 0, slice.Len())
	totalArgs := make([]interface{}, 0)
	var elementValue reflect.Value
	for i := 0; i < slice.Len(); i++ {
		elementValue = slice.Index(i)
		if elementValue.Type().Kind() == reflect.Ptr {
			elementValue = elementValue.Elem()
		}
		sqlstr, args := s.sqlGen.GenInsert(elementValue, table, s.clauseList)
		sqls = append(sqls, sqlstr)
		totalArgs = append(totalArgs, args...)
	}
	sqlList := strings.Join(sqls, ";")
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlList, totalArgs)
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(sqlList)
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(totalArgs...); err != nil {
		return err
	}
	return nil
}
