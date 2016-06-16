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
	err        error
}

type temp struct {
	values []interface{}
	sqls   []string
	args   []interface{}
}

func (s *Session) getTableAndValue(model interface{}) (table *TableMetadata, value reflect.Value, err error) {
	table, err = getTableMeta(model)
	if err != nil {
		return
	}
	value = reflect.ValueOf(model)
	if value.Type().Kind() == reflect.Ptr {
		value = value.Elem()
	}
	return
}

func (s *Session) genMultiInsertSql(table *TableMetadata, sliceValue reflect.Value) (string, []interface{}) {
	sqls := make([]string, 0, sliceValue.Len())
	totalArgs := make([]interface{}, 0, sliceValue.Len()*len(table.Columns))
	var elementValue reflect.Value
	for i := 0; i < sliceValue.Len(); i++ {
		elementValue = sliceValue.Index(i)
		if elementValue.Type().Kind() == reflect.Ptr {
			elementValue = elementValue.Elem()
		}
		sqlstr, args := s.sqlGen.GenInsert(elementValue, table, s.clauseList)
		sqls = append(sqls, sqlstr)
		totalArgs = append(totalArgs, args...)
	}
	return strings.Join(sqls, ";"), totalArgs
}

func (s *Session) insertSlice2(table *TableMetadata, slice reflect.Value) (int64, error) {
	sqlStr, args := s.genMultiInsertSql(table, slice)
	node, _ := s.group.GetMaster()
	s.logger.Printf("sql:%s\r\n args:%v\r\n", sqlStr, args)
	result, err := node.Db.Exec(sqlStr, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// InsertSlice implements insert multiple records in one database call.
// If no sharding value specified, it does not guarantee all data are in one db transaction,
// but will guarantee the data lines in same db node are in db transaction
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

	if s.hasShardKey {
		var count int64
		if count, err = s.insertSlice2(table, slice); err != nil {
			return nil, err
		}
		return &SqlResult{Success: int(count)}, nil
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
		wait.Add(1)
		go func(group *DbGroup, t *temp) {
			node, _ := group.GetMaster()
			sqlstr := strings.Join(v.sqls, ";")
			s.logger.Printf("sql:%s\r\n args:%v\r\n", sqlstr, t.args)
			s.logger.Printf("exec sql againt db node %s\r\n", node.Name)
			_, err = node.Db.Exec(sqlstr, t.args...)
			r := SqlResult{}
			if err != nil {
				r.FailedData = t.values
				r.err = fmt.Errorf("Group: %s, Node:%s, error:%v", group.Name, node.Name, err)
			} else {
				r.Success = len(t.values)
			}
			ch_result <- r
			wait.Done()
		}(k, v)
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

func (s *Session) innerExec(model interface{}, value reflect.Value, table *TableMetadata,
	sqlStr string, args []interface{}) (sql.Result, error) {
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
	s.logger.Printf("exec sql against node %s", node.Name)
	return node.Db.Exec(sqlStr, args...)
}

//Insert data to db
func (s *Session) Insert(model interface{}) (int64, error) {
	defer s.reset()
	table, value, err := s.getTableAndValue(model)
	if err != nil {
		return 0, err
	}
	sqlStr, args := s.sqlGen.GenInsert(value, table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)

	var result sql.Result
	if result, err = s.innerExec(model, value, table, sqlStr, args); err != nil {
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
	table, value, err := s.getTableAndValue(model)
	if err != nil {
		return err
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
	sqlStr, args := s.genMultiInsertSql(table, slice)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	_, err = tx.Exec(sqlStr, args...)
	return err
}

func (s *Session) Update(model interface{}) (int64, error) {
	defer s.reset()
	table, value, err := s.getTableAndValue(model)
	if err != nil {
		return 0, err
	}
	sqlStr, args := s.sqlGen.GenUpdate(value, table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	var result sql.Result
	if s.hasShardKey {
		node, _ := s.group.GetMaster()
		if result, err = node.Db.Exec(sqlStr, args...); err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}

	return s.execSqlOnAllGroups(sqlStr, args)
}

func (s *Session) updateWithTx(tx *sql.Tx, model interface{}) error {
	defer s.reset()
	table, value, err := s.getTableAndValue(model)
	if err != nil {
		return err
	}
	sqlStr, args := s.sqlGen.GenUpdate(value, table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	_, err = tx.Exec(sqlStr, args...)
	return err
}

// Delete implements delete sql statment.
// If don't specify sharding value, the delete sql will be exeucted on master nodes of all groups.
func (s *Session) Delete(model interface{}) (int64, error) {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return 0, err
	}
	sqlStr, args := s.sqlGen.GenDelete(table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	var result sql.Result
	if s.hasShardKey {
		node, _ := s.group.GetMaster()
		s.logger.Printf("exec sql against node %s", node.Name)
		if result, err = node.Db.Exec(sqlStr, args...); err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}
	return s.execSqlOnAllGroups(sqlStr, args)
}

type tempResult struct {
	result sql.Result
	err    error
}

func (s *Session) execSqlOnAllGroups(sqlStr string, args []interface{}) (int64, error) {
	var err error
	ch_result := make(chan tempResult)
	wait := &sync.WaitGroup{}
	for _, g := range s.engine.cluster.Groups {
		wait.Add(1)
		go func(group *DbGroup) {
			node, _ := group.GetMaster()
			r := tempResult{}
			s.logger.Printf("exec sql against node %s", node.Name)
			r.result, r.err = node.Db.Exec(sqlStr, args...)
			if r.err != nil {
				r.err = fmt.Errorf("Group: %s, Node: %s, error:%v", group.Name, node.Name, r.err)
			}
			ch_result <- r
			wait.Done()
		}(g)
	}
	go func() {
		wait.Wait()
		close(ch_result)
	}()
	count := int64(0)
	for r := range ch_result {
		if r.err != nil {
			err = fmt.Errorf("%v;%v", err, r.err)
		} else {
			n, _ := r.result.RowsAffected()
			count += n
		}
	}
	return count, err
}

func (s *Session) deleteWithTx(tx *sql.Tx, model interface{}) error {
	defer s.reset()
	table, err := getTableMeta(model)
	if err != nil {
		return err
	}
	sqlStr, args := s.sqlGen.GenDelete(table, s.clauseList)
	s.logger.Printf("sql:%s, args:%#v\r\n", sqlStr, args)
	_, err = tx.Exec(sqlStr, args...)
	return err
}
