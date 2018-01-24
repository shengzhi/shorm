// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package shorm  implements simple select, insert, update, delete operations
//against relation database cluster.
package shorm

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

var sqlGenDict = map[string]SqlGenerator{
	"mssql":    NewMSSqlGenerator(),
	"mysql":    newBaseGenerator(),
	"mymysql":  newBaseGenerator(),
	"postgres": newBaseGenerator(),
	"sqlite":   newBaseGenerator(),
}

type emptyLogger struct {
}

func (e *emptyLogger) Write(b []byte) (n int, err error) {
	return
}

//Engine provides the entry point to interact with database
type Engine struct {
	cluster *Cluster
	Logger  *log.Logger
	pool    *sync.Pool
	driver  string
}

//NewEngine will create *Engine type according to specified driver and cluster,
//it is recommended to maintain one instance for Engine type.
/*
	driver provided by third package, for example: mysql, mymysql, mssql etc... .
	cluster is the relation database cluster includes one or more db groups.
*/
func NewEngine(driver string, cluster *Cluster) *Engine {
	e := &Engine{
		cluster: cluster,
		pool:    &sync.Pool{},
		driver:  driver,
		Logger:  log.New(&emptyLogger{}, "", 0),
	}
	e.cluster.RealGroups = len(e.cluster.Groups)
	e.pool.New = func() interface{} { return &Session{} }
	return e
}

// NewEngineV2 creates DB Engine with specified connection string
func NewEngineV2(driver, connstr string) *Engine {
	cluster := &Cluster{TotalGroups: 1}
	cluster.Groups = append(cluster.Groups, &DbGroup{
		Nodes: []*DbNode{
			{
				Name:    "master",
				ConnStr: connstr,
				Type:    NodeType_Master,
			}},
	})
	return NewEngine(driver, cluster)
}

type clusterConfig struct {
	XMLName xml.Name `json:"-" xml:"ClusterConfig"`
	Driver  string   `json:"driver"`
	Cluster *Cluster `json:"cluster"`
}

// NewEngineFromConfig allows Engine can be created from config file.
// Currently, the config file could be xml or json file.
/*
	json config:
	{
	  "driver": "mssql",
	  "cluster": {
	    "total_groups": 10,
	    "groups": [
	      {
	        "range_from": 0,
	        "range_to": 4,
	        "is_default": true,
	        "name": "group1",
	        "nodes": [
	          {
	            "name": "g1_master",
	            "conn_string": "server=localhost;database=Test;user id=user;password=pwd",
	            "node_type": "master"
	          },
	          {
	            "name": "g1_node1",
	            "conn_string": "server=localhost;database=Test;user id=user;password=pwd",
	            "node_type": "slave",
	            "weight": 2
	          }
	        ]
	      },
	      {
	        "range_from": 5,
	        "range_to": 9,
	        "name": "group2",
	        "nodes": [
	          {
	            "name": "g2_master",
	            "conn_string": "server=localhost;database=Test;user id=user;password=pwd",
	            "node_type": "master"
	          },
	          {
	            "name": "g2_node1",
	            "conn_string": "server=localhost;database=Test;user id=user;password=pwd",
	            "node_type": "slave",
	            "weight": 2
	          }
	        ]
	      }
	    ]
	  }
	}

	xml config:
	<ClusterConfig>
		<Driver>mssql</Driver>
		<Cluster>
			<TotalGroups>10</TotalGroups>
			<Groups>
				<Group>
					<Name>group1</Name>
					<RangeFrom>0</RangeFrom>
					<RangeTo>4</RangeTo>
					<Nodes>
						<Node>
							<Name>g1_master</Name>
							<ConnStr>database connection string</ConnStr>
							<NodeType>master</NodeType>
						</Node>
						<Node>
							<Name>g1_node</Name>
							<ConnStr>database connection string</ConnStr>
							<NodeType>slave</NodeType>
							<Weight>2</Weight>
						</Node>
					</Nodes>
				</Group>
				<Group>
					<Name>group1</Name>
					<RangeFrom>5</RangeFrom>
					<RangeTo>9</RangeTo>
					<Nodes>
						<Node>
							<Name>g2_master</Name>
							<ConnStr>database connection string</ConnStr>
							<NodeType>master</NodeType>
						</Node>
						<Node>
							<Name>g2_node</Name>
							<ConnStr>database connection string</ConnStr>
							<NodeType>slave</NodeType>
							<Weight>2</Weight>
						</Node>
					</Nodes>
				</Group>
			</Groups>
		</Cluster>
	</ClusterConfig>
*/
func NewEngineFromConfig(config string) (*Engine, error) {
	var cluster clusterConfig
	file, err := os.Open(config)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	config = strings.ToLower(config)
	if strings.HasSuffix(config, ".json") {
		d := json.NewDecoder(file)
		if err = d.Decode(&cluster); err != nil {
			return nil, err
		}
	} else if strings.HasSuffix(config, ".xml") {
		d := xml.NewDecoder(file)
		if err = d.Decode(&cluster); err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("the file format of config file is not be supported.")
	}
	return NewEngine(cluster.Driver, cluster.Cluster), nil
}

//Current the framework only supports single database transaction,
//doesn't support distribute database trasnaction.
//Db transaction only will happen on master node.
/*
	Usage:
	trans, err := engine.BeginTrans(shardValue)
	if err = trans.Insert(m1);err != nil{
		trans.Rollback()
		retrun
	}
	if err = trans.Insert(m2);err != nil{
		trans.Rollback()
		retrun
	}
	trans.Commit()
*/
func (e *Engine) BeginTrans(shardValue int64) (*DbTrans, error) {
	return newDbTrans(e, shardValue)
}

//When executing non-transaction sql query, call StartSession to create db session to execute sql operation.
//After sql query done, call EndSession to put it back into object pool, save memory space
/*
	Usage:
		s := engine.StartSession()
		defer engine.EndSession(s)
		m := Model{}
		s.Id(1).Get(&m)

		var slice []*Model
		s.Where("Age>?",12).Limit(20,10).Find(&slice)
*/
func (e *Engine) StartSession() *Session {
	s := e.pool.Get().(*Session)
	s.logger = e.Logger
	s.sqlGen = sqlGenDict[e.driver]
	s.engine = e
	return s
}

// After sql query done, call EndSession to put session back into object pool.
func (e *Engine) EndSession(s *Session) {
	s.reset()
	e.pool.Put(s)
}

// Open will open all database nodes in cluster
func (e *Engine) Open() error {
	return e.cluster.Open(e.driver)
}

// Close will close all database nodes in cluster
func (e *Engine) Close() error {
	return e.cluster.Close()
}

// EnableDebug will make framwork running on debug model
func (e *Engine) EnableDebug() {
	e.Logger = log.New(os.Stdout, "", log.LstdFlags)
}

// SetLogger allows user can set customer logger
func (e *Engine) SetLogger(logger *log.Logger) {
	e.Logger = logger
}

// GetByPK 按主键获取记录
func (e *Engine) GetByPK(pk, v interface{}) (bool, error) {
	s := e.StartSession()
	defer e.EndSession(s)
	return s.Id(pk).Get(v)
}

// Get 获取单条记录
func (e *Engine) Get(where SqlWhere, v interface{}) (bool, error) {
	s := e.StartSession()
	defer e.EndSession(s)
	s.clauseList = append(s.clauseList, where...)
	return s.Get(v)
}

// GetAll 获取整张表
func (e *Engine) GetAll(slicePtr interface{}, cols ...string) error {
	s := e.StartSession()
	defer e.EndSession(s)
	if len(cols) >= 0 {
		s = s.Cols(strings.Join(cols, ","))
	}
	return s.Where("1=1").Find(slicePtr)
}

// GetAllByPage 按页获取整表数据
func (e *Engine) GetAllByPage(slicePtr interface{}, skip, size int, orderby string, cols ...string) error {
	s := e.StartSession()
	defer e.EndSession(s)
	if len(cols) >= 0 {
		s = s.Cols(strings.Join(cols, ","))
	}
	if orderby != "" {
		s = s.OrderBy(orderby)
	}
	return s.Limit(skip, size).Find(slicePtr)
}

// Count 统计
func (e *Engine) Count(where SqlWhere, model interface{}) (int64, error) {
	s := e.StartSession()
	defer e.EndSession(s)
	s.clauseList = append(s.clauseList, where...)
	return s.Count(model)
}

// Find 查询
func (e *Engine) Find(where SqlWhere, slicePtr interface{}) error {
	s := e.StartSession()
	defer e.EndSession(s)
	s.clauseList = append(s.clauseList, where...)
	return s.Find(slicePtr)
}

// Insert 保存单个对象
func (e *Engine) Insert(model interface{}) (int64, error) {
	s := e.StartSession()
	defer e.EndSession(s)
	return s.Insert(model)
}

// InsertMulti 保存多个对象
func (e *Engine) InsertMulti(models ...interface{}) error {
	s := e.StartSession()
	defer e.EndSession(s)
	return e.InsertMulti(models...)
}

// InsertSlice 保存数组
func (e *Engine) InsertSlice(slice interface{}) error {
	s := e.StartSession()
	defer e.EndSession(s)
	return e.InsertSlice(slice)
}

// UpdateByPK 按主键修改
func (e *Engine) UpdateByPK(pk, model interface{}, cols ...string) error {
	s := e.StartSession()
	defer e.EndSession(s)
	if len(cols) >= 0 {
		s = s.Cols(strings.Join(cols, ","))
	}
	_, err := s.Id(pk).Update(model)
	return err
}

// Delete 按主键删除记录
func (e *Engine) Delete(pk, model interface{}) (int64, error) {
	s := e.StartSession()
	defer e.EndSession(s)
	return s.Id(pk).Delete(model)
}

// GetTableName 获取实体对应的数据表名称
func GetTableName(model interface{}) string {
	meta, _ := getTableMeta(model)
	return meta.Name
}
