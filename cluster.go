// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Defined cluster, dbgroup, table, column models etc...

package shorm

import (
	"bytes"
	"database/sql"
	"fmt"
)

//Sharding interface
type Shardinger interface {
	GetShardValue() int64
}

//Cluster that includes one or more db groups
type Cluster struct {
	TotalGroups   int        `json:"total_groups"`
	RealGroups    int        `json:"-"`
	Groups        []*DbGroup `json:"groups" xml:"Groups>Group"`
	_defaultGroup *DbGroup
}

//Open all db nodes
func (c *Cluster) Open(driver string) (err error) {
Loop:
	for _, g := range c.Groups {
		for _, n := range g.Nodes {
			err = n.open(driver)
			if err != nil {
				break Loop
			}
		}
	}
	return
}

//Close all db nodes
func (c *Cluster) Close() (err error) {
	var buf bytes.Buffer
	for _, g := range c.Groups {
		for _, n := range g.Nodes {
			if n.Db != nil {
				if err = n.Db.Close(); err != nil {
					buf.WriteString(fmt.Sprintf("try to close database %s occurs error:%v\r\n", n.Name, err))
				}
			}
		}
	}
	if err != nil {
		return fmt.Errorf(buf.String())
	}
	return nil
}

func (c *Cluster) has1DbGroup() bool {
	return len(c.Groups) == 1
}

//Find db group according to specified shardkey
func (c *Cluster) findGroup(shardKey int64) (*DbGroup, bool) {
	if c.RealGroups == 1 {
		return c.Groups[0], true
	}
	mod := shardKey%int64(c.TotalGroups)
	for _, g := range c.Groups {
		if g.in(mod) {
			return g, true
		}
	}
	return nil, false
}

func (c *Cluster) DefaultGroup() (*DbGroup, error) {
	if c._defaultGroup != nil {
		return c._defaultGroup, nil
	}
	if c.has1DbGroup() {
		c._defaultGroup = c.Groups[0]
		return c.Groups[0], nil
	}
	for _, m := range c.Groups {
		if m.IsDefault {
			c._defaultGroup = m
			return m, nil
		}
	}
	return nil, fmt.Errorf("Not specified defaut db group")
}

//DbGroup that includes one master and 0 or more salve nodes
//provides support for high availability and high performance
type DbGroup struct {
	Name      string    `json:"name"`
	RangeFrom int64     `json:"range_from"`
	RangeTo   int64     `json:"range_to"`
	Nodes     []*DbNode `json:"nodes" xml:"Nodes>Node"`
	master    *DbNode
	slaves    []*DbNode
	circle    int
	IsDefault bool `json:"is_default"`
}

func (d *DbGroup) in(mod int64) bool {
	return d.RangeFrom <= mod && mod < d.RangeTo
}

func (d *DbGroup) GetMaster() (*DbNode, error) {
	if d.master != nil {
		return d.master, nil
	}
	if len(d.Nodes) == 1 {
		d.master = d.Nodes[0]
		return d.master, nil
	}
	for _, n := range d.Nodes {
		if n.Type == NodeType_Master {
			d.master = n
			return n, nil
		}
	}
	return nil, fmt.Errorf("Group %s has no master node", d.Name)
}

func (d *DbGroup) GetNode() *DbNode {
	if len(d.Nodes) == 1 {
		return d.Nodes[0]
	}
	if len(d.slaves) <= 0 {
		for _, n := range d.Nodes {
			if n.Type == NodeType_Slave {
				d.slaves = append(d.slaves, n)
			}
		}
	}
	if len(d.slaves) == 1 {
		return d.slaves[0]
	}
	index := d.circle % len(d.slaves)
	d.circle++
	return d.slaves[index]
}

//DbNode is a db instance on physical or virtual machine
type DbNode struct {
	Name    string   `json:"name"`
	Db      *sql.DB  `json:"-"`
	ConnStr string   `json:"conn_string"`
	Type    NodeType `json:"node_type" xml:"NodeType"` //Indicates the node is master or salve node
	Weight  int8     `json:"weight"`                   //Weight for slave node, if IsMaster=true, it is ignored
}

func (d *DbNode) open(driver string) (err error) {
	d.Db, err = sql.Open(driver, d.ConnStr)
	return err
}

func (d *DbNode) ping() error {
	return d.Db.Ping()
}

type NodeType string

const (
	NodeType_Master = "master"
	NodeType_Slave  = "slave"
)
