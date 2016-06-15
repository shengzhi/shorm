// Copyright 2016 The shorm Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//Db transaction

package shorm

import "database/sql"

type DbTrans struct {
	engine  *Engine
	tx      *sql.Tx
	session *Session
}

func newDbTrans(e *Engine, shardValue int64) (*DbTrans, error) {
	trans := &DbTrans{engine: e, session: e.StartSession()}
	group, has := e.cluster.findGroup(shardValue)
	if !has {
		group, _ = e.cluster.DefaultGroup()
	}
	node, _ := group.GetMaster()
	var err error
	trans.tx, err = node.Db.Begin()
	if err != nil {
		if trans.tx != nil {
			trans.tx.Rollback()
		}
		return nil, err
	}
	return trans, nil
}

func (d *DbTrans) Commit() error {
	d.engine.EndSession(d.session)
	return d.tx.Commit()
}

func (d *DbTrans) Rollback() error {
	d.engine.EndSession(d.session)
	return d.tx.Rollback()
}

func (d *DbTrans) Insert(model interface{}) error {
	return d.session.insertWithTx(d.tx, model)
}
