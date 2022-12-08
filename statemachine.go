package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

type ExStateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
}

func NewStateMachine(clusterId uint64, nodeId uint64) sm.IStateMachine {
	return &ExStateMachine{
		ClusterID: clusterId,
		NodeID:    nodeId,
		Count:     0,
	}
}

func (s *ExStateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result, nil
}

func (s *ExStateMachine) Update(data []byte) (sm.Result, error) {
	s.Count++
	fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count:%d\n", string(data), s.Count)
	return sm.Result{Value: uint64(len(data))}, nil
}

func (s *ExStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, s.Count)
	_, err := w.Write(data)
	return err
}

func (s *ExStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint64(data)
	s.Count = v
	return nil
}

func (s *ExStateMachine) Close() error { return nil }
