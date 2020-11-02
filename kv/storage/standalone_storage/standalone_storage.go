package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kve := engine_util.CreateDB(conf.DBPath, conf.Raft)
	sas := &StandAloneStorage{
		engine: engine_util.NewEngines(kve, nil, conf.DBPath, ""),
	}
	return sas
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	sr := &StorageReader{
		txn: txn,
	}
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, data.Cf, data.Key, data.Value)
			if err != nil {
				//
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, data.Cf, data.Key)
			if err != nil {
				//
				return err
			}
		}
	}
	return nil
}

type StorageReader struct {
	txn *badger.Txn
}

func (s *StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *StorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, s.txn)
	iter.Rewind()
	return iter
}

func (s *StorageReader) Close() {
	s.txn.Discard()
}




