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
	db *badger.DB
	//eng  *engine_util.Engines
	//conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//return nil
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{db}

	//kvPath := path.Join(conf.DBPath, "kv")
	//raftPath := path.Join(conf.DBPath, "raft")
	//kvEngine := engine_util.CreateDB(kvPath, false)
	//raftEngine := engine_util.CreateDB(raftPath, true)
	//e := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)
	//return &StandAloneStorage{
	//	eng:  e,
	//	conf: conf,
	//}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//return s.db.Load(io.Reader())
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//return nil
	//return s.eng.Close()
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	//return &Readers{s.eng.Kv.NewTransaction(false)}, nil
	return &Readers{s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, m := range batch {

		switch m.Data.(type) {

		//case storage.Put:
		//	engine_util.PutCF(s.eng.Kv, m.Cf(), m.Key(), m.Value())
		//case storage.Delete:
		//	engine_util.DeleteCF(s.eng.Kv, m.Cf(), m.Key())
		case storage.Put:
			engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			engine_util.DeleteCF(s.db, m.Cf(), m.Key())

		}
	}

	return nil
}

type Readers struct {
	d *badger.Txn
}

func (r *Readers) GetCF(cf string, key []byte) ([]byte, error) {

	txn, err := engine_util.GetCFFromTxn(r.d, cf, key)
	if err != nil {
		return txn, nil
	}
	return txn, err
}

func (r *Readers) IterCF(cf string) engine_util.DBIterator {

	return engine_util.NewCFIterator(cf, r.d)
}

func (r *Readers) Close() {
	r.d.Discard()
}
