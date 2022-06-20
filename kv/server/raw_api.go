package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	b, _ := reader.GetCF(req.Cf, req.Key)
	return &kvrpcpb.RawGetResponse{
		Value:    b,
		NotFound: true,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	put := storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}

	modify := storage.Modify{Data: put}

	err := server.storage.Write(req.Context, []storage.Modify{modify})

	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	delete := storage.Delete{Cf: req.Cf, Key: req.Key}
	modify := storage.Modify{Data: delete}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, _ := server.storage.Reader(req.Context)

	iterator := reader.IterCF(req.Cf)

	defer reader.Close()
	defer iterator.Close()
	limit := req.Limit

	kvs := []*kvrpcpb.KvPair{}
	for iterator.Seek(req.StartKey); iterator.Valid() && limit > 0; iterator.Next() {
		//item := iterator.Item()

		//value, _ := item.Value()

		value, _ := iterator.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{Key: iterator.Item().Key(), Value: value})
		limit--
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
