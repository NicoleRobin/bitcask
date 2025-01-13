package main

import (
	"context"

	"github.com/nicolerobin/bitcask"
	"github.com/nicolerobin/zrpc/log"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	key := "key"
	val := "value01"

	db, err := bitcask.Open("./test.db")
	if err != nil {
		log.Error(ctx, "bitcask.Open() failed", zap.Error(err))
	}

	err = db.Set(key, val)
	if err != nil {
		log.Error(ctx, "db.Set() failed", zap.Error(err))
	}

	v, err := db.Get(key)
	if err != nil {
		log.Error(ctx, "db.Get() failed", zap.String("key", key), zap.Error(err))
	}
	log.Info(ctx, "db.Get() success", zap.String("key", key), zap.String("value", v))
	err = db.Close()
	if err != nil {
		log.Error(ctx, "db.Close() failed", zap.Error(err))
	}
}
