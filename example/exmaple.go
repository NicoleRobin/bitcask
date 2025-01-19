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

	db, err := bitcask.NewBitCask("./bitcask")
	if err != nil {
		log.Error(ctx, "bitcask.NewBitCask() failed", zap.Error(err))
		return
	}
	defer db.Close()

	err = db.Set(ctx, key, val)
	if err != nil {
		log.Error(ctx, "db.Set() failed", zap.Error(err))
		return
	}

	v, err := db.Get(ctx, key)
	if err != nil {
		log.Error(ctx, "db.Get() failed", zap.String("key", key), zap.Error(err))
		return
	}
	log.Info(ctx, "db.Get() success", zap.String("key", key), zap.String("value", v))
	err = db.Close()
	if err != nil {
		log.Error(ctx, "db.Close() failed", zap.Error(err))
		return
	}
}
