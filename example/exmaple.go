package main

import (
	"github.com/nicolerobin/bitcask"
	"github.com/nicolerobin/log"
)

func main() {
	key := "key"
	val := "value01"

	db, err := bitcask.Open("./test.db")
	if err != nil {
		log.Error("bitcask.Open() failed, err:%s", err)
	}

	err = db.Set(key, val)
	if err != nil {
		log.Error("db.Set() failed, err:%s", err)
	}

	v, err := db.Get(key)
	if err != nil {
		log.Error("db.Get() failed, key:%s, err:%s", key, err)
	}
	log.Debug("db.Get() success, key:%s, val:%s", key, v)
	err = db.Close()
	if err != nil {
		log.Error("db.Close() failed, err:%s", err)
	}
}
