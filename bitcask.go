package bitcask

import (
	"fmt"
	"golang.org/x/xerrors"
	"io"
	"os"
	"strings"

	"github.com/nicolerobin/bitcask/errors"
	"github.com/nicolerobin/log"
)

type Db struct {
	path string
	file *os.File
	kvs  map[string]string
}

// Open open a db
func Open(path string) (*Db, error) {
	// # TODO: use file lock to avoid db file open by more than one process
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Error("os.OpenFile() failed, err:%s", err)
		return nil, err
	}

	if err != nil {
		log.Error("file.WriteString() failed, err:%s", err)
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		log.Error("file.Stat() failed, err:%s", err)
		return nil, err
	}
	log.Debug("fileInfo:%+v", fileInfo)

	kvs := make(map[string]string)
	buf := make([]byte, fileInfo.Size())
	n, err := file.Read(buf)
	if err != nil {
		if !xerrors.Is(err, io.EOF) {
			log.Error("file.Read() failed, err:%s", err)
			return nil, err
		}
	}
	log.Debug("file.Read() success, n:%d", n)
	pairs := strings.Split(string(buf), "\n")
	for _, pair := range pairs {
		if len(pair) == 0 {
			continue
		}

		items := strings.Split(pair, ",")
		if len(items) == 2 {
			kvs[items[0]] = kvs[items[1]]
		} else {
			log.Warn("unexpected pair:%s", pair)
		}
	}

	return &Db{
		path: path,
		file: file,
		kvs:  kvs,
	}, nil
}

func (db *Db) Set(key, val string) error {
	db.kvs[key] = val
	return nil
}

func (db *Db) Get(key string) (string, error) {
	if val, ok := db.kvs[key]; ok {
		return val, nil
	}
	return "", errors.ErrNotFound
}

func (db *Db) Delete(key string) {
	delete(db.kvs, key)
}

func (db *Db) Close() error {
	for k, v := range db.kvs {
		n, err := db.file.WriteString(fmt.Sprintf("%s,%s\n", k, v))
		if err != nil {
			log.Error("db.file.WriteString() failed, err:%s", err)
			return err
		}
		log.Debug("db.file.WriteString() success, n:%d", n)
	}
	return db.file.Close()
}
