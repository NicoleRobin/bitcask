package bitcask

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nicolerobin/bitcask/errors"
	"github.com/nicolerobin/zrpc/log"
	"golang.org/x/xerrors"
)

type Db struct {
	path string
	file *os.File
	kvs  map[string]string
}

// Open open a db
func Open(path string) (*Db, error) {
	ctx := context.Background()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile() failed, err:%w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("file.Stat() failed, err:%w", err)
	}

	kvs := make(map[string]string)
	buf := make([]byte, fileInfo.Size())
	_, err = file.Read(buf)
	if err != nil {
		if !xerrors.Is(err, io.EOF) {
			return nil, fmt.Errorf("file.Read() failed, err:%w", err)
		}
	}
	pairs := strings.Split(string(buf), "\n")
	for _, pair := range pairs {
		if len(pair) == 0 {
			continue
		}

		items := strings.Split(pair, ",")
		if len(items) == 2 {
			kvs[items[0]] = kvs[items[1]]
		} else {
			log.Warnf(ctx, "unexpected pair:%s", pair)
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
		_, err := db.file.WriteString(fmt.Sprintf("%s,%s\n", k, v))
		if err != nil {
			return fmt.Errorf("db.file.WriteString() failed, err:%w", err)
		}
	}
	return db.file.Close()
}
