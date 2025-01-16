package bitcask

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/nicolerobin/bitcask/errors"
)

type BitCask struct {
	directory  string
	index      map[string]indexEntry
	activeFile *os.File
	indexFile  *os.File
	mu         sync.Mutex
	kvs        map[string]string
}

func NewBitCask(directory string) (*BitCask, error) {
	bitCask := &BitCask{
		directory: directory,
		index:     make(map[string]indexEntry),
	}
	if err := bitCask.loadIndex(); err != nil {
		return nil, err
	}
	return bitCask, nil
}

func (b *BitCask) loadIndex() error {
	file, err := os.OpenFile(b.getIndexFilePath(), os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	for {
		key, offset, size, err := b.readIndexEntry(file)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		b.index[key] = indexEntry{fileID: 0, offset: offset, size: size}
	}
	return nil
}

func (b *BitCask) getIndexFilePath() string {
	return path.Join(b.directory, IndexFileName)
}

func (b *BitCask) getActiveFilePath() string {
	return path.Join(b.directory, DataFileName)
}

func (b *BitCask) readIndexEntry(file *os.File) (string, int64, int64, error) {
	var keyLen int32
	if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
		return "", 0, 0, err
	}
	keyBytes := make([]byte, keyLen)
	if _, err := file.Read(keyBytes); err != nil {
		return "", 0, 0, err
	}
	var fileID, offset, size int64
	if err := binary.Read(file, binary.BigEndian, &fileID); err != nil {
		return "", 0, 0, err
	}
	if err := binary.Read(file, binary.BigEndian, &offset); err != nil {
		return "", 0, 0, err
	}
	if err := binary.Read(file, binary.BigEndian, &size); err != nil {
		return "", 0, 0, err
	}
	return string(keyBytes), offset, size, nil
}

func (b *BitCask) writeIndexEntry(ctx context.Context, key string, fileID int, offset, size int64) error {
	file, err := os.OpenFile(b.getIndexFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	keyBytes := []byte(key)
	keyLen := int32(len(keyBytes))
	binary.Write(file, binary.BigEndian, keyLen)
	file.Write(keyBytes)
	binary.Write(file, binary.BigEndian, int64(fileID))
	binary.Write(file, binary.BigEndian, offset)
	binary.Write(file, binary.BigEndian, size)
	return nil
}

func (b *BitCask) Set(ctx context.Context, key, value string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.activeFile == nil {
		var err error
		b.activeFile, err = os.OpenFile(b.getActiveFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}
	timestamp := time.Now().UnixNano()
	entry := struct {
		Timestamp int64
		KeyLen    int32
		ValueLen  int32
	}{
		Timestamp: timestamp,
		KeyLen:    int32(len(key)),
		ValueLen:  int32(len(value)),
	}
	if err := binary.Write(b.activeFile, binary.BigEndian, entry); err != nil {
		return err
	}
	if _, err := b.activeFile.Write([]byte(key)); err != nil {
		return err
	}
	if _, err := b.activeFile.Write([]byte(value)); err != nil {
		return err
	}
	offset, err := b.activeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("file.Seek() failed, err:%w", err)
	}

	offset = offset - int64(binary.Size(entry)) - int64(len(key)) - int64(len(value))
	size := int64(binary.Size(entry)) + int64(len(key)) + int64(len(value))
	b.index[key] = indexEntry{fileID: 0, offset: offset, size: size}
	return b.writeIndexEntry(ctx, key, 0, offset, size)
}

func (b *BitCask) Get(ctx context.Context, key string) (string, error) {
	if _, ok := b.index[key]; !ok {
		return "", errors.ErrNotFound
	}

	return "", nil
}

func (b *BitCask) Delete(ctx context.Context, key string) {
}

func (b *BitCask) Close() error {
	return nil
}
