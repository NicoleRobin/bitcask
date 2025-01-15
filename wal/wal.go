package wal

import "context"

type Wal struct {
	path string
}

func Open(ctx context.Context, path string) (*Wal, error) {
	wal := &Wal{
		path: path,
	}

	return wal, nil
}

func (w *Wal) Write(ctx context.Context, data []byte) (int64, error) {

	return 0, nil
}
