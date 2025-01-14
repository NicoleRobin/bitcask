package bitcask

type Entry struct {
	Crc       [32]byte
	Timestamp int32
	KeySize   int32
	ValueSize int32
	Key       []byte
	Value     []byte
}
