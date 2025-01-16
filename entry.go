package bitcask

type indexEntry struct {
	fileID int
	offset int64
	size   int64
}
