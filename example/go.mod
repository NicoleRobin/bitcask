module example

go 1.23.4

require (
	github.com/nicolerobin/bitcask v0.0.0-20230413161658-5a7657894dea
	github.com/nicolerobin/zrpc v0.0.0-20250119154810-a80296023c7b
	go.uber.org/zap v1.27.0
)

replace github.com/nicolerobin/bitcask => ../

require go.uber.org/multierr v1.10.0 // indirect
