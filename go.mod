module go.withmatt.com/connect-etcd

go 1.20

require (
	github.com/bufbuild/connect-go v1.9.0
	go.uber.org/zap v1.24.0
	golang.org/x/net v0.12.0
	google.golang.org/protobuf v1.31.0
)

require (
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/text v0.11.0 // indirect
)

//replace github.com/bufbuild/connect-go => ../connect-go

//replace golang.org/x/net => ../golang/net
