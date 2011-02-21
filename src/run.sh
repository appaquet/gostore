./format.sh
rm -rf $GO_ROOT/pkg/linux_386/gostore
make clean
make
cd cmd/gostore-server

export GOMAXPROCS=4000
./gostore-server -verbosity=10
