for I in `find . -name "*.go"`
do
gofmt -w=true $I
done
