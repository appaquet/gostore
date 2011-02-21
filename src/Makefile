all: install

DIRS=	pkg\
	cmd/gostore-server\
	tests/fs\
	tests/cls\


NOTEST=	server\
		
NOBENCH=

TEST=\
	$(filter-out $(NOTEST),$(DIRS))

BENCH=\
	$(filter-out $(NOBENCH),$(TEST))


clean.dirs: $(addsuffix .clean, $(DIRS))
install.dirs: $(addsuffix .install, $(DIRS))
test.dirs: $(addsuffix .test, $(TEST))
bench.dirs: $(addsuffix .bench, $(BENCH))

%.clean:
	rm -rf $${GOROOT}/pkg/darwin_amd64/gostore
	rm -rf tests/fs/data/*
	+cd $* && gomake clean

%.install:                                                                                                                          
	+cd $* && gomake install

%.test:
	+cd $* && gomake test

%.bench:                                                                                                                          
	+cd $* && gomake bench
    
    
install: install.dirs

clean: clean.dirs

test: test.dirs

bench: bench.dirs
