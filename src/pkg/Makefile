all: install

DIRS=\
	gostore/log\
	gostore/tools/typedio\
	gostore/tools/buffer\
	gostore/tools/commitlog\
	gostore/tools/hashring\
	gostore\
	gostore/cluster\
	gostore/comm\
	gostore/api/rest\
	gostore/services/cls\
	gostore/services/db/embdb\
	gostore/services/fs\
	gostore/process\
	
NOTEST=\
	gostore/log\
	gostore/tools/typedio\
	gostore/comm\
	gostore/api/rest\
	gostore/cluster\
	gostore/services/cls\
	gostore/services/fs\
	gostore/process\
	gostore\
	
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

-include Make.deps
