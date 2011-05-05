# Copyright 2009 The Go Authors.  All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=gostore/services/db

GOFILES=db.go\
	operations.go\
	container.go\
	transaction.go\
	transaction.pb.go\
	segment.go\
	viewstate.go\

include $(GOROOT)/src/pkg/goprotobuf.googlecode.com/hg/Make.protobuf
include $(GOROOT)/src/Make.pkg
