package main_test

import (
	"testing"
	"bytes"
	"io"
	"gostore/comm"
	"gostore/services/fs"
	"gostore/tools/buffer"
	"net"
	"time"
	"gostore/log"
)

/*
	[ ] Timeout network (IP)
	[ ] Timeout machine (destination too slow)
	[ ] Connection break
	[ ] Flapping
	[ ] Test multiple break points (check all connexions made by each method)
	[ ] Test timeout fallback (get another node!)
*/

func timeoutTest(delay int, cmdcb func(), timeoutcb func(returned bool)) {
	var returned = false
	go func() {
		time.Sleep(int64(delay) * 1000 * 1000)
		timeoutcb(returned)
	}()

	cmdcb()
	returned = true
}

func TestFsReadNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsReadNetworkTimeout...")

	bufwriter := bytes.NewBuffer(make([]byte, 0))
	buffer := io.Writer(bufwriter)

	resp, other := GetProcessForPath("/")

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		_, err := other.Fss.Read(fs.NewPath("/"), 0, -1, 0, buffer, nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Read network timeout is endlessly sleeping")
			}
		})

	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		_, err := other.Fss.Read(fs.NewPath("/"), 0, -1, 0, buffer, nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Read network timeout is endlessly sleeping")
			}
		})
}

func TestFsExistsNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsExistsNetworkTimeout...")

	resp, other := GetProcessForPath("/")

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		_, err := other.Fss.Exists(fs.NewPath("/"), nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Exists network timeout is endlessly sleeping")
			}
		})

	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		_, err := other.Fss.Exists(fs.NewPath("/"), nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Exists network timeout is endlessly sleeping")
			}
		})
}

func TestFsWriteNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsWriteNetworkTimeout...")

	resp, other := GetProcessForPath("/")

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		buf := buffer.NewFromString("write1")
		err := other.Fss.Write(fs.NewPath("/"), buf.Size, "", buf, nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Write network timeout is endlessly sleeping")
			}
		})

	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		buf := buffer.NewFromString("write1")
		err := other.Fss.Write(fs.NewPath("/"), buf.Size, "", buf, nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Write network timeout is endlessly sleeping")
			}
		})
}

func TestFsDeleteNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsDeleteNetworkTimeout...")

	resp, other := GetProcessForPath("/tests/timeouts/delete1")
	buf := buffer.NewFromString("write1")
	other.Fss.Write(fs.NewPath("/tests/timeouts/delete1"), buf.Size, "", buf, nil)

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		buf = buffer.NewFromString("write1")
		err := other.Fss.Delete(fs.NewPath("/tests/timeouts/delete1"), false, nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Write network timeout is endlessly sleeping")
			}
		})

	resp, other = GetProcessForPath("/tests/timeouts/delete2")
	buf = buffer.NewFromString("write1")
	other.Fss.Write(fs.NewPath("/tests/timeouts/delete2"), buf.Size, "", buf, nil)

	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		buf = buffer.NewFromString("write1")
		err := other.Fss.Write(fs.NewPath("/tests/timeouts/delete2"), buf.Size, "", buf, nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Write network timeout is endlessly sleeping")
			}
		})
}

func TestFsHeaderNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsHeaderNetworkTimeout...")

	time.Sleep(500000000)

	resp, other := GetProcessForPath("/")

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		_, err := other.Fss.Header(fs.NewPath("/"), nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Write network timeout is endlessly sleeping")
			}
		})


	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		_, err := other.Fss.Header(fs.NewPath("/"), nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Write network timeout is endlessly sleeping")
			}
		})
}

func TestFsHeaderJSONNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsHeaderJSONNetworkTimeout...")

	resp, other := GetProcessForPath("/")

	initadr := other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address
	other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Test full timeout and return of an error
	timeoutTest(1500*4, func() {
		_, err := other.Fss.HeaderJSON(fs.NewPath("/"), nil)
		if err != comm.ErrorTimeout {
			t.Error("1) Didn't receive timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("2) Write network timeout is endlessly sleeping")
			}
		})

	// Test one timeout + 1 retry, no error
	timeoutTest(1500*4, func() {
		// wait 50ms and change addr of the remote node
		go func() {
			time.Sleep(500000000)
			other.Cluster.Nodes.Get(resp.Cluster.MyNode.Id).Address = initadr
		}()

		_, err := other.Fss.HeaderJSON(fs.NewPath("/"), nil)
		if err == comm.ErrorTimeout {
			t.Error("3) Received timeout error: %s", err)
		}
	},
		func(returned bool) {
			if !returned {
				t.Error("4) Write network timeout is endlessly sleeping")
			}
		})
}

func TestFsChildAddWriteNeedNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsChildAddWriteNeedNetworkTimeout...")

	resp2, other2 := GetProcessForPath("/tests/timeout2/write")
	resp1, other1 := GetProcessForPath("/tests/timeout2")

	initadr := resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address
	resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	// Create the child, it should never get added to the parent
	buf := buffer.NewFromString("write1")
	err := other2.Fss.Write(fs.NewPath("/tests/timeout2/write"), buf.Size, "", buf, nil)
	if err != nil && err != comm.ErrorTimeout {
		t.Error("1) Received an error: %s", err)
	}
	time.Sleep(900000000)
	children, _ := other1.Fss.Children(fs.NewPath("/tests/timeout2"), nil)
	found := false
	for _, child := range children {
		if child.Name == "write" {
			found = true
		}
	}
	if found {
		t.Error("2) Child should not have been added")
	}

	// Test one timeout + 1 retry, no error
	buf = buffer.NewFromString("write1")

	go func() {
		time.Sleep(1500 * 1000 * 1000)
		// set the address back
		resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = initadr
	}()

	err = other2.Fss.Write(fs.NewPath("/tests/timeout2/write3"), buf.Size, "", buf, nil)
	if err != nil {
		t.Error("3) Received an error: %s", err)
	}

	children, err = other1.Fss.Children(fs.NewPath("/tests/timeout2"), nil)
	found = false
	for _, child := range children {
		if child.Name == "write3" {
			found = true
		}
	}
	if !found {
		t.Error("4) Child should have been added")
	}
}

func TestFsChildRemoveDeleteNetworkTimeout(t *testing.T) {
	SetupCluster()
	log.Info("Testing TestFsChildRemoveDeleteNetworkTimeout...")

	resp3, other3 := GetProcessForPath("/test/timeout/delete1", "/test/timeout")
	resp2, other2 := GetProcessForPath("/test/timeout/delete2", "/test/timeout", "/test/timeout/delete2")
	resp1, other1 := GetProcessForPath("/test/timeout")

	buf := buffer.NewFromString("write1")
	resp3.Fss.Write(fs.NewPath("/test/timeout/delete1"), buf.Size, "", buf, nil)
	buf = buffer.NewFromString("write1")
	resp3.Fss.Write(fs.NewPath("/test/timeout/delete2"), buf.Size, "", buf, nil)

	initadr := resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address
	resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")
	resp3.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = net.ParseIP("224.0.0.2")

	time.Sleep(900000000)
	// Delete child, it should never get delete from the parent
	err := other2.Fss.Delete(fs.NewPath("/test/timeout/delete1"), false, nil)
	if err != nil {
		t.Error("1) Received an error: %s", err)
	}
	time.Sleep(900000000)
	children, _ := other1.Fss.Children(fs.NewPath("/test/timeout"), nil)
	found := false
	for _, child := range children {
		if child.Name == "delete1" {
			found = true
		}
	}
	if !found {
		t.Error("2) Child should not have been deleted")
	}

	// Test timeout of the first and write of a new one
	err = other3.Fss.Delete(fs.NewPath("/test/timeout/delete2"), false, nil)
	if err != nil {
		t.Error("3) Received an error: %s", err)
	}

	// Set back the address
	resp2.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = initadr
	resp3.Cluster.Nodes.Get(resp1.Cluster.MyNode.Id).Address = initadr

	time.Sleep(6000000000)
	children, _ = other1.Fss.Children(fs.NewPath("/test/timeout"), nil)
	found1, found2 := false, false
	for _, child := range children {
		if child.Name == "delete2" {
			found2 = true
		} else if child.Name == "delete1" {
			found1 = true
		}
	}
	if found1 || found2 {
		t.Error("4) At least one of the children is still there", found1, found2)
	}

}
