package watcher

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testWatcherSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testWatcherSuite struct {
}

func (t *testWatcherSuite) TestWatcher(c *C) {
	var (
		oldFileName = "mysql-bin.000001"
		newFileName = "mysql-bin.000002"
		oldFilePath = ""
		newFilePath = ""
		wg          sync.WaitGroup
	)

	// create dir
	dir, err := ioutil.TempDir("", "test_watcher")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	// join the path
	oldFilePath = filepath.Join(dir, oldFileName)
	newFilePath = filepath.Join(dir, newFileName)

	// create watcher
	w := NewWatcher()

	// watch directory
	err = w.Add(dir)
	c.Assert(err, IsNil)

	// start watcher
	err = w.Start(10 * time.Millisecond)
	c.Assert(err, IsNil)
	defer w.Close()

	// create file
	f, err := os.Create(oldFilePath)
	c.Assert(err, IsNil)
	f.Close()

	// watch for create
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Create, c)
	}()
	wg.Wait()

	// watch for write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Modify, c)
	}()

	f, err = os.OpenFile(oldFilePath, os.O_WRONLY, 0766)
	c.Assert(err, IsNil)
	f.Write([]byte("meaningless content"))
	f.Close()
	wg.Wait()

	// watch for chmod
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Chmod, c)
	}()

	err = os.Chmod(oldFilePath, 0777)
	c.Assert(err, IsNil)
	wg.Wait()

	// watch for rename
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Rename, c)
	}()

	err = os.Rename(oldFilePath, newFilePath)
	c.Assert(err, IsNil)
	wg.Wait()

	// watch for remove
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, newFilePath, Remove, c)
	}()

	err = os.Remove(newFilePath)
	c.Assert(err, IsNil)
	wg.Wait()

	// watch for create again
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Create, c)
	}()

	// create file again
	f, err = os.Create(oldFilePath)
	c.Assert(err, IsNil)
	f.Close()
	wg.Wait()

	// create another dir
	dir2, err := ioutil.TempDir("", "test_watcher")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir2)
	oldFilePath2 := filepath.Join(dir2, oldFileName)

	// add another directory for watching
	err = w.Add(dir2)
	c.Assert(err, IsNil)

	// watch for move (rename to another directory)
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Move, c)
	}()

	err = os.Rename(oldFilePath, oldFilePath2)
	c.Assert(err, IsNil)
	wg.Wait()
}

func assertEvent(w *Watcher, path string, op Op, c *C) {
	for {
		select {
		case ev := <-w.Events:
			if ev.IsDirEvent() {
				continue // skip event for directory
			}
			c.Assert(ev.HasOps(op), IsTrue)
			c.Assert(ev.Path, Equals, path)
			return
		case err2 := <-w.Errors:
			c.Fatal(err2)
			return
		}
	}
}
