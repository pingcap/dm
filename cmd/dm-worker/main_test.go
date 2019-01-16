package main

// Reference: https://dzone.com/articles/measuring-integration-test-coverage-rate-in-pouchc

import (
	"os"
	"strings"
	"testing"
)

func TestRunMain(t *testing.T) {
	var args []string
	for _, arg := range os.Args {
		switch {
		case arg == "DEVEL":
		case strings.HasPrefix(arg, "-test."):
		default:
			args = append(args, arg)
		}
	}

	waitCh := make(chan struct{}, 1)

	os.Args = args
	go func() {
		main()
		close(waitCh)
	}()

	<-waitCh
}
