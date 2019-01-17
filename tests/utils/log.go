package utils

import (
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func ExitWithError(err error) {
	log.Error(errors.ErrorStack(err))
	os.Exit(1)
}
