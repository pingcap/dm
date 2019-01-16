package main

import (
	"context"
	"os"

	"github.com/pingcap/tidb-enterprise-tools/tests2/utils"
)

func main() {
	cli, err := utils.CreateDmCtl("127.0.0.1:8261")
	if err != nil {
		utils.ExitWithError(err)
	}
	conf := os.Args[1]
	err = utils.StartTask(context.Background(), cli, conf, nil)
	if err != nil {
		utils.ExitWithError(err)
	}
}
