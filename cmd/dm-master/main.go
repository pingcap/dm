package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/dm/dm/master"
	"github.com/pingcap/dm/pkg/utils"
)

func main() {
	cfg := master.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)

		if cfg.LogRotate == "day" {
			log.SetRotateByDay()
		} else {
			log.SetRotateByHour()
		}
	}

	utils.PrintInfo("dm-master", func() {
		log.Infof("config: %s", cfg)
	})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	server := master.NewServer(cfg)

	go func() {
		sig := <-sc
		log.Infof("got signal [%v], exit", sig)
		server.Close()
	}()

	err = server.Start()
	if err != nil {
		log.Errorf("dm-master start with error %v", errors.ErrorStack(err))
	}
	server.Close()

	log.Info("dm-master exit")
}
