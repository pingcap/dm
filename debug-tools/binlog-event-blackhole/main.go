package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
)

func main() {
	cfg := newConfig()
	err := cfg.parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Printf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	err = log.InitLogger(&log.Config{
		File:  cfg.logFile,
		Level: strings.ToLower(cfg.logLevel),
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(2)
	}

	conn, err := registerSlave(cfg.addr, cfg.username, cfg.password, uint32(cfg.serverID))
	if err != nil {
		log.L().Error("register slave", zap.Error(err))
		os.Exit(2)
	}
	log.L().Info("registered slave", zap.Uint32("connection ID", conn.GetConnectionID()))

	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		cancel()
		log.L().Info("got signal to exit", zap.Stringer("signal", sig))
		err2 := closeConn(conn)
		if err2 != nil {
			log.L().Error("close connection", zap.Error(err2))
		}
	}()

	err = startSync(conn, uint32(cfg.serverID), cfg.binlogName, uint32(cfg.binlogPos))
	if err != nil {
		log.L().Error("start sync", zap.Error(err))
		os.Exit(2)
	}
	log.L().Info("start sync",
		zap.Int("server-id", cfg.serverID), zap.String("binlog-name", cfg.binlogName),
		zap.Int("binlog-pos", cfg.binlogPos))

	count, duration, err := readEventsWithGoMySQL(ctx, conn)
	if err != nil {
		log.L().Error("read events", zap.Error(err))
	}

	tps := float64(count) / duration.Seconds()
	log.L().Info("binlog-event-blackhole exit", zap.Uint64("count", count),
		zap.Duration("duration", duration), zap.Float64("tps", tps))
}
