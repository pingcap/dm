package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := newConfig()
	err := cfg.parse(os.Args[1:])
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sc
		cancel()
	}()

	conn, err := registerSlave(cfg.addr, cfg.username, cfg.password, uint32(cfg.serverID))
	if err != nil {
		panic(err)
	}
	defer func() {
		killConn(conn)
		conn.Close()
	}()

	fmt.Printf("registered slave as %d\n", conn.GetConnectionID())

	err = startSync(conn, uint32(cfg.serverID), cfg.binlogName, uint32(cfg.binlogPos))
	if err != nil {
		panic(err)
	}

	fmt.Println("okok")

	err = readEventsWithGoMySQL(ctx, conn)
	if err != nil {
		panic(err)
	}

	fmt.Println("haha")
}
