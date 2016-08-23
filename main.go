package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/kayako/beanstalk-broker/broker"
	"github.com/kayako/beanstalk-broker/cli"
)

func main() {
	opts := cli.MustParseFlags()

	bd := broker.NewBrokerDispatcher(opts)

	if opts.All {
		bd.RunAllTubes()
	} else {
		bd.RunTubes(opts.Tubes)
	}

	handleShutdown(bd.Shutdown)
	bd.Wait()
}

// handleShutdown registers a listener for signals and
// executes the handler when a signal is trapped
func handleShutdown(handle func()) {
	sh := make(chan os.Signal)
	signal.Notify(sh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
	go func(s chan os.Signal) {
		<-s
		handle()
	}(sh)
}
