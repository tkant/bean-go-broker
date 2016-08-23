package broker

import (
	"sync"
	"time"

	"github.com/kayako/beanstalk-broker/cli"
	"github.com/kr/beanstalk"
	log "github.com/sirupsen/logrus"
)

const (
	// ListTubeDelay is the time between sending list-tube to beanstalkd
	// to discover and watch newly created tubes.
	ListTubeDelay = 10 * time.Second
)

// BrokerDispatcher manages the running of Broker instances for tubes.  It can
// be manually told tubes to start, or it can poll for tubes as they are
// created. The `perTube` option determines how many brokers are started for
// each tube.
type BrokerDispatcher struct {
	address string
	conn    *beanstalk.Conn
	perTube uint64
	tubeSet map[string]bool
	options cli.Options
	sync.WaitGroup
	ret chan bool
}

func NewBrokerDispatcher(o cli.Options) *BrokerDispatcher {
	return &BrokerDispatcher{
		address: o.Address,
		perTube: o.PerTube,
		tubeSet: make(map[string]bool),
		options: o,
		ret:     make(chan bool),
	}
}

// Shutdown finishes all active jobs and shuts down the listener
func (bd *BrokerDispatcher) Shutdown() {
	close(bd.ret)
}

// RunTube runs broker(s) for the specified tube.
// The number of brokers started is determined by the perTube argument to
// NewBrokerDispatcher.
func (bd *BrokerDispatcher) RunTube(tube string) {
	bd.tubeSet[tube] = true
	for i := uint64(0); i < bd.perTube; i++ {
		bd.runBroker(tube, i)
	}
}

// RunTube runs brokers for the specified tubes.
func (bd *BrokerDispatcher) RunTubes(tubes []string) {
	for _, tube := range tubes {
		bd.RunTube(tube)
	}
}

// RunAllTubes polls beanstalkd, running broker as new tubes are created.
func (bd *BrokerDispatcher) RunAllTubes() (err error) {
	conn, err := beanstalk.Dial("tcp", bd.address)
	if err == nil {
		bd.conn = conn
	} else {
		return
	}

	go func() {
		ticker := instantTicker(ListTubeDelay)
		for _ = range ticker {
			if e := bd.watchNewTubes(); e != nil {
				log.Error(e)
			}
		}
	}()

	return
}

func (bd *BrokerDispatcher) runBroker(tube string, slot uint64) {
	ticker := make(chan bool)
	bd.Add(1)

	go func() {
		b := New(bd.options, tube, slot, nil)
		b.Run(ticker, bd.Done)
	}()

	end := false
	go func() {
		for {
			if end {
				return
			}

			ticker <- true
		}
	}()

	<-bd.ret
	end = true
	close(ticker)
}

func (bd *BrokerDispatcher) watchNewTubes() (err error) {
	tubes, err := bd.conn.ListTubes()
	if err != nil {
		return
	}

	for _, tube := range tubes {
		if !bd.tubeSet[tube] {
			bd.RunTube(tube)
		}
	}

	return
}

// Like time.Tick() but also fires immediately.
func instantTicker(t time.Duration) <-chan time.Time {
	c := make(chan time.Time)
	ticker := time.NewTicker(t)
	go func() {
		c <- time.Now()
		for t := range ticker.C {
			c <- t
		}
	}()
	return c
}
