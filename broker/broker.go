package broker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kayako/beanstalk-broker/bs"
	"github.com/kayako/beanstalk-broker/cli"
	"github.com/kayako/beanstalk-broker/cmd"
	"github.com/kr/beanstalk"
	log "github.com/sirupsen/logrus"
	"github.com/wulijun/go-php-serialize/phpserialize"
)

const (
	// ttrMargin compensates for beanstalkd's integer precision.
	// e.g. reserving a TTR=1 job will show time-left=0.
	// We need to set our SIGTERM timer to time-left + ttrMargin.
	ttrMargin = 1 * time.Second

	// TimeoutTries is the number of timeouts a job must reach before it is
	// buried. Zero means never execute.
	TimeoutTries = 1

	// ReleaseTries is the number of releases a job must reach before it is
	// buried. Zero means never execute.
	ReleaseTries = 10

	// ClusterRoot is the full path to cluster directory
	ClusterRoot = "/opt/cluster/"

	// InstanceRoot is the full path to directory where instances are stored
	InstanceRoot = "/var/www/html/"
)

type Broker struct {

	// Address of the beanstalkd server.
	Address string

	// The shell command to execute for each job.
	Cmd string

	// Tube name this broker will service.
	Tube string

	options cli.Options

	log     *log.Entry
	results chan<- *JobResult

	sync.WaitGroup
}

type JobResult struct {

	// Buried is true if the job was buried.
	Buried bool

	// Executed is true if the job command was executed (or attempted).
	Executed bool

	// ExitStatus of the command; 0 for success.
	ExitStatus int

	// JobId from beanstalkd.
	JobId uint64

	// Stdout of the command.
	Stdout []byte

	// TimedOut indicates the worker exceeded TTR for the job.
	// Note this is tracked by a timer, separately to beanstalkd.
	TimedOut bool

	// Error raised while attempting to handle the job.
	Error error
}

// New broker instance.
func New(o cli.Options, tube string, slot uint64, results chan<- *JobResult) (b Broker) {
	b.Address = o.Address
	b.Tube = tube
	b.options = o

	b.log = log.WithFields(log.Fields{
		"tube": tube,
		"slot": slot,
	})

	b.results = results
	return
}

// Run connects to beanstalkd and starts broking.
// If ticks channel is present, one job is processed per tick.
func (b *Broker) Run(ticks chan bool, fin func()) {
	defer fin()
	b.log.Debugf("connecting to address: %s", b.Address)
	conn, err := beanstalk.Dial("tcp", b.Address)
	if err != nil {
		log.Error(err)
		return
	}

	b.log.Printf("watching tube %s", b.Tube)
	ts := beanstalk.NewTubeSet(conn, b.Tube)

	for {
		if _, ok := <-ticks; !ok {
			b.log.Info("preparing for shutdown")
			return
		}

		b.log.Info("reserve (waiting for job)")
		id, body := bs.MustReserveWithoutTimeout(ts)
		job := bs.NewJob(id, body, conn)

		t, err := job.Timeouts()
		if err != nil {
			b.log.Error(err)
			return
		}
		if t >= TimeoutTries {
			b.log.Warnf("job %d has %d timeouts, burying", job.Id, t)
			err := job.Release(b.options.RequeueDelay)
			if err != nil {
				b.log.Errorf("failed to re-queue a timed out job, error: %s", err.Error())
				continue
			}
			if b.results != nil {
				b.results <- &JobResult{JobId: job.Id, Buried: true}
			}
			continue
		}

		releases, err := job.Releases()
		if err != nil {
			b.log.Error(err)
			return
		}
		if releases >= ReleaseTries {
			b.log.Infof("job %d has %d releases, re queueing", job.Id, releases)
			err := job.Release(b.options.RequeueDelay)
			if err != nil {
				b.log.Errorf("failed to re-queue the job, error: %s", err.Error())
				continue
			}
			if b.results != nil {
				b.results <- &JobResult{JobId: job.Id, Buried: true}
			}
			continue
		}

		wd, err := getJobWD(b.options, job)
		if err != nil {
			log.Error(err)
			return
		}

		b.log.Infof("executing job %d in path %s", job.Id, wd)

		result, err := b.executeJob(job, wd)
		if err != nil {
			log.Error(err)
			return
		}

		err = b.handleResult(job, result)
		if err != nil {
			log.Error(err)
			return
		}

		if result.Error != nil {
			b.log.Warnf("result had error: %s", result.Error)
		}

		if b.results != nil {
			b.results <- result
		}
	}

	b.log.Infof("broker finished")
}

func getJobWD(o cli.Options, job bs.Job) (string, error) {
	dec, err := phpserialize.Decode(string(job.Body))
	if err != nil {
		return "", fmt.Errorf("failed to unserialize the job, error: %s", err)
	}

	var domain string

	switch dec.(type) {
	case map[interface{}]interface{}:
		domain, err = findDomain(dec.(map[interface{}]interface{}))
		if err != nil {
			return "", err
		}

	default:
		return "", fmt.Errorf("failed to interpret the job packet, expecting a map got %v", dec)
	}

	if strings.ToLower(domain) == "cluster" {
		return o.ClusterRoot + "/worker", nil
	}

	return o.InstanceRoot + "/" + domain + "/worker", nil
}

func findDomain(dec map[interface{}]interface{}) (string, error) {
	for k, v := range dec {
		switch k.(type) {
		case string:
			if k != "domain" {
				continue
			}

			if d, ok := v.(string); ok {
				return d, nil
			}

			return "", errors.New("value of domain key is not a string")
		}
	}

	return "", errors.New("failed to find domain key in job packet")
}

func (b *Broker) executeJob(job bs.Job, cwd string) (result *JobResult, err error) {
	result = &JobResult{JobId: job.Id, Executed: true}

	ttr, err := job.TimeLeft()
	timer := time.NewTimer(ttr + ttrMargin)
	if err != nil {
		return
	}

	cmd, out, err := cmd.NewCommand(cwd, b.options.PHPBinary, "-c", b.options.PHPINI, "index.php", b.options.Controller)
	if err != nil {
		return
	}

	if err = cmd.StartWithStdin(job.Body); err != nil {
		return
	}

stdoutReader:
	for {
		select {
		case <-timer.C:
			if err = cmd.Terminate(); err != nil {
				return
			}
			result.TimedOut = true
		case data, ok := <-out:
			if !ok {
				break stdoutReader
			}
			b.log.Infof("stdout: %s", data)
			result.Stdout = append(result.Stdout, data...)
		}
	}

	waitC := cmd.WaitChan()

waitLoop:
	for {
		select {
		case wr := <-waitC:
			timer.Stop()
			if wr.Err == nil {
				err = wr.Err
			}
			result.ExitStatus = wr.Status
			break waitLoop
		case <-timer.C:
			cmd.Terminate()
			result.TimedOut = true
		}
	}

	return
}

func (b *Broker) handleResult(job bs.Job, result *JobResult) (err error) {
	if result.TimedOut {
		b.log.Warnf("job %d timed out", job.Id)
		return
	}
	b.log.Infof("job %d finished with exit(%d)", job.Id, result.ExitStatus)
	switch result.ExitStatus {
	case 0:
		b.log.Infof("deleting job %d", job.Id)
		err = job.Delete()
	default:
		r, err := job.Releases()
		if err != nil {
			r = ReleaseTries
		}
		// r*r*r*r means final of 10 tries has 1h49m21s delay, 4h15m33s total.
		// See: http://play.golang.org/p/I15lUWoabI
		delay := time.Duration(r*r*r*r) * time.Second
		b.log.Infof("releasing job %d with %v delay (%d retries)", job.Id, delay, r)
		err = job.Release(delay)
	}
	return
}
