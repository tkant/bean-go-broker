/*
	Package bs provides a richer and/or more domain-specific layer over
	github.com/kr/beanstalk, including active-record style Job type.
*/
package bs

import (
	"time"

	"github.com/kr/beanstalk"
	log "github.com/sirupsen/logrus"
)

const (
	// deadlineSoonDelay defines a period to sleep between receiving
	// DEADLINE_SOON in response to reserve, and re-attempting the reserve.
	DeadlineSoonDelay = 1 * time.Second
)

// reserve-with-timeout until there's a job or something critical
// Handles beanstalk.ErrTimeout by retrying immediately.
// Handles beanstalk.ErrDeadline by sleeping DeadlineSoonDelay before retry.
// print other errors.
func MustReserveWithoutTimeout(ts *beanstalk.TubeSet) (uint64, []byte) {
	for {
		id, body, err := ts.Reserve(1 * time.Hour)
		if err == nil {
			return id, body
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
			continue
		} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
			time.Sleep(DeadlineSoonDelay)
			continue
		} else {
			log.Error(err)
			continue
		}
	}
}
