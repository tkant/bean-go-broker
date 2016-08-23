/*
	Package cli provides command line support for cmdstalk.
*/
package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Options contains runtime configuration, and is generally the result of
// parsing command line flags.
type Options struct {

	// The beanstalkd TCP address.
	Address string

	// All == true means all tubes will be watched.
	All bool

	// PerTube is the number of workers servicing each tube concurrently.
	PerTube uint64

	// The beanstalkd tubes to watch.
	Tubes TubeList

	// Full path to PHP Binary that should be used
	PHPBinary string

	// full path to configuration ini file that should be used
	PHPINI string

	// Full path to the directory where all instances are located
	InstanceRoot string

	// Full path to the directory where cluster is located
	ClusterRoot string

	// Controller that will handle the Job
	Controller string

	// RequeueDelay is the delay to be used when a task is re-requeued
	RequeueDelay time.Duration
}

// TubeList is a list of beanstalkd tube names.
type TubeList []string

// Calls ParseFlags(), os.Exit(1) on error.
func MustParseFlags() (o Options) {
	o, err := ParseFlags()
	if err != nil {
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println(err)
		os.Exit(1)
	}
	return
}

// ParseFlags parses and validates CLI flags into an Options struct.
func ParseFlags() (o Options, err error) {
	o.Tubes = TubeList{"default"}

	flag.StringVar(&o.Address, "address", "127.0.0.1:11300", "beanstalkd TCP address.")
	flag.StringVar(&o.PHPBinary, "php", "/usr/bin/php", "php binary to use")
	flag.StringVar(&o.PHPINI, "php-ini", "/etc/php.ini", "php.ini file to use for configuration")
	flag.StringVar(&o.InstanceRoot, "instance-root", "/var/www/html", "path to the directory where instances are located")
	flag.StringVar(&o.ClusterRoot, "cluster-root", "/opt/cluster", "path to the directory where cluster is located")
	flag.StringVar(&o.Controller, "controller", "/Core/Job/Console", "Controller that will handle the Job")
	flag.DurationVar(&o.RequeueDelay, "requeue-delay", 1*time.Minute, "Delay duration for when the job is requeued")
	flag.BoolVar(&o.All, "all", false, "Listen to all tubes, instead of -tubes=...")
	flag.Uint64Var(&o.PerTube, "per-tube", 1, "Number of workers per tube.")
	flag.Var(&o.Tubes, "tubes", "Comma separated list of tubes.")
	flag.Parse()

	err = validateOptions(o)

	return
}

func validateOptions(o Options) error {
	msgs := make([]string, 0)

	if o.Address == "" {
		msgs = append(msgs, "Address must not be empty (use -address flag)")
	}
	if o.PHPBinary == "" {
		msgs = append(msgs, "Path to PHP binary must not be empty (use -php flag)")
	}
	if o.PHPINI == "" {
		msgs = append(msgs, "Path to PHP ini file must not be empty (use -php-ini flag)")
	}
	if o.InstanceRoot == "" {
		msgs = append(msgs, "Instance root path must not be empty (use -instance-root flag)")
	}
	if o.ClusterRoot == "" {
		msgs = append(msgs, "Path to cluster must not be empty (use -cluster-root flag)")
	}
	if o.Controller == "" {
		msgs = append(msgs, "Controller must not be empty (use -controller flag)")
	}

	if len(msgs) == 0 {
		return nil
	} else {
		return errors.New(strings.Join(msgs, "\n"))
	}
}

// Set replaces the TubeList by parsing the comma-separated value string.
func (t *TubeList) Set(value string) error {
	list := strings.Split(value, ",")
	for i, value := range list {
		list[i] = value
	}
	*t = list
	return nil
}

func (t *TubeList) String() string {
	return fmt.Sprint(*t)
}
