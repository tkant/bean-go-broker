beanstalk broker
========

Beanstalk Broker handles beanstalk jobs and forward them to NOVO console controller.
Bulk of this codebase is taken from [cmdstalk][] command line broker and modified to forward
the class to console controllers instead of a shell command.

Each job is passed as stdin to a new instance of a console command.
On `exit(0)` the job is deleted. On `exit(1)` (or any non-zero status) the job
is released with an exponential-backoff delay (releases^4), up to 10 times.

If the worker has not finished by the time the job TTR is reached, the worker
is killed (SIGTERM, SIGKILL) and the job is allowed to time out. When the
job is subsequently reserved, the `timeouts: 1` will cause it to be buried.


Install
-------

From source:

```sh
# Make sure you have a sane $GOPATH
go get github.com/kayako/beanstalk-broker
```

Usage
-----

```sh
beanstalk-broker -help

Usage of beanstalk-broker:
   -address="127.0.0.1:11300": beanstalkd TCP address.
   -all=false: Listen to all tubes, instead of -tubes=...
   -per-tube=1: Number of workers per tube.
   -tubes=[default]: Comma separated list of tubes.
   -php=/usr/bin/php: PHP Binary to use
   -php-ini=/etc/php.ini: ini file to use for PHP configuration
   -controller=/Core/Job/Console: Controller that will handle the jobs

# Watch three specific tubes.
cmdstalk -tubes="one,two,three"

# Watch all current and future tubes, four workers per tube.
cmdstalk -all -per-tube=4
```

TODO
----

* Graceful shutdown.

Credits
---

Cmdstalk Created by [Paul Annesley][pda] and [Lachlan Donald][lox].

[beanstalkd]: http://kr.github.io/beanstalkd/
[beanstalk]: http://godoc.org/github.com/kr/beanstalk
[pda]: https://twitter.com/pda
[lox]: https://twitter.com/lox
