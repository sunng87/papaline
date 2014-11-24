# papaline

Clojure concurrent pipeline on
[core.async](https://github.com/clojure/core.async).

[![Build Status](https://travis-ci.org/sunng87/papaline.png?branch=master)](https://travis-ci.org/sunng87/papaline)

## What is a pipeline ?

A pipeline is a series of stages connected by channels, where each
stage is a group of ~~goroutines~~ **go blocks** running the same
function. In each stage, the ~~goroutines~~ **go blocks**

* receive values from upstream via inbound channels
* perform some function on that data, usually producing new values
* send values downstream via outbound channels

Each stage has any number of inbound and outbound channels, except the
first and last stages, which have only outbound or inbound channels,
respectively. The first stage is sometimes called the source or
producer; the last stage, the sink or consumer.

([source](http://blog.golang.org/pipelines))

## Usage

Like `clojure.core/comp`, with concurrent magic.

### Bootstrap

#### Leiningen

![latest version on clojars](https://clojars.org/info.sunng/papaline/latest-version.svg)

#### Clojure

```clojure
(require '[papaline.core :refer :all])
```

### Pipeline functions

A stage is a simple clojure function wrapped with the `stage`
function.

A pipeline is an ordered sequence of stages.

```clojure
(defn save-msg [user-id msg]
  ;; write the msg to mysql and get the msg id
  [user-id msg-id])

(defn query-followers [id msg-id]
  ;; ask mysql for some ids
  [follower-ids msg]
)

(defn fan-out-to-redis [follower-ids msg]
  ;; spread msg to redis lists
)

(def save-status
  (pipeline (map stage [save-msg query-followers fan-out-to-redis])))

```

Run the pipeline

```clojure
(run-pipeline save-status
  "userid"
  "Putin orders to approve draft bill on integration of #Crimea into Russia http://on.rt.com/xhz2zu")
```

Run pipeline and wait for the results

```clojure
(println (run-pipeline-wait save-status
  "userid"
  "Putin orders to approve draft bill on integration of #Crimea into
  Russia http://on.rt.com/xhz2zu"))
```

And wait with a timeout

```clojure
(println (run-pipeline-timeout save-status
  5000 ;; timeout in msecs
  nil ;; return value on timeout
  ;; arguments
  "userid"
  "Putin orders to approve draft bill on integration of #Crimea into
  Russia http://on.rt.com/xhz2zu"))
```

Stop a pipeline: the entire pipeline will be stopped and no more input
will be processed.

```clojure
(cancel-pipeline save-status)
```

### Special return types in stage functions

#### (abort)

You can abort task execution at any stage of pipeline. Just return in
your stage function.

```clojure
(defn my-stage []
  (if ...
    ...
    (abort)))
```

You can also add some return value for `run-pipeline-wait` and
`run-pipeline-timeout` with `(abort)`:

```clojure
;; returns 123 as pipeline result
(abort 123)
```

#### (fork) and (join)

New in 0.3, you can use `(fork)` to split tasks, to execute them in
parallel in the next stage.

`(join)`ed results will wait for all forked tasks to finish, then pass
them as a whole result to next stage.

Note that if you do not join the results, then the first sub-task
result in forked staged will be returned value for `run-pipeline-wait`
and `run-pipeline-timeout`

```clojure
(defn find-followers []
  ...
  (fork (map #(vector % msg-id) followers)))

(defn fanout-to-redis [user-id msg-id]
  ...
  (join true))
```

### Dealing with Except!ons

It is possible to define an error handler for each pipeline. An error
handler is a simple function takes the Exception and current input as
arguments.

```clojure
(pipeline
  (map stage [save-msg query-followers fan-out-to-redis])
  :error-handler (fn [e args]
    ;; logging...
    ))
```

Currently we don't information about the exact stage which the
exception is produced because we don't have metadata for stages. If
you need stage specific error handler, it's better to do try-catch in
the stage function.

For synchronous call like `(run-pipeline-wait)` and
`(run-pipeline-timeout)`, the exception will be thrown to caller
thread too.

## License

Copyright © 2014 Sun Ning <sunng@about.me>

Distributed under the Eclipse Public License either version 1.0 or any
later version.
