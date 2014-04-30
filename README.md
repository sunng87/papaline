# papaline

Clojure concurrent pipeline on core.async

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

## Leiningen

`[info.sunng/papaline 0.1.1]`

## (require '[papaline.core :refer :all])

A stage is a simple clojure function wrapped with the `stage`
function.

A pipeline is a ordered sequence of stages.

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

More detailed doc coming soon.

## License

Copyright © 2014 Sun Ning <sunng@about.me>

Distributed under the Eclipse Public License either version 1.0 or any
later version.
