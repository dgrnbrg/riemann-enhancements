# riemann-enhancements

This will be a plugin to allow Riemann to store its metrics into a database, expose them via the Graphite REST API, and perform interesting analyses.

Right now, it implements a subset of the Graphite API, enough that (Giraffe)[https://github.com/kenhub/giraffe] works. It stores all data in Datomic in a specially designed index, designed for efficient queries and optimal data locality.

## Usage

You should add this as a dependency of Riemann, then use `riemann-enhancements.core/log-to-datomic` to make a Riemann stream sink. Call `riemann-enhancements.core/start-server` to start the REST API. The metric names are `host:service` in the Graphite API.

## TODO

The transaction retry logic and error reporting must be implemented before this is useful in a real setting.

The resampler's settings should be exposed through the Graphite API.

Implement enough of Graphite's API to support (graphitus)[https://github.com/erezmazor/graphitus].

Clean up all the println/comments.

The current version only can handle up to a million metrics due to Datomic design decisions. By implementing DB sharding, we could scale to many million of metrics and improve write parallelism. This *does not* affect the number of metrics storable.

There's no way to purge old data.

## Design

The Datomic indices are structured so that a given metric is indexed by its host and service, and that finding all services on a host or all hosts running a service is efficient. Once a metric has been chosen, queries are able to seek directly to the starting time of the requested data and read `[time, value]` pairs by linearly scanning the raw index, until the ending time of the data is reached.

The data-containing transactions themselves are batched, so that every 10s or 500 events causes a transaction, using core.async.

The metric entities and interesting partition scheme that allows the efficiency are implemented using sophisticated database functions. These demonstrate idempotent transaction functions and global singletons (i.e. the ident `:metrics/total).

@ztellman's narrator is used to implement a sophisticated resampling technique that is able to highlight gapped data and downsample in a statistically robust manner (in the ring handler).

## License

Copyright © 2013 David Greenberg

Distributed under the Eclipse Public License, the same as Clojure.
