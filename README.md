`datadog` cannot arbitrarily glob metric names at read time, i.e:

    sum(nsq.topic.*.channel.*.message_count)

Instead, it offers you the ability to "tag" metrics. Unfortunately, metrics must be tagged (and
re-written) at write time to be able to slice in the dimensions you want.

`ddstatsd` is a UDP proxy that takes a JSON configuration file with regex-based rules to rewrite
incoming metric names and add tags, outputing to the local `dd-agent` in its custom statsd format.
