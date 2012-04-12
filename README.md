# Plines

Note: this isn't yet implemented; this README is a more of a general
discussion.

A primary piece of the moz platform is the processing pipeline that
builds shards out of the backend data. For simplicity, we need to be
able to break this down into small, easily composable steps. We need
some smart code that wires up the pipeline steps and kicks off the
process whenever a backend is updated. Plines is intended to be that
piece.

Here's what step definitions may look like:

``` ruby
module PipelineStep
  class FetchSiloRankings
    include Plines::Step
    depends_on_backend :silo

    def perform(campaign_id)
      db.multi_insert(silo_rankings)
    end
  end

  class FetchGARankings
    include Plines::Step
    depends_on_backend :ga
  end

  class AggregateSiloGAData
    include Plines::Step
    depends_on_steps :FetchSiloRankings, :FetchGARankings
  end
end
```

This is a step that fetches rankings data from silo and puts it into a
database shard. A few things to note:

* Plines will take care of creating the intial empty shard (w/ proper schema).
* The `perform` method will get called with a connection to the correct
  shard already set up.  Within the method, `db` is a connection to the
  shard.
* The `depends_on_*` declarations build up a dependency graph of steps.
  The `AggregateSiloGAData` step will run automatically a short time
  after the `FetchSiloRankings` and `FetchGARankings` steps have run.
  Those steps will run shortly after their backend dependencies have
  notified Plines that they have new data.

## Open Questions

### What job processing system should we use?

Options I'm considering:

* [Resque](https://github.com/defunkt/resque) -- We know resque and have
  had great success with it. One downside: resque can occasionally drop
  jobs. This hasn't usually been a problem for us but it is a downside.
* [Sidekiq](https://github.com/mperham/sidekiq) -- Sidekiq is like
  resque++. It's mostly compatible with resque but includes a bunch of
  built-in improvements, including a multi-threading actor model that is
  for more memory efficient then resque's process-based model. It also
  has a bunch of features from resque plugins (like exponential
  backoff/retry) baked in. Biggest downsides: we don't know it yet, the
  license is a bit funny.
* [Qless](https://github.com/seomoz/qless) -- This is the system Dan has
  been developing.  I've been involved in some of the conversations and
  am very impressed.  It provides "no job gets dropped" guarantees (via
  atomic-operation lua scripts and heartbeating), built-in retry,
  comprehensive logged history of jobs, and more.
* [Ruote](http://ruote.rubyforge.org/) -- This is a system that provides
  many of the features we want for Plines (defining workflows from
  composite steps, running steps in parallel, defining timeouts, etc). I
  played with it for most of a day, and while it had many features we
  want, it didn't handle the step dependency graph well.

Which should we use?  Or should Pline be agnostic to this and work for
any of them? Regardless of the system used, I anticipate using redis
underneath. It's a great fit to power this type of job processing
system. Resque, Sidekiq and Qless are all built on top of redis, and
Ruote provides multiple pluggable storage backends, one of which is
redis.

### How should we handle backends finishing at different times?

In a perfect world, all backends will finish their weekly data
collection at the same time but we all know reality is different.

* One or more backends may finish that week's data collection a few days
  late.  Consider the case where rankings data is done on time but GA
  traffic data is late. We want to make the new rankings data available
  to users on the day it completed. Later that week, when GA data is
  ready we want to make that data available to users.
* How do we handle aggregate data for services that are out-of-sync? For
  example, if rankings data has come in on time but the most recent GA
  data is a week old, do we still build the aggregate views that combine
  traffic and rankings? Does it make sense to combine data from
  different weeks? Or do we have rankings pages show the current
  rankings data but aggregate pages show last week's aggregate data (so
  that it's actually apples & apples being compared)? Maybe this depends
  on which backends and aggregations we're talking about?
* If a backend notifies us of new data mid-week, what do we do?  Do we
  build a new shard based on it?  Do we ignore it and wait until that
  campaign's collection day? Sometimes this may be triggered by a
  campaign metadata change (e.g. a new keyword causes silo to re-run it's
  recent-rankings map reduce job). Other times, it may be a "corrective"
  measure taken by us to re-process data on the backend when there was a
  problem. For example: sometimes when we get burned by bing, I
  re-enqueue SERP collector jobs that got weird results.  This may be a
  day or two after the original jobs ran.

Maybe someone from product needs to be involved in these conversations?

### Shard construction: built off a clone of the last shard or always from scratch?

As I see it, we have two options for the basic model of shard construction:

* Each time we build a shard from scratch.
* Each time, we restore a MySQL dump of the previous week's shard, then
  run the pipeline processing steps, which are built to be idempotent
  and work fine against an empty or non-empty DB.

Which is better?

## Installation

Add this line to your application's Gemfile:

    gem 'plines'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install plines

## Usage

TODO: Write usage instructions here

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

