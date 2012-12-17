# Plines

Plines creates job pipelines out of a complex set of step dependencies.
It's intended to maximize the efficiency and throughput of the jobs
(ensuring jobs are run as soon as their dependencies have been met)
while minimizing the amount of "glue" code you have to write to make it
work.

Plines is built on top of [Qless](https://github.com/seomoz/qless) and
[Redis](http://redis.io/).

## Installation

Add this line to your application's Gemfile:

    gem 'plines'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install plines

## Getting Started

First, create a pipeline using the `Plines::Pipeline` module:

``` ruby
module MyProcessingPipeline
  extend Plines::Pipeline

  configure do |config|
    # configuration goes here; see below for available options
  end
end
```

`MyProcessingPipeline` will function both as the namespace for your
pipeline steps and also as a singleton holding some state for your
pipeline.

Next, define some pipeline steps. Your steps should be simple ruby
classes that extend the `Plines::Step` module and define a `perform`
method:

``` ruby
module MyProcessingPipeline
  class CountWidgets
    extend Plines::Step

    def perform
      # do some work
    end
  end
end
```

The `Plines::Step` module makes available some class-level
macros for declaring step dependency relationships. See the **Step Class
DSL** section below for more details.

Once you've defined all your steps, you can enqueue jobs for them:

``` ruby
MyProcessingPipeline.enqueue_jobs_for("some" => "data", "goes" => "here")
```

`MyProcessingPipeline.enqueue_jobs_for` will enqueue a full set of qless
jobs (or a `JobBatch` in Plines terminology) for the given batch data
based on your step classes' macro declarations.

## Configuring a Pipeline

Plines supports configuration at the pipeline level:

``` ruby
module MyProcessingPipeline
  extend Plines::Pipeline

  configure do |config|
    # Determines how job batches are identified. Plines provides an API
    # to find the most recent existing job batch based on this key.
    config.batch_list_key { |batch_data| batch_data.fetch(:user_id) }

    # Determines how long the Plines job batch data will be kept around
    # in redis after the batch reaches a final state (cancelled or
    # completed). By default, this is set to 6 months, but you
    # will probably want to set it to something shorter (like 2 weeks)
    config.data_ttl_in_seconds = 14 * 24 * 60 * 60

    # Use this callback to set additional global qless job
    # options (such as queue, tags and priority). You can also set
    # options on an individual step class (see below).
    config.qless_job_options do |job|
      { tags: [job.data[:user_id]] }
    end
  end
end
```

## The Step Class DSL

An example will help illustrate the Step class DSL. (Note that this
example omits the `perform` method declarations for brevity).

``` ruby
module MakeThanksgivingDinner
  extend Plines::Pipeline

  class BuyGroceries
    extend Plines::Step

    # Indicates that the BuyGroceries step must run before all other steps.
    # Essentially creates an implicit dependency of all steps on this one.
    # You can have only one step declare `depended_on_by_all_steps`.
    # Doing this relieves you of the burden of having to add
    # `depends_on :BuyGroceries` to all step definitions.
    depended_on_by_all_steps
  end

  # This step depends on BuyGroceries automatically due to the
  # depended_on_by_all_steps declaration above.
  class MakeStuffing
    extend Plines::Step

    # qless_options lets you set qless job options for this step.
    qless_options do |qless|
      # By default, jobs are enqueued to the :plines queue but you can override it
      # Plines::Step overrides here will override any configurations in a Plines::Pipeline class
      qless.queue = :make_stuffing
      qless.tags = [:foo, :bar]
      qless.priority = -10
      qless.retries = 7
    end
  end

  class PickupTurkey
    extend Plines::Step

    # External dependencies are named things that must be resolved
    # before this step is allowed to proceed. They are intended for
    # use when a step has a dependency on data from an external
    # asynchronous system that operates on its own schedule.
    has_external_dependencies(wait_up_to: 12.hours) do |job_data|
      # you can return a single dependency or an array of them.
      "await_turkey_is_ready_for_pickup_notice"
    end
  end

  class PrepareTurkey
    extend Plines::Step

    # Declares that the PrepareTurkey job cannot run until the
    # PickupTurkey has run first. Note that the step class name
    # is relative to the pipeline module namespace.
    depends_on :PickupTurkey, wait_up_to: 6.hours
  end

  class MakePie
    extend Plines::Step

    # By default, a single instance of a step will get enqueued in a
    # pipeline job batch. The `fan_out` macro can be used to get multiple
    # instances of the same step in a single job batch, each with
    # different arguments.
    #
    # In this example, we will have multiple `MakePie` steps--one for
    # each pie type, each with a different pie type argument.
    fan_out do |batch_data|
      batch_data['pie_types'].map do |type|
        { 'pie_type' => type, 'family' => batch_data['family'] }
      end
    end
  end

  class AddWhipCreamToPie
    extend Plines::Step

    fan_out do |batch_data|
      batch_data['pie_types'].map do |type|
        { 'pie_type' => type, 'family' => batch_data['family'] }
      end
    end

    # By default, `depends_on` makes all instances of this step depend on all
    # instances of the named step. If you only want it to depend on some
    # instances of the named step, pass a block; the instances of this step
    # will only depend on the MakePie jobs for which the pie_type is the same.
    depends_on :MakePie do |add_whip_cream_data, make_pie_data|
      add_whip_cream_data['pie_type'] == make_pie_data['pie_type']
    end
  end

  class SetTable
    extend Plines::Step

    # Indicates that this step should run last. This relieves you
    # from the burden of having to add an extra `depends_on` declaration
    # for each new step you create.
    depends_on_all_steps
  end
end
```

## Enqueing Jobs

To enqueue a job batch, use `#enqueue_jobs_for`:

``` ruby
MakeThanksgivingDinner.enqueue_jobs_for(
  "family"    => "Smith",
  "pie_types" => %w[ apple pumpkin pecan ]
)
```

The argument given to `enqueue_jobs_for` _must_ be a hash. This
hash will be yielded to the `fan_out` blocks. In addition, this hash
(or the one returned by a `fan_out` block) will be available as
`#job_data` in a step's `#perform` method.

Based on the `MakeThanksgivingDinner` example above, the following jobs
will be enqueued in this batch:

* 1 BuyGroceries job
* 1 MakeStuffing job
* 1 PickupTurkey job
* 1 PrepareTurkey job
* 3 MakePie jobs, each with slightly different arguments (1 each with
  "apple", "pumpkin" and "pecan")
* 3 AddWhipCreamToPie jobs, each with slightly different arguments (1
  each with "apple", "pumpkin" and "pecan")
* 1 SetTable job

The declared dependencies will be honored as well:

* BuyGroceries is guaranteed to run first.
* MakeStuffing and the 3 MakePie jobs will be available for processing
  immediately after the BuyGroceries job has finished.
* The 3 AddWhipCreamToPie jobs will be available for processing once
  their corresponding MakePie jobs have completed.
* PickupTurkey will not run until the
  `"await_turkey_is_ready_for_pickup_notice"` external dependency is
  fulfilled (see below for more details).
* PrepareTurkey will be available for processing once the PickupTurkey
  job has finished.
* SetTable will wait to be processed until all other jobs are complete.

## Working With Job Batches

Plines stores data about the batch in redis. It also provides a
first-class `JobBatch` object that allows you to work with job batches.

First, you need to configure the pipeline so that it knows how your
batches are identified:

``` ruby
MakeThanksgivingDinner.configure do |config|
  config.batch_list_key do |batch_data|
    batch_data["family"]
  end
end
```

Once this is in place, you can find a particular job batch:

``` ruby
job_batch = MakeThanksgivingDinner.most_recent_job_batch_for("family" => "Smith")
```

The `batch_list_key` config option above means the job batch will be
keyed by the "family" entry in the batch data hash. Thus, you can easily
look up a job batch by giving it a hash with the same "family" entry.

Once you have a job batch, there are several things you can do with it:

``` ruby
# returns whether or not the job batch is finished.
job_batch.complete?

# cancels all remaining jobs in this batch
job_batch.cancel!

# Resolves the named external dependency. For the example above,
# calling this will allow the PickupTurkey job to proceed.
job_batch.resolve_external_dependency "await_turkey_is_ready_for_pickup_notice"
```

Plines sets expiration on the redis keys it uses to track job batches as
soon as the job batch is completed or canceled. By default, the
expiration is set to 6 months.  You can configure it if you wish to
shorten it:

``` ruby
MakeThanksgivingDinner.configure do |config|
  config.data_ttl_in_seconds = 14 * 24 * 60 * 60 # 2 weeks
end
```

## External Dependency Timeouts

Under normal configuration, no job will run until all of its
dependencies have been met. However, plines provides support
for timing out an external dependency:

``` ruby
module MyPipeline
  class MyStep
    extend Plines::Step
    has_external_dependencies(wait_up_to: 3.hours) do |job_data|
      "my_async_service"
    end
  end
end
```

With this configuration, Plines will schedule a Qless job to run in
3 hours that will timeout the `"my_async_service"` external dependency,
allowing the `MyStep` job to run without the dependency being resolved.

## Step Dependency Timeouts (TODO)

Plines does not yet support step dependency timeouts; however,
Patrick and I (Myron) have thought a fair bit about it and have
come up with what we think is the right way to do it.  I'm documenting
it here so that I can move on to work on other things, but retain
the design we came up with.

Step dependency timeouts are declared similarly to external dependency timeouts:

``` ruby
module MyPipeline
  class MyStep
    extend Plines::Step
    depends_on :SomeOtherStep, wait_up_to: 2.hours
    qless_options do |qless|
      qless.queue = :awesome
    end
  end
end
```

Step dependency timeouts work in a similar fashion to external
dependency timeouts (e.g. by scheduling a job at a high priority
that will time out the dependency once the delay elapses). However,
we don't want timeouts to occur because the `:awesome` queue is
backed up more than 2 hours. The timeout should only occur because the
job itself took too long to run, or failed and did not get retried, or
failed all of its retries. Thus, the scheduled timeout job should not be
enqueued until SomeOtherStep is popped off the queue.  Right before the
`#perform` method is called, Plines will enqueue a scheduled timeout
job. If the perform method completes successfully, the scheduled job
will get cancelled.

## Performing Work

When a job gets run, the `#perform` instance method of your step class
will be called. The return value of your perform method is ignored.
The perform method will have access to a few helper methods:

``` ruby
module MakeThanksgivingDinner
  class MakeStuffing
    extend Plines::Step

    def perform
      # job_data gives you a struct-like object that is built off of
      # your job_data hash
      job_data.family # => returns "Smith" for our example

      # The job_batch instance this job is a part of is available as
      # well, so you can do things like cancel the batch.
      job_batch.cancel!

      # External dependencies may be unresolved if it timed out (see above).
      # #unresolved_external_dependencies returns an array of symbols,
      # listing the external dependencies that are unresolved.
      #
      # Note that this does not necessarily indicate whether or not an
      # external dependency timed out; it may have timed out, but then
      # got resolved before this job ran.
      # In addition, pending external dependencies are included (e.g.
      # if the job was manually moved into the processing queue)
      if unresolved_external_dependencies.any?
        # do something different because there's an unresolved dependency
      end
    end
  end
end
```

Plines also supports a middleware stack that wraps your `perform` method.
To create a middleware, define a module with an `around_perform` method:

``` ruby
module TimeWork
  def around_perform
    start_time = Time.now

    # Use super at the point the work should occur...
    super

    end_time = Time.now
    log_time(end_time - start_time)
  end
end
```

Then, include the module in your step class:

``` ruby
module MakeThanksgivingDinner
  class MakeStuffing
    include TimeWork
  end
end
```

You can include as many middleware modules as you like.

## TODO

* Implement the step dependency timeout schema described above.
* Provide a means to configure the redis connection. Currently,
  `Redis.connect` is used, which uses the `REDIS_URL` environment
  variable, but long term it would be nice to be able to configure it.
* Cancel scheduled timeout jobs when dependencies are met.
* Open source this! It's currently a private github repo, but I think we
  should plan to open source it.  However, it probably makes sense to
  actually use it in production first :).

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

