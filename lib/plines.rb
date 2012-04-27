require "plines/version"
require 'plines/configuration'
require 'plines/step'
require 'plines/job_enqueuer'
require 'plines/job_batch'
require 'plines/job_batch_list'
require 'qless'

module Plines
  extend self
  attr_writer :qless

  def qless
    @qless ||= Qless::Client.new
  end

  def default_queue
    @default_queue ||= qless.queue("plines")
  end

  def configuration
    @configuration ||= Configuration.new
  end

  def configure
    yield configuration
  end

  def redis
    qless.redis
  end

  def enqueue_jobs_for(batch_data = {})
    JobEnqueuer.new(batch_data).enqueue_jobs
  end

  def most_recent_job_batch_for(batch_data)
    JobBatchList.for(batch_data).most_recent_batch
  end
end

# Add Redis::Set#move from redis-objects master branch. Don't want to
# switch to a :git gem just for this. Hopefully it'll be released w/
# this soon.
require 'redis/set'
Redis::Set.class_eval do
  # Moves value from one set to another. Destination can be a String
  # or Redis::Set.
  #
  #   set.move(value, "name_of_key_in_redis")
  #   set.move(value, set2)
  #
  # Returns true if moved successfully.
  #
  # Redis: SMOVE
  def move(value, destination)
    redis.smove(key,
      destination.is_a?(Redis::Set) ? destination.key : destination.to_s,
      value)
  end unless instance_method(:move).arity == 2
  # All redis objects have a #move method that takes 1 argument; it moves the
  # entire object to a new redis key. #move here takes 2 args and moves one
  # entry in the set to a different set.
end
