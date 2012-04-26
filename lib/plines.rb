require "plines/version"
require 'plines/configuration'
require 'plines/step'
require 'plines/job_enqueuer'
require 'plines/job_batch'
require 'plines/job_performer'
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
    graph = DependencyGraph.new(batch_data)
    JobEnqueuer.new(graph).enqueue_jobs
  end

  def job_batch_for(batch_data)
    JobBatch.new(configuration.batch_group_for batch_data)
  end
end

