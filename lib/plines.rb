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

