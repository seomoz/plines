require "plines/version"
require 'plines/step'
require 'plines/job_enqueuer'
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

  def start_processing(data = {})
    graph = DependencyGraph.build_for(data)
    JobEnqueuer.new(graph).enqueue_jobs
  end
end

