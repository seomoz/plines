require 'plines'

module Plines
  class JobEnqueuer
    def initialize(dependency_graph)
      @dependency_graph = dependency_graph
    end

    def enqueue_jobs
      @dependency_graph.steps.each { |step| jids[step] }
    end

  private

    def jids
      @jids ||= Hash.new { |h, step| h[step] = enqueue_job_for(step) }
    end

    def enqueue_job_for(step)
      Plines.default_queue.put(step.klass, step.data, depends: dependency_jids_for(step))
    end

    def dependency_jids_for(step)
      step.dependencies.map { |d| jids[d] }
    end
  end
end

