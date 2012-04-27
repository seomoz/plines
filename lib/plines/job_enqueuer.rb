require 'plines'

module Plines
  # Responsible for enqueing Qless jobs based on the given dependency graph.
  class JobEnqueuer
    def initialize(batch_data)
      @batch_data = batch_data
      @dependency_graph = DependencyGraph.new(batch_data)
    end

    def enqueue_jobs
      @dependency_graph.steps.each { |step| jids[step] }
      JobBatchList.for(@batch_data).create_new_batch(jids.values)
      self
    end

  private

    def jids
      @jids ||= Hash.new { |h, step| h[step] = enqueue_job_for(step) }
    end

    def enqueue_job_for(step)
      Plines.default_queue.put \
        step.klass,
        step.data,
        depends: dependency_jids_for(step)
    end

    def dependency_jids_for(step)
      step.dependencies.map { |d| jids[d] }
    end
  end
end

