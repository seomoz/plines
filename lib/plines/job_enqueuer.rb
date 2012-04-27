require 'plines'

module Plines
  # Responsible for enqueing Qless jobs based on the given dependency graph.
  class JobEnqueuer
    def initialize(batch_data)
      @job_batch = JobBatchList.for(batch_data).create_new_batch([])
      @dependency_graph = DependencyGraph.new(batch_data)
    end

    def enqueue_jobs
      @dependency_graph.steps.each { |step| jids[step] }
      jids.values.each { |jid| @job_batch.pending_job_jids << jid }
      self
    end

  private

    def jids
      @jids ||= Hash.new { |h, step| h[step] = enqueue_job_for(step) }
    end

    def enqueue_job_for(step)
      Plines.default_queue.put \
        step.klass,
        step.data.merge('_job_batch_id' => @job_batch.id),
        depends: dependency_jids_for(step)
    end

    def dependency_jids_for(step)
      step.dependencies.map { |d| jids[d] }
    end
  end
end

