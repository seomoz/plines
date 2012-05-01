module Plines
  # Responsible for enqueing Qless jobs based on the given dependency graph.
  class JobEnqueuer
    def initialize(dependency_graph, job_batch)
      @dependency_graph = dependency_graph
      @job_batch = job_batch
    end

    def enqueue_jobs
      @dependency_graph.steps.each { |step| jids[step] }

      jids.each do |job, jid|
        @job_batch.add_job(jid, *job.external_dependencies)
      end

      self
    end

  private

    def jids
      @jids ||= Hash.new { |h, step| h[step] = enqueue_job_for(step) }
    end

    def enqueue_job_for(step)
      step.qless_queue.put \
        step.klass,
        step.data.merge('_job_batch_id' => @job_batch.id),
        depends: dependency_jids_for(step)
    end

    def dependency_jids_for(step)
      step.dependencies.map { |d| jids[d] }
    end
  end
end

