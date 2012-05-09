require 'set'

module Plines
  # Responsible for enqueing Qless jobs based on the given dependency graph.
  class JobEnqueuer
    def initialize(dependency_graph, job_batch, &qless_job_options_block)
      @dependency_graph = dependency_graph
      @job_batch = job_batch
      @qless_job_options_block = qless_job_options_block
    end

    def enqueue_jobs
      @dependency_graph.steps.each { |step| jids[step] }

      jids.each do |job, jid|
        @job_batch.add_job(jid, *job.external_dependencies.keys)

        job.external_dependencies.each do |key, value|
          next unless timeout = value[:wait_up_to]
          external_dep_timeouts[TimeoutKey.new(key, timeout)] << job
        end
      end

      enqueue_external_dependency_timeouts

      self
    end

  private

    def jids
      @jids ||= Hash.new { |h, step| h[step] = enqueue_job_for(step) }
    end

    def enqueue_job_for(step)
      step.klass.enqueue_qless_job \
        step.data.merge('_job_batch_id' => @job_batch.id),
        @qless_job_options_block[step].merge(depends: dependency_jids_for(step))
    end

    def dependency_jids_for(step)
      step.dependencies.map { |d| jids[d] }
    end

    def external_dep_timeouts
      @external_dep_timeouts ||= Hash.new do |h, k|
        h[k] = Set.new
      end
    end

    TIMEOUT_JOB_PRIORITY = -999999 # an arbitrary high priority

    def enqueue_external_dependency_timeouts
      external_dep_timeouts.each do |tk, jobs|
        job_ids = jobs.map { |k| jids[k] }
        job_data = ExternalDependencyTimeout.job_data_for \
          @job_batch, tk.dep_name, job_ids

        jobs.first.processing_queue.put \
          ExternalDependencyTimeout, job_data,
          delay: tk.timeout, priority: TIMEOUT_JOB_PRIORITY
      end
    end

    TimeoutKey = Struct.new(:dep_name, :timeout)
  end
end

