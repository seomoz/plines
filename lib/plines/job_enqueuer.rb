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
      @dependency_graph.ordered_steps.each do |step|
        jid = Qless.generate_jid
        jids[step] = jid
        @job_batch.add_job(jid, *step.external_dependencies.keys)

        enqueue_job_for(step, jid, dependency_jids_for(step))

        step.external_dependencies.each do |key, value|
          next unless timeout = value[:wait_up_to]
          external_dep_timeouts[TimeoutKey.new(key, timeout)] << step
        end
      end

      enqueue_external_dependency_timeouts

      self
    end

  private
    def jids
      @jids ||= {}
    end

    def enqueue_job_for(step, jid, depends_on)
      step.klass.enqueue_qless_job \
        step.data.merge('_job_batch_id' => @job_batch.id),
        @qless_job_options_block[step].merge(depends: depends_on, jid: jid)
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

