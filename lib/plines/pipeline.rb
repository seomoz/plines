module Plines
  # This module should be extended onto a class or module in order
  # to make it a pipeline. Steps declared within that module will
  # automatically belong to that pipeline. This enables one application
  # to have multiple pipelines.
  module Pipeline
    attr_writer :qless

    def qless
      @qless ||= Qless::Client.new
    end

    def default_queue
      @default_queue ||= qless.queue("plines")
    end

    def awaiting_external_dependency_queue
      @awaiting_external_dependency_queue ||= qless.queue("awaiting_ext_dep")
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

    def enqueue_jobs_for(batch_data)
      graph = DependencyGraph.new(step_classes, batch_data)
      job_batch = job_batch_list_for(batch_data).create_new_batch
      JobEnqueuer.new(graph, job_batch).enqueue_jobs
    end

    def most_recent_job_batch_for(batch_data)
      job_batch_list_for(batch_data).most_recent_batch
    end

    def step_classes
      @step_classes ||= []
    end

  private

    def job_batch_list_for(batch_data)
      key = configuration.batch_list_key_for(batch_data)
      JobBatchList.new(self, key)
    end
  end
end

