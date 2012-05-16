require 'qless'

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
      @default_queue ||= qless.queues["plines"]
    end

    def awaiting_external_dependency_queue
      @awaiting_external_dependency_queue ||= qless.queues["awaiting_ext_dep"]
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
      job_options_block = configuration.qless_job_options_block
      JobEnqueuer.new(graph, job_batch, &job_options_block).enqueue_jobs
    end

    def most_recent_job_batch_for(batch_data)
      job_batch_list_for(batch_data).most_recent_batch
    end

    def step_classes
      @step_classes ||= []
    end

    # Null Object pattern implementation of the root dependency
    # object so we don't need nil checks on #root_dependency
    class NullRootDependency
      def self.jobs_for(*args)
        []
      end

      def self.nil?
        true
      end
    end

    # Error raised when two steps declare `depended_on_by_all_steps`;
    # This cannot be allowed or there would be a circular dependency.
    class RootDependencyAlreadySetError < StandardError; end

    def root_dependency=(value)
      if root_dependency.nil?
        @root_dependency = value
      else
        raise RootDependencyAlreadySetError,
          "The root dependency for pipeline #{self} is already set. " +
          "Multiple root dependencies are not supported."
      end
    end

    def root_dependency
      @root_dependency ||= NullRootDependency
    end

    def set_expiration_on(*redis_keys)
      redis_keys.each do |key|
        redis.pexpire(key, configuration.data_ttl_in_milliseconds)
      end
    end

  private

    def job_batch_list_for(batch_data)
      key = configuration.batch_list_key_for(batch_data)
      JobBatchList.new(self, key)
    end
  end
end

