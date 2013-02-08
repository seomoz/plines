require 'qless'
require 'forwardable'

module Plines
  # This module should be extended onto a class or module in order
  # to make it a pipeline. Steps declared within that module will
  # automatically belong to that pipeline. This enables one application
  # to have multiple pipelines.
  module Pipeline
    extend Forwardable
    def_delegators :configuration#, :qless, :redis

    DEFAULT_QUEUE = "plines"
    AWAITING_EXTERNAL_DEPENDENCY_QUEUE = "awaiting_ext_dep"

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield configuration
    end

    def enqueue_jobs_for(batch_data)
      graph = DependencyGraph.new(step_classes, batch_data)
      job_batch_list = job_batch_list_for(batch_data)

      job_batch_list.create_new_batch(batch_data).tap do |job_batch|
        job_options_block = configuration.qless_job_options_block
        JobEnqueuer.new(graph, job_batch, &job_options_block).enqueue_jobs
      end
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

    def job_batch_list_for(batch_data)
      key = configuration.batch_list_key_for(batch_data)
      JobBatchList.new(self, key)
    end

    def matching_older_unfinished_job_batches(main_job_batch)
      job_batch_list = job_batch_list_for(main_job_batch.data)
      job_batch_list.each.select do |job_batch|
        !job_batch.complete? && job_batch.created_at < main_job_batch.created_at
      end
    end
  end
end

