require 'qless'
require 'forwardable'
require 'plines/indifferent_hash'

module Plines
  # This module should be extended onto a class or module in order
  # to make it a pipeline. Steps declared within that module will
  # automatically belong to that pipeline. This enables one application
  # to have multiple pipelines.
  module Pipeline
    extend Forwardable
    def_delegators :configuration

    DEFAULT_QUEUE = "plines"
    AWAITING_EXTERNAL_DEPENDENCY_QUEUE = "awaiting_ext_dep"

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield configuration
    end

    def enqueue_jobs_for(batch_data, options = {})
      batch_data = IndifferentHash.from(batch_data)
      graph = DependencyGraph.new(self, batch_data)
      job_batch_list = job_batch_list_for(batch_data)

      job_batch_list.create_new_batch(batch_data, options) do |job_batch|
        job_options_block = configuration.qless_job_options_block
        JobEnqueuer.new(graph, job_batch, &job_options_block).enqueue_jobs
      end
    end

    def most_recent_job_batch_for(batch_data)
      job_batch_list_for(batch_data).most_recent_batch
    end

    def find_job_batch(id)
      key = id[/\A(.*):\d+\z/, 1] # http://rubular.com/r/fMGv1TaZZA
      qless = configuration.qless_client_for(key)
      Plines::JobBatch.find(qless, self, id)
    end

    def step_classes
      @step_classes ||= []
    end

    # Null Object pattern implementation of a step class
    class NullStep
      def self.jobs_for(*args)
        []
      end
    end

    # Error raised when two steps are declared as the same boundary step.
    # Having more than one initial or terminal step is not well defined.
    class BoundaryStepAlreadySetError < StandardError; end

    def self.define_boundary_step(name)
      define_method "#{name}=" do |value|
        current_value = public_send(name)
        if current_value == NullStep
          instance_variable_set(:"@#{name}", value)
        else
          raise BoundaryStepAlreadySetError,
            "The #{name} for pipeline #{self} is already set. " +
            "Multiple of these boundary steps are not supported."
        end
      end

      define_method name do
        current_value = instance_variable_get(:"@#{name}")
        return current_value if current_value
        instance_variable_set(:"@#{name}", NullStep)
      end
    end
    private_class_method :define_boundary_step

    define_boundary_step :initial_step
    define_boundary_step :terminal_step

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

