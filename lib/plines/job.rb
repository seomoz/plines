require 'forwardable'
require 'plines/indifferent_hash'

module Plines
  NotAHashError = Class.new(TypeError)

  # An instance of a Step: a step class paired with some data for the job.
  Job = Struct.new(:klass, :data) do
    extend Forwardable
    attr_reader :dependencies, :dependents
    def_delegators :klass, :qless_queue

    def initialize(klass, data)
      unless (data.is_a?(Hash) || data.is_a?(IndifferentHash))
        raise NotAHashError, "Expected a hash, got #{data.inspect}"
      end

      super(klass, klass.pipeline.configuration.exposed_hash_from(data))
      @dependencies = Set.new
      @dependents = Set.new
      yield self if block_given?
    end

    def add_dependency(step)
      dependencies << step
      step.dependents << self
    end

    def processing_queue
      @processing_queue ||= klass.processing_queue_for(data)
    end

    RemoveNonexistentDependencyError = Class.new(StandardError)
    def remove_dependency(step)
      unless dependencies.delete?(step) && step.dependents.delete?(self)
        raise RemoveNonexistentDependencyError,
          "Attempted to remove nonexistent dependency #{step} from #{self}"
      end
    end

    def add_dependencies_for(batch_data, jobs_by_klass)
      klass.dependencies_for(self, batch_data, jobs_by_klass).each do |job|
        add_dependency(job)
      end
    end

    def external_dependencies
      klass.external_dependencies_for(data)
    end
  end
end

