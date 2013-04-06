require 'forwardable'

module Plines
  # An instance of a Step: a step class paired with some data for the job.
  Job = Struct.new(:klass, :data) do
    extend Forwardable
    attr_reader :dependencies, :dependents
    def_delegators :klass, :qless_queue, :processing_queue, :terminal_step?

    def initialize(*args)
      super
      raise ArgumentError.new, "data must be a hash" unless data.is_a?(Hash)
      @dependencies = Set.new
      @dependents = Set.new
      yield self if block_given?
    end

    def add_dependency(step)
      dependencies << step
      step.dependents << self
    end

    RemoveNonexistentDependencyError = Class.new(StandardError)
    def remove_dependency(step)
      unless dependencies.delete?(step) && step.dependents.delete?(self)
        raise RemoveNonexistentDependencyError,
          "Attempted to remove nonexistent dependency #{step} from #{self}"
      end
    end

    def add_dependencies_for(batch_data)
      klass.dependencies_for(self, batch_data).each do |job|
        add_dependency(job)
      end
    end

    def external_dependencies
      klass.external_dependencies_for(data)
    end

    class << self
      # Prevent users of this class from constructing a new instance directly;
      # Instead, they should use #build.
      #
      # Note: I tried to override #new (w/ a `super` call) but it didn't work...
      # I think it was overriding Struct.new rather than Job.new
      # or something.
      private :new

      # Ensures all "identical" instances (same klass and data)
      # created within the block are in fact the same object.
      # This is important when constructing the dependency graph,
      # so that all the dependency/dependee relationships point to
      # the right objects (rather than duplicate objects).
      def accumulate_instances
        self.repository = Hash.new { |h,k| h[k] = new(*k) }

        begin
          yield
          return repository.values
        ensure
          self.repository = nil
        end
      end

      def build(*args, &block)
        repository[args, &block]
      end

    private

      def repository=(value)
        Thread.current[:plines_job_repository] = value
      end

      NullRepository = Class.new do
        def self.[](args, &block)
          Job.send(:new, *args, &block)
        end
      end

      def repository
        Thread.current[:plines_job_repository] || NullRepository
      end
    end
  end
end

