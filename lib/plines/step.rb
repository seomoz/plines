require 'plines/dependency_graph'

module Plines
  # An instance of a Step: a step class paired with some data for the job.
  StepInstance = Struct.new(:klass, :data) do
    attr_reader :dependencies, :dependees

    def initialize(*args)
      super
      @dependencies = Set.new
      @dependees = Set.new
      yield self if block_given?
    end

    def add_dependency(step)
      dependencies << step
      step.dependees << self
      self
    end

    class << self
      # Prevent users of this class from constructing a new instance directly;
      # Instead, they should use #build.
      #
      # Note: I tried to override #new (w/ a `super` call) but it didn't work...
      # I think it was overriding Struct.new rather than StepInstance.new
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
        Thread.current[:plines_step_instance_repository] = value
      end

      NullRepository = Class.new do
        def self.[](args, &block)
          StepInstance.send(:new, *args, &block)
        end
      end

      def repository
        Thread.current[:plines_step_instance_repository] || NullRepository
      end
    end
  end

  # This is the module that should be included in any class that
  # is intended to be a Plines step.
  module Step
    def self.all_classes
      @all_classes ||= []
    end

    def self.included(klass)
      klass.extend ClassMethods
      klass.fan_out { |d| [d] } # default to one step instance
      Plines::Step.all_classes << klass
    end

    # The class-level Plines step macros.
    module ClassMethods
      def depends_on(*klasses, &block)
        klasses.each do |klass|
          dependency_filters[klass] = (block || Proc.new { true })
        end
      end

      def fan_out(&block)
        @fan_out_block = block
      end

      def dependencies_for(batch_data)
        Enumerator.new do |yielder|
          dependency_filters.each do |klass, filter|
            klass = module_namespace.const_get(klass)
            klass.step_instances_for(batch_data).each do |step_instance|
              yielder.yield step_instance if filter[step_instance.data]
            end
          end
        end
      end

      def has_no_dependencies?
        dependency_filters.none?
      end

      def step_instances_for(batch_data)
        @fan_out_block.call(batch_data).map do |step_instance_data|
          StepInstance.build(self, step_instance_data)
        end
      end

    private

      def module_namespace
        namespaces = name.split('::')
        namespaces.pop # ignore the last one
        namespaces.inject(Object) { |ns, mod| ns.const_get(mod) }
      end

      def dependency_filters
        @dependency_filters ||= {}
      end
    end
  end
end

