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
  end

  # This is the module that should be included in any class that
  # is intended to be a Plines step.
  module Step
    def self.all_classes
      @all_classes ||= []
    end

    def self.included(klass)
      klass.extend ClassMethods
      Plines::Step.all_classes << klass
    end

    # The class-level Plines step macros.
    module ClassMethods
      def dependency_declarations
        @dependency_declarations ||= []
      end

      def depends_on(*args, &block)
        args.each do |klass_name|
          depends_on do |data|
            StepInstance.new(module_namespace.const_get(klass_name), data)
          end
        end

        dependency_declarations << block if block
      end

      def dependencies_for(job_data)
        dependency_declarations.flat_map { |dd| dd[job_data] }
      end

      def has_no_dependencies?
        dependency_declarations.none?
      end

    private

      def module_namespace
        namespaces = name.split('::')
        namespaces.pop # ignore the last one
        namespaces.inject(Object) { |ns, mod| ns.const_get(mod) }
      end
    end
  end
end

