require 'plines/dependency_graph'

module Plines
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

  module Step
    def self.all
      @all ||= []
    end

    def self.to_dependency_graph(job_data)
      DependencyGraph.new do |graph|
        all.each do |step_klass|
          step = graph.step_for(step_klass, job_data)
          step_klass.dependencies_for(job_data).each do |dep|
            step.add_dependency(graph.step_for(dep.klass, dep.data))
          end
        end
      end
    end

    def self.included(klass)
      klass.extend ClassMethods
      Plines::Step.all << klass
    end

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

