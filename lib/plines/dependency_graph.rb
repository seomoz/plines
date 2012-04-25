module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # StepInstances (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    def initialize
      @step_repository = Hash.new { |h,k| h[k] = StepInstance.new(*k) }
      yield self if block_given?
    end

    def step_for(*args)
      @step_repository[args]
    end

    def steps
      @step_repository.values
    end

    def self.build_for(job_data)
      new do |graph|
        Plines::Step.all.each do |step_klass|
          step = graph.step_for(step_klass, job_data)
          step_klass.dependencies_for(job_data).each do |dep|
            step.add_dependency(graph.step_for(dep.klass, dep.data))
          end
        end
      end
    end
  end
end

