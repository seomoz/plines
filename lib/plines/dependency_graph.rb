module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # StepInstances (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    attr_reader :steps

    def initialize(batch_data)
      @steps = StepInstance.accumulate_instances do
        Plines::Step.all_classes.each do |step_klass|
          dependencies = step_klass.dependencies_for(batch_data)
          step_klass.step_instances_for(batch_data).each do |step|
            dependencies.each do |dep|
              step.add_dependency(dep)
            end
          end
        end
      end
    end
  end
end

