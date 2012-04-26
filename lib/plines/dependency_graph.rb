require 'set'

module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # StepInstances (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    attr_reader :steps

    # Raised when a circular dependency is detected.
    class CircularDependencyError < StandardError; end

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

      detect_circular_dependencies!
    end

  private

    def detect_circular_dependencies!
      @visited_steps = Set.new

      @steps.each do |step|
        next if @visited_steps.include?(step)
        depth_first_search_from(step)
      end
    end

    def depth_first_search_from(step, current_stack=Set.new)
      @visited_steps << step

      if current_stack.include?(step)
        raise CircularDependencyError, "Your graph appears to have a circular dependency: #{current_stack.inspect}"
      end

      step.dependencies.each do |dep|
        depth_first_search_from(dep, current_stack | [step])
      end
    end
  end
end

