require 'set'

module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # Jobs (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    attr_reader :steps

    # Raised when a circular dependency is detected.
    class CircularDependencyError < StandardError; end

    def initialize(step_classes, batch_data)
      @steps = Job.accumulate_instances do
        step_classes.each do |step_klass|
          step_klass.jobs_for(batch_data).each do |job|
            job.add_dependencies_for(batch_data)
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
        raise CircularDependencyError,
          "Your graph appears to have a circular dependency: " +
          current_stack.inspect
      end

      step.dependencies.each do |dep|
        depth_first_search_from(dep, current_stack | [step])
      end
    end
  end
end

