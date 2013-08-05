require 'set'

module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # Jobs (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    attr_reader :steps # FYI, steps is not ordered according to dependencies

    # Raised when a circular dependency is detected.
    class CircularDependencyError < StandardError; end

    def initialize(pipeline, batch_data)
      step_classes = pipeline.step_classes

      @steps = Job.accumulate_instances do
        step_classes.each do |step_klass|
          step_klass.jobs_for(batch_data).each do |job|
            job.add_dependencies_for(batch_data)
          end
        end

        @terminal_jobs = pipeline.terminal_step.jobs_for(batch_data)
      end

      setup_terminal_dependencies
      cleanup_and_validate_dependencies!
    end

    def ordered_steps
      visited = Set.new
      Enumerator.new do |yielder|
        steps.each do |step|
          yield_next_ordered_step_for(step, visited, yielder)
        end
      end
    end

  private

    def cleanup_and_validate_dependencies!
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

    def setup_terminal_dependencies
      @steps.each do |job|
        if job.dependents.none? && !@terminal_jobs.include?(job)
          @terminal_jobs.each { |term_job| term_job.add_dependency(job) }
        end
      end
    end

    def yield_next_ordered_step_for(step, visited, yielder)
      return if visited.include?(step)

      step.dependencies.each do |dependency|
        yield_next_ordered_step_for(dependency, visited, yielder)
      end

      visited << step
      yielder.yield step
    end
  end
end

