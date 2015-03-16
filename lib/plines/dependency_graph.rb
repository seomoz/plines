require 'set'
require 'tsort'

module Plines
  # Represents a dependency graph of Plines steps. This graph contains
  # Jobs (i.e. Step classes paired with data). The graph
  # takes care of preventing duplicate step instances.
  class DependencyGraph
    attr_reader :steps, :ordered_steps

    # Raised when a circular dependency is detected.
    class CircularDependencyError < StandardError; end

    def initialize(pipeline, batch_data)
      jobs_by_klass = self.class.jobs_by_klass_for(pipeline, batch_data)

      jobs_by_klass.values.flatten.each do |job|
        job.add_dependencies_for(batch_data, jobs_by_klass)
      end

      @terminal_jobs = jobs_by_klass.fetch(pipeline.terminal_step)
      @steps = jobs_by_klass.values.flatten

      setup_terminal_dependencies
      detect_circular_dependencies!
    end

  private

    def self.jobs_by_klass_for(pipeline, batch_data)
      pipeline.step_classes.each_with_object(
        Pipeline::NullStep => []
      ) do |step_klass, hash|
        hash[step_klass] = step_klass.jobs_for(batch_data)
      end
    end

    def detect_circular_dependencies!
      @ordered_steps = tsort
    rescue TSort::Cyclic => e
      raise CircularDependencyError, e.message
    end

    def setup_terminal_dependencies
      @steps.each do |job|
        if job.dependents.none? && !@terminal_jobs.include?(job)
          @terminal_jobs.each { |term_job| term_job.add_dependency(job) }
        end
      end
    end

    include TSort

    def tsort_each_node(&block)
      @steps.each(&block)
    end

    def tsort_each_child(step, &block)
      step.dependencies.each(&block)
    end
  end
end

