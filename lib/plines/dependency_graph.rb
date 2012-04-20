module Plines
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
  end
end

