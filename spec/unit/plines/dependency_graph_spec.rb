require 'spec_helper'
require 'plines'

module Plines
  describe DependencyGraph do
    describe ".build_from" do
      step_class(:A) { depends_on :B, :C, :D }
      step_class(:B) { depends_on :E }
      step_class(:C) { depends_on :E, :F }
      step_class(:D); step_class(:E); step_class(:F)
      let(:graph) { DependencyGraph.build_for("args") }
      let(:steps_by_klass) { Hash.new { |h, k| h[k] = graph.steps.find { |s| s.klass == k } } }

      def step(klass)
        steps_by_klass[klass]
      end

      it 'constructs a full dependency graph from the given declarations' do
        graph.should have(6).steps

        step(A).dependencies.to_a.should =~ [step(B), step(C), step(D)]
        step(A).dependees.to_a.should =~ []

        step(B).dependencies.to_a.should =~ [step(E)]
        step(B).dependees.to_a.should =~ [step(A)]

        step(C).dependencies.to_a.should =~ [step(E), step(F)]
        step(C).dependees.to_a.should =~ [step(A)]

        step(D).dependencies.to_a.should =~ []
        step(D).dependees.to_a.should =~ [step(A)]

        step(E).dependencies.to_a.should =~ []
        step(E).dependees.to_a.should =~ [step(B), step(C)]

        step(F).dependencies.to_a.should =~ []
        step(F).dependees.to_a.should =~ [step(C)]
      end
    end
  end
end

