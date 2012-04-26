require 'spec_helper'
require 'plines'

module Plines
  describe DependencyGraph do
    describe ".new" do
      let(:graph) { DependencyGraph.new(10) }

      let(:steps_by) do
        Hash.new do |h, (klass, data)|
          h[[klass, data]] = graph.steps.find { |s| s.klass == klass && s.data == data }
        end
      end

      def step(klass, data = 10)
        steps_by[[klass, data]]
      end

      it 'constructs a full dependency graph from the given declarations' do
        step_class(:A) { depends_on :B, :C, :D }
        step_class(:B) { depends_on :E }

        step_class(:C) do
          depends_on :E, :F
          fan_out do |data|
            [data + 1, data + 2]
          end
        end

        step_class(:D); step_class(:E);

        step_class(:F) do
          fan_out do |data|
            [data * 1, data * 2]
          end
        end

        graph.steps.should eq([
          step(A),
          step(B), step(C, 11), step(C, 12), step(D),
          step(E), step(F, 10), step(F, 20)
        ])

        step(A).dependencies.to_a.should =~ [step(B), step(C, 11), step(C, 12), step(D)]
        step(A).dependees.to_a.should =~ []

        step(B).dependencies.to_a.should =~ [step(E)]
        step(B).dependees.to_a.should =~ [step(A)]

        step(C, 11).dependencies.to_a.should =~ [step(E), step(F, 10), step(F, 20)]
        step(C, 11).dependees.to_a.should =~ [step(A)]
        step(C, 12).dependencies.to_a.should =~ [step(E), step(F, 10), step(F, 20)]
        step(C, 12).dependees.to_a.should =~ [step(A)]

        step(D).dependencies.to_a.should =~ []
        step(D).dependees.to_a.should =~ [step(A)]

        step(E).dependencies.to_a.should =~ []
        step(E).dependees.to_a.should =~ [step(B), step(C, 11), step(C, 12)]

        step(F, 10).dependencies.to_a.should =~ []
        step(F, 10).dependees.to_a.should =~ [step(C, 11), step(C, 12)]
        step(F, 20).dependencies.to_a.should =~ []
        step(F, 20).dependees.to_a.should =~ [step(C, 11), step(C, 12)]
      end

      it 'detects direct circular dependencies' do
        step_class(:X) { depends_on :Y }
        step_class(:Y) { depends_on :X }

        expect { graph }.to raise_error(DependencyGraph::CircularDependencyError)
      end

      it 'detects indirect circular dependencies' do
        step_class(:W) { depends_on :X }
        step_class(:X) { depends_on :Y }
        step_class(:Y) { depends_on :Z }
        step_class(:Z) { depends_on :W }

        expect { graph }.to raise_error(DependencyGraph::CircularDependencyError)
      end
    end
  end
end

