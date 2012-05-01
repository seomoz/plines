require 'spec_helper'
require 'plines/job'
require 'plines/dependency_graph'
require 'plines/pipeline'
require 'plines/step'

module Plines
  describe DependencyGraph do
    describe ".new" do
      let(:graph) { DependencyGraph.new(P.step_classes, a: 10) }

      let(:steps_by) do
        Hash.new do |h, (klass, data)|
          h[[klass, data]] = graph.steps.find { |s| s.klass == klass && s.data[:a] == data }
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
            [ { a: data[:a] + 1 }, { a: data[:a] + 2 } ]
          end
        end

        step_class(:D); step_class(:E);

        step_class(:F) do
          fan_out do |data|
            [ { a: data[:a] * 1 }, { a: data[:a] * 2 } ]
          end
        end

        graph.steps.should eq([
          step(P::A),
          step(P::B), step(P::C, 11), step(P::C, 12), step(P::D),
          step(P::E), step(P::F, 10), step(P::F, 20)
        ])

        step(P::A).dependencies.to_a.should =~ [step(P::B), step(P::C, 11), step(P::C, 12), step(P::D)]
        step(P::A).dependents.to_a.should =~ []

        step(P::B).dependencies.to_a.should =~ [step(P::E)]
        step(P::B).dependents.to_a.should =~ [step(P::A)]

        step(P::C, 11).dependencies.to_a.should =~ [step(P::E), step(P::F, 10), step(P::F, 20)]
        step(P::C, 11).dependents.to_a.should =~ [step(P::A)]
        step(P::C, 12).dependencies.to_a.should =~ [step(P::E), step(P::F, 10), step(P::F, 20)]
        step(P::C, 12).dependents.to_a.should =~ [step(P::A)]

        step(P::D).dependencies.to_a.should =~ []
        step(P::D).dependents.to_a.should =~ [step(P::A)]

        step(P::E).dependencies.to_a.should =~ []
        step(P::E).dependents.to_a.should =~ [step(P::B), step(P::C, 11), step(P::C, 12)]

        step(P::F, 10).dependencies.to_a.should =~ []
        step(P::F, 10).dependents.to_a.should =~ [step(P::C, 11), step(P::C, 12)]
        step(P::F, 20).dependencies.to_a.should =~ []
        step(P::F, 20).dependents.to_a.should =~ [step(P::C, 11), step(P::C, 12)]
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

