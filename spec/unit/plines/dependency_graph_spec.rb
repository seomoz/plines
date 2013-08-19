require 'spec_helper'
require 'plines/job'
require 'plines/dependency_graph'
require 'plines/pipeline'
require 'plines/step'

module Plines
  describe DependencyGraph do
    describe ".new" do
      let(:graph) { DependencyGraph.new(P, a: 10) }

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

        expect(graph.steps).to eq([
          step(P::A),
          step(P::B), step(P::C, 11), step(P::C, 12), step(P::D),
          step(P::E), step(P::F, 10), step(P::F, 20)
        ])

        expect(step(P::A).dependencies.to_a).to match_array [step(P::B), step(P::C, 11), step(P::C, 12), step(P::D)]
        expect(step(P::A).dependents.to_a).to match_array []

        expect(step(P::B).dependencies.to_a).to match_array [step(P::E)]
        expect(step(P::B).dependents.to_a).to match_array [step(P::A)]

        expect(step(P::C, 11).dependencies.to_a).to match_array [step(P::E), step(P::F, 10), step(P::F, 20)]
        expect(step(P::C, 11).dependents.to_a).to match_array [step(P::A)]
        expect(step(P::C, 12).dependencies.to_a).to match_array [step(P::E), step(P::F, 10), step(P::F, 20)]
        expect(step(P::C, 12).dependents.to_a).to match_array [step(P::A)]

        expect(step(P::D).dependencies.to_a).to match_array []
        expect(step(P::D).dependents.to_a).to match_array [step(P::A)]

        expect(step(P::E).dependencies.to_a).to match_array []
        expect(step(P::E).dependents.to_a).to match_array [step(P::B), step(P::C, 11), step(P::C, 12)]

        expect(step(P::F, 10).dependencies.to_a).to match_array []
        expect(step(P::F, 10).dependents.to_a).to match_array [step(P::C, 11), step(P::C, 12)]
        expect(step(P::F, 20).dependencies.to_a).to match_array []
        expect(step(P::F, 20).dependents.to_a).to match_array [step(P::C, 11), step(P::C, 12)]
      end

      it 'correctly sets up individual dependencies on fan_out steps' do
        [:A, :B].each do |klass|
          step_class(klass) do
            fan_out do |data|
              [ { a: data[:a] + 1 }, { a: data[:a] + 2 } ]
            end
          end
        end

        P::A.depends_on :B do |data|
          data.my_data == data.their_data
        end

        expect(graph.steps).to eq([
          step(P::A, 11), step(P::A, 12),
          step(P::B, 11), step(P::B, 12)
        ])

        expect(step(P::A, 11).dependencies.to_a).to eq([step(P::B, 11)])
        expect(step(P::A, 12).dependencies.to_a).to eq([step(P::B, 12)])
        expect(step(P::B, 11).dependents.to_a).to eq([step(P::A, 11)])
        expect(step(P::B, 12).dependents.to_a).to eq([step(P::A, 12)])
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

      it 'adds the correct dependencies to all terminal jobs' do
        step_class(:A)
        step_class(:B)  { depends_on :A }
        step_class(:C)  { depends_on :A }
        step_class(:D)  { depends_on :B }
        step_class(:Terminal) do
          depends_on_all_steps
          fan_out do |data|
            [ { a: data[:a] + 1 }, { a: data[:a] + 2 } ]
          end
        end

        expect(step(P::Terminal, 11).dependencies.to_a).to match_array([step(P::D), step(P::C)])
        expect(step(P::Terminal, 12).dependencies.to_a).to match_array([step(P::D), step(P::C)])
      end

      it 'can return the steps in correct dependency order via an enumerable' do
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

        step_class(:G) { depends_on :D, :E }

        # these steps are totally separate from the above steps
        step_class(:H) { depends_on :I, :J }
        step_class(:I)
        step_class(:J) { depends_on :I }

        # index_for hash will allow quick lookup to determine the index of a step
        index_for = {}
        graph.ordered_steps.each_with_index do |step, index|
          index_for[step] = index
        end

        expect(index_for[step(P::A)]).to be > index_for[step(P::B)]
        expect(index_for[step(P::A)]).to be > index_for[step(P::C, 11)]
        expect(index_for[step(P::A)]).to be > index_for[step(P::C, 12)]
        expect(index_for[step(P::A)]).to be > index_for[step(P::D)]

        expect(index_for[step(P::B)]).to be > index_for[step(P::E)]

        expect(index_for[step(P::C, 11)]).to be > index_for[step(P::E)]
        expect(index_for[step(P::C, 11)]).to be > index_for[step(P::F, 10)]
        expect(index_for[step(P::C, 11)]).to be > index_for[step(P::F, 20)]

        expect(index_for[step(P::C, 12)]).to be > index_for[step(P::E)]
        expect(index_for[step(P::C, 12)]).to be > index_for[step(P::F, 10)]
        expect(index_for[step(P::C, 12)]).to be > index_for[step(P::F, 20)]

        expect(index_for[step(P::G)]).to be > index_for[step(P::D)]
        expect(index_for[step(P::G)]).to be > index_for[step(P::E)]

        expect(index_for[step(P::H)]).to be > index_for[step(P::I)]
        expect(index_for[step(P::H)]).to be > index_for[step(P::J)]

        expect(index_for[step(P::J)]).to be > index_for[step(P::I)]
      end

      context 'when a step depends on a 0-fan-out step' do
        before do
          step_class(:A)

          step_class(:B) { depends_on :A; fan_out { [] } }
          step_class(:C) { depends_on :B; fan_out { [] } }
          step_class(:D) { depends_on :A }
          step_class(:E) { depends_on :C, :D }

          # E(1) --> C(0) --> B(0) --> A(1)
          #  \-------> D(1) -------/

          ::Kernel.stub(:warn)
        end

        it 'treats step dependencies transitively' do
          expect(graph.steps).to eq([step(P::A), step(P::D), step(P::E)])

          expect(step(P::A).dependencies.to_a).to match_array []
          expect(step(P::A).dependents.to_a).to match_array [step(P::D), step(P::E)]

          expect(step(P::D).dependencies.to_a).to match_array [step(P::A)]
          expect(step(P::D).dependents.to_a).to match_array [step(P::E)]

          expect(step(P::E).dependencies.to_a).to match_array [step(P::D), step(P::A)]
          expect(step(P::E).dependents.to_a).to match_array []
        end

        it 'prints a warning when inferring the transitive dependencies' do
          ::Kernel.should_receive(:warn).with(/transitive dependency/i)
          graph.steps
        end
      end
    end
  end
end

