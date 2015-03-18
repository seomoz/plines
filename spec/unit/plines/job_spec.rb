require 'plines/job'
require 'plines/pipeline'
require 'plines/step'
require 'plines/configuration'

module Plines
  RSpec.describe Job do
    step_class(:StepA)
    step_class(:StepB)
    step_class(:StepC)

    let(:a1_1) { Job.new(P::StepA, a: 1) }
    let(:a1_2) { Job.new(P::StepA, a: 1) }
    let(:a2)   { Job.new(P::StepA, a: 2) }
    let(:b)    { Job.new(P::StepB, a: 1) }
    let(:c)    { Job.new(P::StepC, a: 1) }

    it 'is uniquely identified by the class/data combination' do
      steps = Set.new
      steps << a1_1 << a1_2 << a2 << b
      expect(steps.size).to eq(3)
      expect(steps.map(&:object_id)).to match_array [a1_1, a2, b].map(&:object_id)
    end

    it 'raises an error if given data that is not a hash' do
      expect {
        Job.new(P::StepA, 5)
      }.to raise_error(NotAHashError)
    end

    it 'normally exposes #data as a normal hash' do
      expect(b.data[:a]).to eq(1)
      expect(b.data["a"]).to be_nil
    end

    it 'exposes #data as an indifferent hash if `config.expose_indifferent_hashes = true` is est' do
      P.configuration.expose_indifferent_hashes = true
      expect(b.data["a"]).to eq(1)
      expect(b.data[:a]).to eq(1)
    end

    it 'initializes #dependencies and #dependents to empty collections' do
      expect(b.dependencies).to be_empty
      expect(b.dependents).to be_empty
    end

    it 'sets up the dependency/dependent relationship when a dependency is added' do
      expect(a2.dependencies).to be_empty
      expect(b.dependencies).to be_empty
      a2.add_dependency(b)
      expect(a2.dependencies.to_a).to eq([b])
      expect(b.dependents.to_a).to eq([a2])
    end

    it 'yields when constructed if passed a block' do
      yielded_object = nil
      si = Job.new(P::StepA, a: 5) { |a| yielded_object = a }
      expect(yielded_object).to be(si)
    end

    describe "#external_dependencies" do
      it 'returns each of the external dependencies of the job' do
        step_class(:F) do
          has_external_dependencies do |deps|
            deps.add "foo"
            deps.add "bar"
          end
        end

        j = Job.new(P::F, a: 1)
        expect(j.external_dependencies.map(&:name)).to eq(["foo", "bar"])
      end

      it 'only includes external dependencies for which the conditional block returns true' do
        step_class(:F) do
          has_external_dependencies do |deps, data|
            deps.add "foo" if data[:foo]
            deps.add "bar" if data[:bar]
          end
        end

        j = Job.new(P::F, a: 1)
        expect(j.external_dependencies).to eq([])

        j = Job.new(P::F, foo: true)
        expect(j.external_dependencies.map(&:name)).to eq(["foo"])

        j = Job.new(P::F, foo: true, bar: true)
        expect(j.external_dependencies.map(&:name)).to eq(["foo", "bar"])
      end
    end
  end
end

