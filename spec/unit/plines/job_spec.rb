require 'spec_helper'
require 'plines/job'
require 'plines/pipeline'
require 'plines/step'

module Plines
  describe Job do
    step_class(:StepA)
    step_class(:StepB)
    step_class(:StepC)

    let(:a1_1) { Job.build(P::StepA, a: 1) }
    let(:a1_2) { Job.build(P::StepA, a: 1) }
    let(:a2)   { Job.build(P::StepA, a: 2) }
    let(:b)    { Job.build(P::StepB, a: 1) }
    let(:c)    { Job.build(P::StepC, a: 1) }

    it 'is uniquely identified by the class/data combination' do
      steps = Set.new
      steps << a1_1 << a1_2 << a2 << b
      expect(steps.size).to eq(3)
      expect(steps.map(&:object_id)).to match_array [a1_1, a2, b].map(&:object_id)
    end

    it 'raises an error if given data that is not a hash' do
      expect {
        Job.build(P::StepA, 5)
      }.to raise_error(IndifferentHash::NotAHashError)
    end

    it 'exposes #data as an indifferent hash' do
      expect(b.data[:a]).to eq(1)
      expect(b.data["a"]).to eq(1)
    end

    it 'initializes #dependencies and #dependents to empty sets' do
      expect(b.dependencies).to eq(Set.new)
      expect(b.dependents).to eq(Set.new)
    end

    it 'sets up the dependency/dependent relationship when a dependency is added' do
      expect(a2.dependencies).to be_empty
      expect(b.dependencies).to be_empty
      a2.add_dependency(b)
      expect(a2.dependencies.to_a).to eq([b])
      expect(b.dependents.to_a).to eq([a2])
    end

    it 'modifies the dependency and dependent when a dependency is removed' do
      a2.add_dependency(b)
      a2.add_dependency(c)
      c.add_dependency(b)
      a2.remove_dependency(b)
      expect(a2.dependencies.to_a).to eq([c])
      expect(b.dependents.to_a).to eq([c])
    end

    it 'raises a helpful error if a nonexistent dependency is removed' do
      expect(a2.dependencies).not_to include(b)
      expect {
        a2.remove_dependency(b)
      }.to raise_error(/attempted to remove nonexistent dependency/i)
    end

    it 'yields when constructed if passed a block' do
      yielded_object = nil
      si = Job.build(P::StepA, a: 5) { |a| yielded_object = a }
      expect(yielded_object).to be(si)
    end

    it 'does not allow consumers to construct instances using .new (since we need accumulation behavior and we cannot override .new)' do
      expect { Job.new(P::StepA, a: 3) }.to raise_error(NoMethodError)
    end

    describe '.accumulate_instances' do
      it 'causes .build to return identical object instances for the same arguments for the duration of the block' do
        expect(Job.build(P::StepA, a: 1)).not_to be(Job.build(P::StepA, a: 1))
        s1 = s2 = nil

        Job.accumulate_instances do
          s1 = Job.build(P::StepA, a: 1)
          s2 = Job.build(P::StepA, a: 1)
        end

        expect(s1).to be(s2)
      end

      it 'returns the accumulated instances' do
        s1 = s2 = s3 = nil

        instances = Job.accumulate_instances do
          s1 = Job.build(P::StepA, a: 1)
          s2 = Job.build(P::StepA, a: 1)
          s3 = Job.build(P::StepA, a: 2)
        end

        expect(instances).to match_array [s1, s3]
      end

      it 'correctly restores the initial state if an error is raised in the block' do
        expect {
          Job.accumulate_instances { raise "boom" }
        }.to raise_error("boom")

        expect(Job.build(P::StepA, a: 1)).not_to be(Job.build(P::StepA, a: 1))
      end
    end

    describe "#external_dependencies" do
      it 'returns each of the external dependencies of the job' do
        step_class(:F) do
          has_external_dependencies do |deps|
            deps.add "foo"
            deps.add "bar"
          end
        end

        j = Job.build(P::F, a: 1)
        expect(j.external_dependencies.map(&:name)).to eq(["foo", "bar"])
      end

      it 'only includes external dependencies for which the conditional block returns true' do
        step_class(:F) do
          has_external_dependencies do |deps, data|
            deps.add "foo" if data[:foo]
            deps.add "bar" if data[:bar]
          end
        end

        j = Job.build(P::F, a: 1)
        expect(j.external_dependencies).to eq([])

        j = Job.build(P::F, foo: true)
        expect(j.external_dependencies.map(&:name)).to eq(["foo"])

        j = Job.build(P::F, foo: true, bar: true)
        expect(j.external_dependencies.map(&:name)).to eq(["foo", "bar"])
      end
    end
  end
end

