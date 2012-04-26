require 'spec_helper'
require 'plines/job'

module Plines
  describe Job do
    step_class(:StepA)
    step_class(:StepB)

    let(:a1_1) { Job.build(StepA, 1) }
    let(:a1_2) { Job.build(StepA, 1) }
    let(:a2)   { Job.build(StepA, 2) }
    let(:b)    { Job.build(StepB, 1) }

    it 'is uniquely identified by the class/data combination' do
      steps = Set.new
      steps << a1_1 << a1_2 << a2 << b
      steps.size.should eq(3)
      steps.map(&:object_id).should =~ [a1_1, a2, b].map(&:object_id)
    end

    it 'initializes #dependencies and #dependees to empty sets' do
      b.dependencies.should eq(Set.new)
      b.dependees.should eq(Set.new)
    end

    it 'sets up the dependency/dependee relationship when a dependency is added' do
      a2.dependencies.should be_empty
      b.dependencies.should be_empty
      a2.add_dependency(b)
      a2.dependencies.to_a.should eq([b])
      b.dependees.to_a.should eq([a2])
    end

    it 'yields when constructed if passed a block' do
      yielded_object = nil
      si = Job.build(StepA, 5) { |a| yielded_object = a }
      yielded_object.should be(si)
    end

    it 'does not allow consumers to construct instances using .new (since we need accumulation behavior and we cannot override .new)' do
      expect { Job.new(StepA, 3) }.to raise_error(NoMethodError)
    end

    describe '.accumulate_instances' do
      it 'causes .build to return identical object instances for the same arguments for the duration of the block' do
        Job.build(StepA, 1).should_not be(Job.build(StepA, 1))
        s1 = s2 = nil

        Job.accumulate_instances do
          s1 = Job.build(StepA, 1)
          s2 = Job.build(StepA, 1)
        end

        s1.should be(s2)
      end

      it 'returns the accumulated instances' do
        s1 = s2 = s3 = nil

        instances = Job.accumulate_instances do
          s1 = Job.build(StepA, 1)
          s2 = Job.build(StepA, 1)
          s3 = Job.build(StepA, 2)
        end

        instances.should =~ [s1, s3]
      end

      it 'correctly restores the initial state if an error is raised in the block' do
        expect {
          Job.accumulate_instances { raise "boom" }
        }.to raise_error("boom")

        Job.build(StepA, 1).should_not be(Job.build(StepA, 1))
      end
    end
  end
end
