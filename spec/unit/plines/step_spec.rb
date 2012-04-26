require 'spec_helper'
require 'plines/step'
require 'set'

module Plines
  describe Step do
    describe ".all_classes" do
      step_class(:StepA)
      step_class(:StepB)

      it 'includes all classes that include the Plines::Step module' do
        Plines::Step.all_classes.should eq([StepA, StepB])
      end
    end

    describe "#jobs_for" do
      it 'returns just 1 instance w/ the given data by default' do
        step_class(:A)
        instances = A.jobs_for("a")
        instances.map(&:klass).should eq([A])
        instances.map(&:data).should eq(["a"])
      end

      it 'returns one instance per array entry returned by the fan_out block' do
        step_class(:A) do
          fan_out do |data|
            [data + 1, data + 2]
          end
        end

        instances = A.jobs_for(3)
        instances.map(&:klass).should eq([A, A])
        instances.map(&:data).should eq([4, 5])
      end
    end

    describe "#dependencies_for" do
      it "returns an empty array for a step with no declared dependencies" do
        step_class(:StepFoo)
        StepFoo.dependencies_for(:data).to_a.should eq([])
      end
    end

    describe "#has_no_dependencies?" do
      step_class(:StepA)

      it "returns true for steps that have no dependencies" do
        StepA.should have_no_dependencies
      end

      it "returns false for steps that have dependencies" do
        step_class(:StepC) { depends_on :StepA }
        StepC.should_not have_no_dependencies
      end
    end

    describe "#depends_on" do
      step_class(:StepA)
      step_class(:StepB)

      it "adds dependencies based on the given class name" do
        step_class(:StepC) do
          depends_on :StepA, :StepB
        end

        dependencies = StepC.dependencies_for(:foo)
        dependencies.map(&:klass).should eq([StepA, StepB])
        dependencies.map(&:data).should eq([:foo, :foo])
      end

      it "resolves step class names in the enclosing module" do
        stub_const("MySteps::A", Class.new { include Plines::Step })

        stub_const("MySteps::B", Class.new {
          include Plines::Step
          depends_on :A
        })

        dependencies = MySteps::B.dependencies_for([])
        dependencies.map(&:klass).should eq([MySteps::A])
      end

      context 'when depending on a fan_out step' do
        step_class(:StepA) do
          fan_out do |data|
            [data + 1, data + 2, data + 3]
          end
        end

        it "depends on all of the step instances of the named type when it fans out into multiple instances" do
          step_class(:StepC) do
            depends_on :StepA
          end

          dependencies = StepC.dependencies_for(17)
          dependencies.map(&:klass).should eq([StepA, StepA, StepA])
          dependencies.map(&:data).should eq([18, 19, 20])
        end

        it "depends on the the subset of instances for which the block returns true when given a block" do
          step_class(:StepC) do
            depends_on(:StepA, &:even?)
          end

          dependencies = StepC.dependencies_for(17)
          dependencies.map(&:klass).should eq([StepA, StepA])
          dependencies.map(&:data).should eq([18, 20])
        end
      end
    end
  end
end

