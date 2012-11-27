require 'spec_helper'
require 'plines/pipeline'
require 'plines/job_batch'
require 'plines/job_batch_list'

module Plines
  describe JobBatchList, :redis do
    let(:foo) { JobBatchList.new(pipeline_module, "foo") }
    let(:bar) { JobBatchList.new(pipeline_module, "bar") }

    it 'remembers the pipeline it is from' do
      jbl = JobBatchList.new(pipeline_module, "foo")
      jbl.pipeline.should be(pipeline_module)
    end

    it 'is uniquely identified by the id and pipeline' do
      j1 = JobBatchList.new(pipeline_module, "a")
      j2 = JobBatchList.new(pipeline_module, "b")
      j3 = JobBatchList.new(pipeline_module, "a")

      j1.should eq(j3)
      j1.should eql(j3)
      j1.should_not eq(j2)
      j1.should_not eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      set.map(&:object_id).should =~ [j1.object_id, j2.object_id]
    end

    describe "#create_new_batch" do
      it 'creates each new batch with a unique ascending id' do
        foo.create_new_batch.id.should eq("P:foo:1")
        bar.create_new_batch.id.should eq("P:bar:1")
        foo.create_new_batch.id.should eq("P:foo:2")
        bar.create_new_batch.id.should eq("P:bar:2")
      end
    end

    describe "#most_recent_batch" do
      it 'returns nil if there are no batches for the given id' do
        foo.create_new_batch
        bar.most_recent_batch.should be_nil
      end

      it 'returns the most recently created batch for the given id' do
        b1 = foo.create_new_batch
        b2 = foo.create_new_batch
        bar.create_new_batch

        foo.most_recent_batch.should eq(b2)
      end
    end

    it 'can enumerate all existing job batches' do
      foo.create_new_batch
      foo.create_new_batch

      foo.map(&:id).should eq(%w[ P:foo:1 P:foo:2 ])
    end

    it 'can return a lazy enumerator' do
      foo.create_new_batch
      foo.create_new_batch

      list = foo.each
      list.map(&:id).should eq(%w[ P:foo:1 P:foo:2 ])
    end
  end
end

