require 'spec_helper'
require 'plines/job_batch_list'

module Plines
  describe JobBatchList, :redis do
    let(:foo) { JobBatchList.new("foo") }
    let(:bar) { JobBatchList.new("bar") }

    it 'is uniquely identified by the id' do
      j1 = JobBatchList.new("a")
      j2 = JobBatchList.new("b")
      j3 = JobBatchList.new("a")

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
        foo.create_new_batch([]).id.should eq("foo:1")
        bar.create_new_batch([]).id.should eq("bar:1")
        foo.create_new_batch([]).id.should eq("foo:2")
        bar.create_new_batch([]).id.should eq("bar:2")
      end

      it 'creates the batch with the given jids' do
        batch = foo.create_new_batch(%w[ a b ])
        batch.job_jids.to_a.should =~ %w[ a b ]
      end
    end

    describe "#most_recent_batch" do
      it 'returns nil if there are no batches for the given id' do
        foo.create_new_batch([])
        bar.most_recent_batch.should be_nil
      end

      it 'returns the most recently created batch for the given id' do
        b1 = foo.create_new_batch([])
        b2 = foo.create_new_batch([])
        bar.create_new_batch([])

        foo.most_recent_batch.should eq(b2)
      end
    end
  end
end

