require 'spec_helper'
require 'plines'
require 'plines/job_batch'

module Plines
  describe JobBatch, :redis do
    it 'is uniquely identified by the id' do
      j1 = JobBatch.new("a")
      j2 = JobBatch.new("b")
      j3 = JobBatch.new("a")

      j1.should eq(j3)
      j1.should eql(j3)
      j1.should_not eq(j2)
      j1.should_not eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      set.map(&:object_id).should =~ [j1.object_id, j2.object_id]
    end

    describe ".create" do
      it "creates a redis set with all the given jids" do
        batch = JobBatch.create("foo", %w[ a b c ])
        Plines.redis.smembers("job_batch:foo:pending_job_jids").should =~ %w[ a b c ]
      end
    end

    describe "#job_jids" do
      it "returns all job jids, even when some have been completed" do
        batch = JobBatch.create("foo", %w[ a b c ])
        batch.job_jids.to_a.should =~ %w[ a b c ]
        batch.mark_job_as_complete("a")
        batch.job_jids.to_a.should =~ %w[ a b c ]
      end
    end

    describe "#mark_job_as_complete" do
      it "moves a jid from the pending to the complete set" do
        batch = JobBatch.create("foo", %w[ a ])

        batch.pending_job_jids.should include("a")
        batch.completed_job_jids.should_not include("a")

        batch.mark_job_as_complete("a")

        batch.pending_job_jids.should_not include("a")
        batch.completed_job_jids.should include("a")
      end

      it "raises an error if the given jid is not in the pending set" do
        batch = JobBatch.create("foo", [])
        batch.completed_job_jids.should_not include("a")
        expect { batch.mark_job_as_complete("a") }.to raise_error(ArgumentError)
        batch.completed_job_jids.should_not include("a")
      end
    end

    describe "#complete?" do
      it 'returns false when there are no pending or completed jobs' do
        batch = JobBatch.new("foo")
        batch.should_not be_complete
      end

      it 'returns false when there are pending jobs and completed jobs' do
        batch = JobBatch.new("foo")
        batch.pending_job_jids << "a"
        batch.completed_job_jids << "b"
        batch.should_not be_complete
      end

      it 'returns true when there are only completed jobs' do
        batch = JobBatch.new("foo")
        batch.completed_job_jids << "b"
        batch.should be_complete
      end
    end

    describe "#cancel!" do
      step_class(:Foo)
      let(:jid)    { Plines.default_queue.put(Foo, {}) }
      let!(:batch) { JobBatch.create("foo", [jid]) }

      it 'cancels all qless jobs' do
        Plines.default_queue.length.should be > 0
        batch.cancel!
        Plines.default_queue.length.should eq(0)
      end

      it 'keeps track of whether or not cancellation has occurred' do
        batch.should_not be_cancelled
        batch.cancel!
        batch.should be_cancelled
      end
    end
  end
end

