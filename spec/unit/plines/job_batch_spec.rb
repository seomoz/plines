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
      it "returns all job jids" do
        batch = JobBatch.create("foo", %w[ a b c ])
        batch.job_jids.to_a.should =~ %w[ a b c ]
      end
    end
  end
end

