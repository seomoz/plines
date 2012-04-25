require 'spec_helper'
require 'plines'
require 'plines/job_batch'

module Plines
  describe JobBatch, :redis do
    describe ".create" do
      it "creates a redis set with all the given jids" do
        batch = JobBatch.create("foo", %w[ a b c ])
        pending_key = batch.send(:redis_key_for, "pending")
        Plines.redis.smembers(pending_key).should =~ %w[ a b c ]
      end
    end

    describe "#job_jids" do
      it "returns all job jids" do
        batch = JobBatch.create("foo", %w[ a b c ])
        batch.job_jids.should =~ %w[ a b c ]
      end
    end
  end
end

