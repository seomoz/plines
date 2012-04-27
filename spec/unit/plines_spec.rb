require 'spec_helper'
require 'plines'

describe Plines do
  describe ".qless" do
    before      { Plines.instance_variable_set(:@qless, nil) }
    after(:all) { Plines.instance_variable_set(:@qless, nil) }

    it 'returns a memoized Qless::Client instance' do
      Plines.qless.should be_a(Qless::Client)
      Plines.qless.should be(Plines.qless)
    end

    it 'can be overridden' do
      orig_instance = Plines.qless
      new_instance = Qless::Client.new
      Plines.qless = new_instance
      Plines.qless.should be(new_instance)
      Plines.qless.should_not be(orig_instance)
    end
  end

  describe ".default_queue" do
    it "returns the 'plines' queue, memoized" do
      Plines.default_queue.should be_a(Qless::Queue)
      Plines.default_queue.should be(Plines.default_queue)
      Plines.default_queue.name.should eq("plines")
    end
  end

  describe ".configuration" do
    it "returns a memoized Plines::Configuration instance" do
      Plines.configuration.should be_a(Plines::Configuration)
      Plines.configuration.should be(Plines.configuration)
    end
  end

  describe ".configure" do
    it "yields the configuration object" do
      yielded = nil
      Plines.configure { |c| yielded = c }
      yielded.should be(Plines.configuration)
    end
  end

  describe ".redis" do
    it "returns the qless redis" do
      Plines.redis.should be(Plines.qless.redis)
    end
  end

  describe ".most_recent_job_batch_for", :redis do
    it 'returns a job batch using the configured key' do
      Plines.configuration.batch_list_key { |data| data["a"] }
      Plines::JobBatchList.new("foo").create_new_batch([])
      batch = Plines.most_recent_job_batch_for("a" => "foo")
      batch.should be_a(Plines::JobBatch)
      batch.id.should include("foo")
    end
  end

  describe ".enqueue_jobs_for" do
    it 'enqueues jobs for the given batch data' do
      enqueuer_class = fire_replaced_class_double("Plines::JobEnqueuer")
      enqueuer = fire_double("Plines::JobEnqueuer")

      enqueuer_class.should_receive(:new).with("job" => "data") { enqueuer }
      enqueuer.should_receive(:enqueue_jobs)

      Plines.enqueue_jobs_for("job" => "data")
    end
  end
end

