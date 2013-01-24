require 'spec_helper'
require 'plines/configuration'

module Plines
  describe Configuration do
    let(:config) { Configuration.new }

    describe "#qless" do
      it 'can be assigned' do
        qless = fire_double("Qless::Client")
        config.qless = qless
        expect(config.qless).to be(qless)
      end

      it 'constructs an instance using the assigned redis client' do
        config.redis = fire_double("Redis")

        qless_client_class = fire_replaced_class_double("Qless::Client")
        qless_client_class.should_receive(:new)
                          .with(redis: config.redis)
                          .and_return(:new_qless)

        expect(config.qless).to be(:new_qless)
      end

      it 'constructs a default instance if qless and redis are not assigned' do
        qless_client_class = fire_replaced_class_double("Qless::Client")
        qless_client_class.should_receive(:new).and_return(:default_client)
        expect(config.qless).to be(:default_client)
      end
    end

    describe "#redis" do
      it 'can be assigned' do
        redis = fire_double("Redis")
        config.redis = redis
        expect(config.redis).to be(redis)
      end

      it 'defaults to the redis client used by the assigned qless client' do
        redis = fire_double("Redis")
        qless = fire_double("Qless::Client", redis: redis)
        config.qless = qless
        expect(config.redis).to be(redis)
      end

      it 'constructs a default instance if qless and redis are not assigned' do
        redis_class = fire_replaced_class_double("Redis")
        redis_class.should_receive(:new).and_return(:new_redis)
        expect(config.redis).to be(:new_redis)
      end
    end

    describe "#batch_list_key_for" do
      it "returns the value constructed by the batch_list block" do
        config.batch_list_key do |batch_data|
          [batch_data.fetch("shard"), batch_data.fetch("campaign")].join('-')
        end

        expect(config.batch_list_key_for("shard" => "23", "campaign" => "12")).to eq("23-12")
      end

      it "raises an error by default" do
        expect {
          config.batch_list_key_for("shard" => "23", "campaign" => "12")
        }.to raise_error(Plines::Configuration::Error)
      end
    end

    describe "#data_ttl_in_milliseconds" do
      it "returns the equivalent of 2 months by default" do
        expected = 2 *  # months
                   30 * # days
                   24 * # hours
                   60 * # minutes
                   60 * # seconds
                   1000 # to milliseconds

        expect(config.data_ttl_in_milliseconds).to eq(expected)
      end

      it "returns 1000 times the data_ttl_in_seconds, rounded to the nearest integer" do
        config.data_ttl_in_seconds = Math::PI
        expect(config.data_ttl_in_milliseconds).to eq(3141)
      end
    end

    describe "#qless_job_options_block" do
      it "returns the configured block" do
        block = lambda { }
        config.qless_job_options(&block)
        expect(config.qless_job_options_block).to be(block)
      end

      it "returns a block that returns an empty hash by default" do
        expect(config.qless_job_options_block.call(stub)).to eq({})
      end
    end

    describe "#after_job_batch_cancellation" do
      let(:the_job_batch) { double }

      it 'adds a callback that can be invoked via #notify(:after_job_batch_cancellation)' do
        hook_1_batch = hook_2_batch = nil
        config.after_job_batch_cancellation { |jb| hook_1_batch = jb }
        config.after_job_batch_cancellation { |jb| hook_2_batch = jb }

        config.notify(:after_job_batch_cancellation, the_job_batch)

        expect(hook_1_batch).to be(the_job_batch)
        expect(hook_2_batch).to be(the_job_batch)
      end
    end
  end
end

