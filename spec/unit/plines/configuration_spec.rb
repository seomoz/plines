require 'spec_helper'
require 'plines/configuration'

module Plines
  describe Configuration do
    let(:config) { Configuration.new }

    describe "#qless_client_for" do
      it 'returns the value returned by the qless_client block' do
        config.qless_client do |key|
          fire_double("Redis", id: "redis://host-#{key}:1234/0")
        end

        expect(config.qless_client_for("foo").id).to include("host-foo")
        expect(config.qless_client_for("bar").id).to include("host-bar")
      end

      it 'raises an error by default' do
        expect {
          config.qless_client_for("foo")
        }.to raise_error(Plines::Configuration::Error)
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

