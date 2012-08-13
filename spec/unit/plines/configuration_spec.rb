require 'spec_helper'
require 'plines/configuration'

module Plines
  describe Configuration do
    let(:config) { Configuration.new }

    describe "#batch_list_for_for" do
      it "returns the value constructed by the batch_list block" do
        config.batch_list_key do |batch_data|
          [batch_data.fetch("shard"), batch_data.fetch("campaign")].join('-')
        end

        config.batch_list_key_for("shard" => "23", "campaign" => "12").should eq("23-12")
      end

      it "raises an error by default" do
        expect {
          config.batch_list_key_for("shard" => "23", "campaign" => "12").should eq("23-12")
        }.to raise_error(Plines::Configuration::Error)
      end
    end

    describe "#data_ttl_in_milliseconds" do
      it "returns the equivalent of 6 months by default" do
        expected = 6 *  # months
                   30 * # days
                   24 * # hours
                   60 * # minutes
                   60 * # seconds
                   1000 # to milliseconds

        config.data_ttl_in_milliseconds.should eq(expected)
      end

      it "returns 1000 times the data_ttl_in_seconds, rounded to the nearest integer" do
        config.data_ttl_in_seconds = Math::PI
        config.data_ttl_in_milliseconds.should eq(3141)
      end
    end

    describe "#qless_job_options_block" do
      it "returns the configured block" do
        block = lambda { }
        config.qless_job_options(&block)
        config.qless_job_options_block.should be(block)
      end

      it "returns a block that returns an empty hash by default" do
        config.qless_job_options_block.call(stub).should eq({})
      end
    end
  end
end

