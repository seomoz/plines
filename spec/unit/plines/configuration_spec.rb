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
  end
end

