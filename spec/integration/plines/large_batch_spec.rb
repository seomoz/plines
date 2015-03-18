require 'plines'

RSpec.describe "A large Plines job batch", :redis do
  module LargePipeline
    extend Plines::Pipeline

    def self.fan_out_by_groups_and_count(klass)
      klass.fan_out do |batch_data|
        batch_data.fetch("groups").map do |group|
          batch_data.merge("group" => group)
        end
      end

      klass.fan_out do |batch_data|
        batch_data.fetch("fan_out_count").times.map do |i|
          batch_data.merge("counter" => i)
        end
      end
    end

    class RootStep
      extend Plines::Step
      depended_on_by_all_steps
      def perform; end
    end

    class Step1
      extend Plines::Step
      LargePipeline.fan_out_by_groups_and_count(self)
      def perform; end
    end

    class Step2
      extend Plines::Step
      LargePipeline.fan_out_by_groups_and_count(self)

      # for more completeness, it would be nice to have this but it makes it much, much slower...
      #depends_on :Step1 do |data|
        #data.my_data.fetch("group") == data.their_data.fetch("group")
      #end

      def perform; end
    end

    class FinalStep
      extend Plines::Step
      depends_on_all_steps
      def perform; end
    end
  end

  before do
    LargePipeline.configure do |plines|
      plines.batch_list_key { "key" }
      plines.qless_client   { qless }
    end
  end

  it 'can be deleted' do
    job_batch = LargePipeline.enqueue_jobs_for("groups" => %w[ a b c d e ], "fan_out_count" => 800)

    expect {
      job_batch.cancel!
    }.to change { job_batch.pending_job_jids.count }.from(a_value > 8_000).to(0)
  end
end
