require 'spec_helper'
require 'plines'
require 'qless'

module Plines
  describe JobPerformer do
    let(:qless_client) { stub("qless").as_null_object }
    let(:step_instance) { stub("step instance") }

    it 'runs the #perform method on an instance of the right class' do
      step_class = fire_replaced_class_double("MySteps::A")
      step_class.should_receive(:new).and_return(step_instance)
      step_instance.should_receive(:perform)

      job = Qless::Job.build(qless_client, JobPerformer, data: { "klass" => "MySteps::A" })
      job.perform
    end

    it 'makes job data available to the step via the job_data helper method' do
      jdata = nil
      step_class(:B) do
        define_method :perform do
          jdata = job_data
        end
      end

      job = Qless::Job.build(qless_client, JobPerformer, data: { "klass" => "B", "data" => { "a" => 3 } })
      job.perform
      jdata.should eq("a" => 3)
    end

    it 'makes an empty hash available as job data if no job data was enqueued with the job' do
      jdata = nil
      step_class(:B) do
        define_method :perform do
          jdata = job_data
        end
      end

      job = Qless::Job.build(qless_client, JobPerformer, data: { "klass" => "B"})
      job.perform
      jdata.should eq({})
    end
  end
end

