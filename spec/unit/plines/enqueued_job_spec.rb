require 'spec_helper'
require 'plines/enqueued_job'

module Plines
  describe EnqueuedJob, :redis do
    it 'is uniquely identified by the jid' do
      j1 = EnqueuedJob.new("a")
      j2 = EnqueuedJob.new("b")
      j3 = EnqueuedJob.new("a")

      j1.should eq(j3)
      j1.should eql(j3)
      j1.should_not eq(j2)
      j1.should_not eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      set.map(&:object_id).should =~ [j1.object_id, j2.object_id]
    end

    it 'provides a jid accessor' do
      j1 = EnqueuedJob.new("abc")
      j1.jid.should eq("abc")
    end

    describe "#create" do
      it "creates an EnqueuedJob with the given external dependencies" do
        EnqueuedJob.create("abc", :foo, :bar)
        ej = EnqueuedJob.new("abc")
        ej.pending_external_dependencies.should =~ [:foo, :bar]
      end
    end

    describe "#resolve_external_dependency" do
      let(:jid) { "abc" }

      it 'marks the external dependency as being resolved' do
        EnqueuedJob.create(jid, :foo, :bar)
        ej = EnqueuedJob.new(jid)
        ej.resolve_external_dependency(:bar)
        ej.pending_external_dependencies.should eq([:foo])
        ej.resolved_external_dependencies.should eq([:bar])
      end

      it 'yields when all external dependencies are resolved' do
        EnqueuedJob.create(jid, :foo, :bar)
        ej = EnqueuedJob.new(jid)

        yielded = false
        ej.resolve_external_dependency(:bar) { yielded = true }
        yielded.should be_false
        ej.resolve_external_dependency(:foo) { yielded = true }
        yielded.should be_true
      end

      it 'raises an error and does not yield' do
        yielded = false
        EnqueuedJob.create(jid)
        ej = EnqueuedJob.new(jid)
        expect { ej.resolve_external_dependency(:bazz) { yielded = true } }.to raise_error(ArgumentError)
        yielded.should be_false
      end
    end
  end
end

