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

    describe "#all_external_dependencies" do
      it "returns pending, resolved and timed out external dependencies" do
        job = EnqueuedJob.create("abc", :foo, :bar, :bazz)
        job.resolve_external_dependency(:foo)
        job.timeout_external_dependency(:bazz)

        job.pending_external_dependencies.should_not be_empty
        job.resolved_external_dependencies.should_not be_empty
        job.timed_out_external_dependencies.should_not be_empty

        job.all_external_dependencies.should =~ [:foo, :bar, :bazz]
      end
    end

    describe "#unresolved_external_dependencies" do
      it "returns pending and timed out external dependencies but not resolved ones" do
        job = EnqueuedJob.create("abc", :foo, :bar, :bazz)
        job.resolve_external_dependency(:foo)
        job.timeout_external_dependency(:bazz)

        job.pending_external_dependencies.should_not be_empty
        job.resolved_external_dependencies.should_not be_empty
        job.timed_out_external_dependencies.should_not be_empty

        job.unresolved_external_dependencies.should =~ [:bar, :bazz]
      end
    end

    describe "#declared_redis_object_keys" do
      it 'returns the keys for each owned object' do
        job = EnqueuedJob.create("abc", :foo, :bar, :bazz)
        job.resolve_external_dependency(:foo)
        job.timeout_external_dependency(:bar)

        keys = job.declared_redis_object_keys
        keys.should have(3).entries
        keys.grep(/pending/).should have(1).entry
        keys.grep(/resolved/).should have(1).entry
        keys.grep(/timed_out/).should have(1).entry

        job.redis.keys.should include(*keys)
      end
    end

    shared_examples_for "updating an enqueued job external dependency" do |meth, final_set|
      describe "##{meth}" do
        let(:jid) { "abc" }

        it "moves the dependency to the #{final_set} set" do
          EnqueuedJob.create(jid, :foo, :bar)
          ej = EnqueuedJob.new(jid)
          ej.send(meth, :bar)
          ej.pending_external_dependencies.should eq([:foo])
          ej.send(final_set).should eq([:bar])
        end

        it 'yields when all external dependencies are resolved' do
          EnqueuedJob.create(jid, :foo, :bar)
          ej = EnqueuedJob.new(jid)

          expect { |b| ej.send(meth, :bar, &b) }.not_to yield_control
          expect { |b| ej.send(meth, :foo, &b) }.to yield_control
        end

        it 'raises an error and does not yield if the given dependency does not exist' do
          yielded = false
          EnqueuedJob.create(jid)
          ej = EnqueuedJob.new(jid)
          expect { ej.send(meth, :bazz) { yielded = true } }.to raise_error(ArgumentError)
          yielded.should be_false
        end
      end
    end

    it_behaves_like "updating an enqueued job external dependency",
      :timeout_external_dependency, :timed_out_external_dependencies

    it_behaves_like "updating an enqueued job external dependency",
      :resolve_external_dependency, :resolved_external_dependencies

    context 'when resolving a previously timed out dependency' do
      let(:ej) { EnqueuedJob.create("abc", :foo, :bar) }

      before { ej.timeout_external_dependency(:foo) }

      it 'moves it to the resolved_external_dependencies set' do
        ej.resolve_external_dependency(:foo)
        ej.resolved_external_dependencies.should include(:foo)
        ej.timed_out_external_dependencies.should_not include(:foo)
      end

      it 'does not yield since it does not affect the number of pending dependencies' do
        expect { |b| ej.resolve_external_dependency(:foo, &b) }.not_to yield_control
        ej.timeout_external_dependency(:bar) { }
        expect { |b| ej.resolve_external_dependency(:bar, &b) }.not_to yield_control
      end
    end

    it 'cannot timeout a resolved dependency' do
      ej = EnqueuedJob.create("abc", :foo)
      ej.resolve_external_dependency(:foo) { }
      expect { |b| ej.timeout_external_dependency(:foo, &b) }.not_to yield_control
      ej.resolved_external_dependencies.should include(:foo)
      ej.timed_out_external_dependencies.should_not include(:foo)
    end
  end
end

