require 'plines/pipeline'
require 'plines/job_batch'
require 'plines/job_batch_list'
require 'plines/enqueued_job'
require 'plines/configuration'

module Plines
  RSpec.describe JobBatchList, :redis do
    let(:foo) { JobBatchList.new(pipeline_module, "foo") }
    let(:bar) { JobBatchList.new(pipeline_module, "bar") }

    it 'remembers the pipeline it is from' do
      jbl = JobBatchList.new(pipeline_module, "foo")
      expect(jbl.pipeline).to be(pipeline_module)
    end

    it 'is uniquely identified by the id and pipeline' do
      j1 = JobBatchList.new(pipeline_module, "a")
      j2 = JobBatchList.new(pipeline_module, "b")
      j3 = JobBatchList.new(pipeline_module, "a")

      expect(j1).to eq(j3)
      expect(j1).to eql(j3)
      expect(j1).not_to eq(j2)
      expect(j1).not_to eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      expect(set.map(&:object_id)).to match_array [j1.object_id, j2.object_id]
    end

    it 'returns the job batches from #to_a' do
      j1 = JobBatchList.new(pipeline_module, "a")
      jb1 = j1.create_new_batch(foo: 1)
      jb2 = j1.create_new_batch(foo: 2)
      jb3 = j1.create_new_batch(foo: 3)

      expect(j1.to_a).to eq([jb1, jb2, jb3])
    end

    describe "#create_new_batch" do
      it 'creates each new batch with a unique ascending id' do
        expect(foo.create_new_batch({}).id).to eq("foo:1")
        expect(bar.create_new_batch({}).id).to eq("bar:1")
        expect(foo.create_new_batch({}).id).to eq("foo:2")
        expect(bar.create_new_batch({}).id).to eq("bar:2")
      end

      it 'passes the given batch data along to the job batch object' do
        expect(foo.create_new_batch('a' => 3).data).to eq('a' => 3)
      end
    end

    describe "#most_recent_batch" do
      it 'returns nil if there are no batches for the given id' do
        foo.create_new_batch({})
        expect(bar.most_recent_batch).to be_nil
      end

      it 'returns the most recently created batch for the given id' do
        _  = foo.create_new_batch({})
        b2 = foo.create_new_batch({})
        bar.create_new_batch({})

        expect(foo.most_recent_batch).to eq(b2)
      end
    end

    it 'can enumerate all existing job batches' do
      foo.create_new_batch({})
      foo.create_new_batch({})

      expect(foo.map(&:id)).to eq(%w[ foo:1 foo:2 ])
    end

    it 'can enumerate all existing job batch ids without the cost of loading them' do
      foo.create_new_batch({})
      foo.create_new_batch({})
      expect(JobBatch).not_to receive(:find)

      expect(foo.each_id.to_a).to eq(%w[ foo:1 foo:2 ])
    end

    it 'does not allow CannotFindExistingJobBatchError errors to propagate' do
      foo.create_new_batch({})
      foo.last_batch_num.increment
      foo.create_new_batch({})
      foo.last_batch_num.increment

      expect(foo.map(&:id)).to eq(%w[ foo:1 foo:3 ])
    end

    it 'can return a lazy enumerator' do
      foo.create_new_batch({})
      foo.create_new_batch({})

      list = foo.each
      expect(list.map(&:id)).to eq(%w[ foo:1 foo:2 ])
    end

    it 'can return a list of batches that timed out a particular dependency' do
      b1, _, b3 = 3.times.map do
        foo.create_new_batch({}) do |batch|
          batch.add_job("a", "foo")
        end
      end

      b1.timeout_external_dependency("foo", "a")
      b3.timeout_external_dependency("foo", "a")

      expect(foo.all_with_external_dependency_timeout('foo')).to contain_exactly(b1, b3)
      expect(foo.all_with_external_dependency_timeout('bar')).to eq([])
    end

    it 'can return a list of batches that timed out any dependency' do
      b1 = foo.create_new_batch({}) { |b| b.add_job("a", "foo") }
      _  = foo.create_new_batch({}) { |b| b.add_job("b", "bar") }
      b3 = foo.create_new_batch({}) { |b| b.add_job("c", "baz") }

      b1.timeout_external_dependency("foo", "a")
      b3.timeout_external_dependency("baz", "c")

      expect(foo.all_with_external_dependency_timeouts).to contain_exactly(b1, b3)
    end

    it 'is directly enumerable' do
      b1 = foo.create_new_batch({})
      b2 = foo.create_new_batch({})

      expect { |b|
        foo.select(&b)
      }.to yield_successive_args(b1, b2)
    end
  end
end

