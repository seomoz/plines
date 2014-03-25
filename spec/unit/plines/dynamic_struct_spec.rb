require 'plines/dynamic_struct'

module Plines
  RSpec.describe DynamicStruct do
    it 'defines methods for each hash entry' do
      ds = DynamicStruct.new("a" => 5, "b" => 3)
      expect(ds.a).to eq(5)
      expect(ds.b).to eq(3)
    end

    it 'provides predicates' do
      ds = DynamicStruct.new("a" => 3, "b" => false)
      expect(ds.a?).to be true
      expect(ds.b?).to be false
    end

    it 'raises a NoMethodError for messages that are not in the hash' do
      ds = DynamicStruct.new("a" => 3)
      expect { ds.foo }.to raise_error(NoMethodError)
    end

    it 'recursively defines a DynamicStruct for nested hashes' do
      ds = DynamicStruct.new("a" => { "b" => { "c" => 3 } })
      expect(ds.a.b.c).to eq(3)
    end

    it 'handles arrays properly' do
      ds = DynamicStruct.new \
        a: [1, 2, 3],
        b: [{ c: 5 }, { c: 4 }]

      expect(ds.a).to eq([1, 2, 3])
      expect(ds.b.map(&:c)).to eq([5, 4])
    end

    it 'exposes the attribute names as a list of symbols' do
      ds = DynamicStruct.new("a" => 3, "c" => 2)
      expect(ds.attribute_names).to eq([:a, :c])
    end

    it 'provides a means to be converted to a hash' do
      ds = DynamicStruct.new("a" => 2, "b" => 1)
      expect(ds.to_hash).to eq("a" => 2, "b" => 1)
    end
  end
end

