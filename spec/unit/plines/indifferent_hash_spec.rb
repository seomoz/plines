require 'spec_helper'
require 'plines/indifferent_hash'
require 'securerandom'

module Plines
  describe IndifferentHash do
    it 'allows keys to be accessed by string or symbol' do
      hash = IndifferentHash.from('a' => 1, b: 2)
      expect(hash['a']).to eq(1)
      expect(hash[:a]).to eq(1)
      expect(hash['b']).to eq(2)
      expect(hash[:b]).to eq(2)
    end

    it 'allows keys to be fetched by string or symbol' do
      hash = IndifferentHash.from('a' => 1, b: 2)
      expect(hash.fetch 'a').to eq(1)
      expect(hash.fetch :a).to eq(1)
      expect(hash.fetch 'b').to eq(2)
      expect(hash.fetch :b).to eq(2)
    end

    it 'raises an error if a the hash has conflicting string/symbol entries' do
      expect {
        IndifferentHash.from('a' => 1, a: 2)
      }.to raise_error(IndifferentHash::ConflictingEntriesError)
    end

    it 'fails with an expected error if a non-existant symbol key is fetched' do
      msg = ({}.fetch(:a) rescue $!).message
      hash = IndifferentHash.from({})

      expect { hash.fetch :a }.to raise_error(KeyError, msg)
    end

    it 'fails with an expected error if a non-existant string key is fetched' do
      error = {}.fetch('a') rescue $!
      hash = IndifferentHash.from({})

      expect { hash.fetch 'a' }.to raise_error(error.class, error.message)
    end

    it 'supports a normal fetch block' do
      hash = IndifferentHash.from({})
      expect(hash.fetch('a') { 5 }).to eq(5)
      expect(hash.fetch(:a) { 5 }).to eq(5)
    end

    it 'supports a normal fetch default arg' do
      hash = IndifferentHash.from({})
      expect(hash.fetch('a', 5)).to eq(5)
      expect(hash.fetch(:a,  5)).to eq(5)
    end

    it 'does not use the fetch block if the key is available in the other form' do
      hash = IndifferentHash.from('a' => 3)
      expect(hash.fetch(:a) { raise "should not get here" }).to eq(3)
    end

    it 'allows keys to be deleted by string or symbol' do
      hash = IndifferentHash.from('a' => 1, :b => 2, 'c' => 3, :d => 4)
      expect(hash.delete 'a').to eq(1)
      expect(hash.delete 'b').to eq(2)
      expect(hash.delete :c).to eq(3)
      expect(hash.delete :d).to eq(4)
    end

    it 'supports a normal delete block' do
      hash = IndifferentHash.from({})
      expect(hash.delete('a') { :default_block }).to eq(:default_block)
      expect(hash.delete(:a) { :default_block }).to eq(:default_block)
    end

    it 'does not add to the symbol table' do
      key = SecureRandom.hex

      expect {
        hash = IndifferentHash.from(key => 23)
        expect(hash[key]).to eq(23)
        expect(hash[SecureRandom.hex]).to eq(nil)
      }.not_to change { Symbol.all_symbols.count }
    end

    let(:nested) do
      IndifferentHash.from('a' => { 'b' => 2 }, 'c' => [{ 'd' => 3 }])
    end

    it 'propogates indifference through nested hash structures' do
      expect(nested['a']['b']).to eq(2)
      expect(nested[:a][:b]).to eq(2)
      expect(nested['c'].first['d']).to eq(3)
      expect(nested[:c].first[:d]).to eq(3)
    end

    it 'propogates the fetch indifference through nested hash structures' do
      expect(nested.fetch('a').fetch('b')).to eq(2)
      expect(nested.fetch(:a).fetch(:b)).to eq(2)
      expect(nested.fetch('c').first.fetch('d')).to eq(3)
      expect(nested.fetch(:c).first.fetch(:d)).to eq(3)
    end

    it 'raises an error if initialized with an array' do
      expect {
        IndifferentHash.from([])
      }.to raise_error(/Expected a hash, got \[\]/)
    end

    it 'works when initalized with an indifferent hash' do
      hash = IndifferentHash.from('a' => 3)
      hash2 = IndifferentHash.from(hash)

      expect(hash[:a]).to eq(3)
    end

    it 'does not allow one to be incorrectly constructed with .new' do
      expect {
        IndifferentHash.new('a' => 3)
      }.to raise_error(NoMethodError)
    end

    it 'returns an indifferent hash when merged' do
      h1 = IndifferentHash.from('a' => 3)
      h2 = h1.merge('b' => 4)

      expect(h2['a']).to eq(3)
      expect(h2[:a]).to eq(3)
      expect(h2['b']).to eq(4)
      expect(h2[:b]).to eq(4)

      expect(h2.fetch :a).to eq(3)
      expect(h2.fetch 'a').to eq(3)
      expect(h2.fetch :b).to eq(4)
      expect(h2.fetch 'b').to eq(4)
    end

    it 'overrides entries properly when merged' do
      h1 = IndifferentHash.from('a' => 3)
      h2 = h1.merge(a: 4)

      expect(h2['a']).to eq(4)
      expect(h2[:a]).to eq(4)

      expect(h2.fetch :a).to eq(4)
      expect(h2.fetch 'a').to eq(4)
    end
  end
end

