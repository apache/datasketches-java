/*
 * Copyright 2024 Niv Dayan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package filters;

import bitmap_implementations.Bitmap;
import bitmap_implementations.QuickBitVectorWrapper;

public class BloomFilter extends Filter {

	Bitmap filter;
	long num_bits; 
	long max_num_entries;
	long current_num_entries;
	long bits_per_entry;
	int num_hash_functions;

	public BloomFilter(int new_num_entries, int new_bits_per_entry) {
		max_num_entries = new_num_entries;
		filter = new QuickBitVectorWrapper(new_bits_per_entry,  (int)max_num_entries);
		num_bits = new_bits_per_entry * max_num_entries;
		bits_per_entry = new_bits_per_entry;
		num_hash_functions = (int) Math.round( bits_per_entry * Math.log(2) );
		hash_type = HashType.xxh;		
		current_num_entries = 0;
	}
	
	@Override
	boolean rejuvenate(long key) {
		return false;
	}

	@Override
	public boolean expand() {
		return false;
	}

	@Override
	protected long _delete(long large_hash) {
		return -1;
	}
	
	long get_target_bit(long large_hash, int hash_num) {
		long this_hash = HashFunctions.xxhash(large_hash, hash_num);
		return Math.abs(this_hash % num_bits);
	}
	
	@Override
	protected boolean _insert(long large_hash, boolean insert_only_if_no_match) {
		
		long target_bit = Math.abs(large_hash % num_bits);
		filter.set(target_bit, true);
		
		for (int i = 1; i < num_hash_functions; i++) {
			target_bit = get_target_bit(large_hash, i);
			//System.out.println(target_bit);
			filter.set(target_bit, true);
		}
		current_num_entries++;
		return true;
	}

	@Override
	protected boolean _search(long large_hash) {
		
		long target_bit = Math.abs(large_hash % num_bits);
		if (! filter.get(target_bit) ) {
			return false;
		}
		
		for (int i = 1; i < num_hash_functions; i++) {
			target_bit = get_target_bit(large_hash, i);
			if (! filter.get(target_bit) ) {
				return false;
			}
		}
		return true;
	}

	@Override
	public long get_num_occupied_slots(boolean include_all_internal_filters) {
		return current_num_entries;
	}
	
	public double get_utilization() {
		return current_num_entries / max_num_entries;
	}
	
	public double measure_num_bits_per_entry() {
		return (max_num_entries * bits_per_entry) / (double)current_num_entries;
	}
}
