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

import java.util.Random;

import bitmap_implementations.Bitmap;
import bitmap_implementations.QuickBitVectorWrapper;

public class CuckooFilter extends Filter {

	Bitmap filter;
	long num_bits; 
	public long max_num_entries;
	int current_num_entries;
	long bits_per_entry;
	int num_hash_functions;
	long[] primes = {1009, 10007, 100003, 1000003};
	long chosen_prime;
	int bucket_size = 4;
	int power_of_two_buckets;
	long num_buckets = 0;
	Random gen = new Random();
	int max_swaps = 1000;

	long leftover_fingerprint = 0;

	public CuckooFilter(int power_of_two, int new_bits_per_entry) {
		max_num_entries = (long) Math.pow(2, power_of_two);
		filter = new QuickBitVectorWrapper(new_bits_per_entry,  (int)max_num_entries);
		num_bits = new_bits_per_entry * max_num_entries;
		bits_per_entry = new_bits_per_entry;
		hash_type = HashType.xxh;
		current_num_entries = 0;

		power_of_two_buckets = (int)(power_of_two - Math.log(bucket_size) / Math.log(2));
		num_buckets = max_num_entries / bucket_size;
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
		long fingerprint = gen_fingerprint(large_hash);
		while (fingerprint == 0) {
			large_hash = HashFunctions.xxhash(large_hash);
			fingerprint = gen_fingerprint(large_hash);
		}
		int bucket1 = get_bucket_index(large_hash);
		long removed = remove(bucket1, fingerprint);	
		if (removed > -1) {
			return removed;
		}	
		long second_large_hash = HashFunctions.xxhash(fingerprint);
		int bucket2 = bucket1 ^ get_bucket_index(second_large_hash);
		removed = remove(bucket2, fingerprint);
		return removed;
	}
	
	// return the index of an empty cell in the bucket, or -1 if there are no empty cells
	long remove(long bucket, long fingerprint) {
		long starting_bit = bucket * bucket_size * bits_per_entry;
		for (int i = 0; i < bucket_size; i++) {
			long from = starting_bit + i * bits_per_entry;
			long to = starting_bit + (i + 1) * bits_per_entry;
			long res = filter.getFromTo(from, to);
			if (res == fingerprint) {
				filter.setFromTo(from, to, 0);
				return res;
			}
		}
		return -1;
	}

	long gen_fingerprint(long large_hash) {
		long fingerprint_mask = (1 << bits_per_entry) - 1;
		fingerprint_mask = fingerprint_mask << power_of_two_buckets;
		long fingerprint = (large_hash & fingerprint_mask) >> power_of_two_buckets;
		//System.out.format("\n**gen_fingerprint(): [total_hash:fingerprint_hash:int_fingerprint] --> [%016x:%016x:%016x]\n", large_hash, ((int)(large_hash>>32)), fingerprint);
		return fingerprint;
	}
	
	// return the index of an empty cell in the bucket, or -1 if there are no empty cells
	int try_store_fingerprint(long bucket, long fingerprint) {
		long starting_bit = bucket * bucket_size * bits_per_entry;
		for (int i = 0; i < bucket_size; i++) {
			long from = starting_bit + i * bits_per_entry;
			long to = starting_bit + (i + 1) * bits_per_entry;
			long res = filter.getFromTo(from, to);
			if (res == 0) {
				filter.setFromTo(from, to, fingerprint);
				return i;
			}
		}
		return -1;
	}
	
	// replace fingerprint at a given bucket slot 
	long replace(long bucket, int slot, long fingerprint) {
		long starting_bit = bucket * bucket_size * bits_per_entry + slot * bits_per_entry;
		long present_fp = filter.getFromTo(starting_bit, starting_bit + bits_per_entry);
		filter.setFromTo(starting_bit, starting_bit + bits_per_entry, fingerprint);
		return present_fp;
	}
	
	// return the index of an empty cell in the bucket, or -1 if there are no empty cells
	boolean check(long bucket, long fingerprint) {
		long starting_bit = bucket * bucket_size * bits_per_entry;
		for (int i = 0; i < bucket_size; i++) {
			long from = starting_bit + i * bits_per_entry;
			long to = starting_bit + (i + 1) * bits_per_entry;
			long res = filter.getFromTo(from, to);
			if (res == fingerprint) {
				return true;
			}
		}
		return false;
	}
	

	
	int get_bucket_index(long large_hash) {
		int bucket_index_mask = (1 << power_of_two_buckets) - 1;
		int bucket_index = ((int)(large_hash)) & bucket_index_mask;
		//System.out.format("\n**get_slot_index(): [total_hash:index_hash:int_index] --> [%016x:%016x:%016x]\n", large_hash, (int)large_hash, slot_index);
		return bucket_index;
	}
	
	@Override
	protected boolean _insert(long large_hash, boolean insert_only_if_no_match) {		
		long fingerprint = gen_fingerprint(large_hash);
		while (fingerprint == 0) {
			//System.out.println("FP zero:  " + large_hash);
			large_hash = HashFunctions.xxhash(large_hash);
			fingerprint = gen_fingerprint(large_hash);
		}
		int bucket1 = get_bucket_index(large_hash);
		
		/*print_long_in_binary(large_hash, 64);
		print_long_in_binary(bucket1, power_of_two_buckets);
		print_long_in_binary(fingerprint, (int) bits_per_entry);*/
		
		long outcome = try_store_fingerprint(bucket1, fingerprint);
		
		if (outcome != -1) {
			current_num_entries++;
			return true;
		}
		
		return replacement_algorithm(bucket1, fingerprint, 0);

	}
	
	// return 0 if successful, and return fingerprint of element there was no space for otherwise
	boolean replacement_algorithm(int original_bucket, long fingerprint, int swaps) {
		long second_large_hash = HashFunctions.xxhash(fingerprint);
		int alternative_bucket = original_bucket ^ get_bucket_index(second_large_hash);
		
		/*System.out.println("swap");
		print_long_in_binary(original_bucket, power_of_two_buckets);
		print_long_in_binary(alternative_bucket, power_of_two_buckets);
		print_long_in_binary(get_bucket_index(second_large_hash), power_of_two_buckets);*/
		
		int outcome = try_store_fingerprint(alternative_bucket, fingerprint);
		//System.out.println("swap " + swaps);
		if (outcome != -1) {
			current_num_entries++;
			return true;
		}
		if (swaps == max_swaps) {
			leftover_fingerprint = fingerprint;
			return false;
		}
		int random_slot = gen.nextInt(bucket_size);
		long existing_fingerprint = replace(alternative_bucket, random_slot, fingerprint);
		return replacement_algorithm(alternative_bucket, existing_fingerprint, swaps + 1);
	}

	@Override
	protected boolean _search(long large_hash) {
		long fingerprint = gen_fingerprint(large_hash);
		while (fingerprint == 0) {
			large_hash = HashFunctions.xxhash(large_hash);
			fingerprint = gen_fingerprint(large_hash);
		}
		int bucket1 = get_bucket_index(large_hash);
		
		boolean exist = check(bucket1, fingerprint);	
		if (exist) {
			return true;
		}	
		long second_large_hash = HashFunctions.xxhash(fingerprint);
		int bucket2 = bucket1 ^ get_bucket_index(second_large_hash);
		exist = check(bucket2, fingerprint);
		return exist;
	}

	@Override
	public long get_num_occupied_slots(boolean include_all_internal_filters) {
		long num_entries = 0;
		for (long i = 0; i < num_buckets; i++) {
			long bucket_start = i * bucket_size * bits_per_entry;
			for (long j = 0; j < bucket_size; j++) {
				long slot_start = bucket_start + j * bits_per_entry;
				long res = filter.getFromTo(slot_start, slot_start + bits_per_entry);
				if (res != 0) {
					num_entries++;
				}
			}
		}		
		return num_entries;
	}
	
	public void pretty_print() {
		for (int i = 0; i < num_buckets; i++) {
			long bucket_start = i * bucket_size * bits_per_entry;
			for (int j = 0; j < bucket_size; j++) {
				long slot_start = bucket_start + j * bits_per_entry;
				long res = filter.getFromTo(slot_start, slot_start + bits_per_entry);
				String s = get_fingerprint_str(res, (int)bits_per_entry);
				System.out.print(s + " ");
			}
			System.out.println();
		}
	}
	
	public void print() {
		long num_bits = num_buckets * bucket_size * bits_per_entry;
		for (int i = 0; i < num_bits; i++) {
			boolean bit = filter.get(i);
			String val = bit ? "1" : "0";
			System.out.print(val);
		}
		System.out.println();
	}
	
	public double measure_num_bits_per_entry() {
		return (max_num_entries * bits_per_entry) / (double)current_num_entries;
	}
}
