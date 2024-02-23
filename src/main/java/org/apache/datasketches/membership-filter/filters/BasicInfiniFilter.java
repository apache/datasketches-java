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

import java.util.Map.Entry;

import bitmap_implementations.Bitmap;

import java.util.TreeMap;

public class BasicInfiniFilter extends QuotientFilter implements Cloneable {

	protected long empty_fingerprint;
	int num_void_entries = 0;
	FingerprintGrowthStrategy.FalsePositiveRateExpansion fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
	int num_distinct_void_entries = 0;
	int num_expansions_estimate = -1;
	
	public void set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion val) {
		fprStyle = val;
	}
	
	BasicInfiniFilter(int power_of_two, int bits_per_entry) {
		super(power_of_two, bits_per_entry);
		max_entries_before_full = (long)(Math.pow(2, power_of_two_size) * fullness_threshold);
		set_empty_fingerprint(fingerprintLength);
	}
	
	@Override
	public Object clone() {
		BasicInfiniFilter f = null;
		f = (BasicInfiniFilter) super.clone();
		f.fprStyle = fprStyle;
		return f;
	}
	
	void set_empty_fingerprint(long fp_length) {
		empty_fingerprint = (1L << fp_length) - 2L;
	}
	
	public int get_num_void_entries() {
		return num_void_entries;
	}
	
	protected boolean compare(long index, long fingerprint) {
		long generation = parse_unary(index);
		return compare(index, fingerprint, generation);
	}
	
	protected boolean compare(long index, long fingerprint, long generation) {
		long first_fp_bit = index * bitPerEntry + 3;
		long last_fp_bit = index * bitPerEntry + 3 + fingerprintLength - (generation + 1);
		long actual_fp_length = last_fp_bit - first_fp_bit;
		long mask = (1L << actual_fp_length) - 1L;
		long existing_fingerprint = filter.getFromTo(first_fp_bit, last_fp_bit);
		long adjusted_saught_fp = fingerprint & mask;
		return existing_fingerprint == adjusted_saught_fp;
	}
	
	protected boolean compare(long index, long search_fingerprint, long generation, long slot_fingerprint) {
		long mask = (1 << (fingerprintLength - generation - 1)) - 1;
		long adjusted_saught_fp = search_fingerprint & mask;
		long adjusted_existing_fp = slot_fingerprint & mask;
		return adjusted_existing_fp == adjusted_saught_fp;
	}
		
	// this is the newer version of parsing the unary encoding. 
	// it is done using just binary operations and no loop. 
	// however, this optimization didn't yield much performance benefit 
	long parse_unary(long slot_index) {
		long f = get_fingerprint(slot_index);
		//.out.println();
		//System.out.println(get_pretty_str(slot_index));
		//print_long_in_binary(f, 32);
		long inverted_fp = ~f;
		//print_long_in_binary(inverted_fp, 32);
		long mask = (1L << fingerprintLength) - 1;
		//print_long_in_binary(mask, 32);
		long masked = mask & inverted_fp;
		//print_long_in_binary(masked, 32);
		long highest = Long.highestOneBit(masked);
		//print_long_in_binary(highest, 32);
		long leading_zeros = Long.numberOfTrailingZeros(highest);
		//System.out.println( leading_zeros );
		long age = fingerprintLength - leading_zeros - 1;
		//System.out.println( age );
		return age;
	}
	
	long parse_unary_from_fingerprint(long fingerprint) {
		//.out.println();
		//System.out.println(get_pretty_str(slot_index));
		//print_long_in_binary(f, 32);
		long inverted_fp = ~fingerprint;
		//print_long_in_binary(inverted_fp, 32);
		long mask = (1L << fingerprintLength) - 1;
		//print_long_in_binary(mask, 32);
		long masked = mask & inverted_fp;
		//print_long_in_binary(masked, 32);
		long highest = Long.highestOneBit(masked);
		//print_long_in_binary(highest, 32);
		long leading_zeros = Long.numberOfTrailingZeros(highest);
		//System.out.println( leading_zeros );
		long age = fingerprintLength - leading_zeros - 1;
		//System.out.println( age );
		return age;
	}
	
	// TODO if we rejuvenate a void entry, we should subtract from num_void_entries 
	// as if this count reaches zero, we can have shorter chains
	public boolean rejuvenate(long key) {
		long large_hash = get_hash(key);
		long fingerprint = gen_fingerprint(large_hash);
		long ideal_index = get_slot_index(large_hash);
		
		boolean does_run_exist = is_occupied(ideal_index);
		if (!does_run_exist) {
			return false;
		}
		
		long run_start_index = find_run_start(ideal_index);
		long smallest_index = find_largest_matching_fingerprint_in_run(run_start_index, fingerprint);
		if (smallest_index == -1) {
			return false;
		}
		swap_fingerprints(smallest_index, fingerprint);
		return true; 
	}

	
	long decide_which_fingerprint_to_delete(long index, long fingerprint) {
		return find_largest_matching_fingerprint_in_run(index, fingerprint);
	}
	
	// returns the index of the entry if found, -1 otherwise
	long find_largest_matching_fingerprint_in_run(long index, long fingerprint) {
		long matching_fingerprint_index = -1;
		long lowest_age = Integer.MAX_VALUE;
		do {
			long slot_fp = get_fingerprint(index);
			long age = parse_unary_from_fingerprint(slot_fp);
			//System.out.println("age " + age);
			if (compare(index, fingerprint, age, slot_fp)) {
				if (age == 0) {
					return index;
				}
				if (age < lowest_age) {
					lowest_age = age;
					matching_fingerprint_index = index;
				}
			}
			index++;
		} while (is_continuation(index));
		return matching_fingerprint_index; 
	}
	
	long gen_fingerprint(long large_hash) {
		long fingerprint_mask = (1L << fingerprintLength) - 1L;
		fingerprint_mask = fingerprint_mask << power_of_two_size;
		long fingerprint = (large_hash & fingerprint_mask) >> power_of_two_size;
		long unary_mask = ~(1L << (fingerprintLength - 1L));
		long updated_fingerprint = fingerprint & unary_mask;
		/*System.out.println(); 
		print_long_in_binary(unary_mask, fingerprintLength);
		print_long_in_binary( fingerprint, fingerprintLength);
		print_long_in_binary( updated_fingerprint, fingerprintLength);*/
		return updated_fingerprint;
	}
	
	void handle_empty_fingerprint(long bucket_index, QuotientFilter insertee) {
		//System.out.println("called");
		/*long bucket1 = bucket_index;
		long bucket_mask = 1L << power_of_two_size; 		// setting this bit to the proper offset of the slot address field
		long bucket2 = bucket1 | bucket_mask;	// adding the pivot bit to the slot address field
		insertee.insert(empty_fingerprint, bucket1, false);
		insertee.insert(empty_fingerprint, bucket2, false);*/
	}
	
	private static int prep_unary_mask(int prev_FP_size, int new_FP_size) {
		int fingerprint_diff = new_FP_size - prev_FP_size;
		
		int unary_mask = 0;
		for (int i = 0; i < fingerprint_diff + 1; i++) {
			unary_mask <<= 1;
			unary_mask |= 1;
		}
		unary_mask <<= new_FP_size - 1 - fingerprint_diff;
		return unary_mask;
	}
	
	int get_num_void_entries_by_counting() {
		int num = 0;
		for (long i = 0; i < get_physcial_num_slots(); i++) {
			long fp = get_fingerprint(i);
			if (fp == empty_fingerprint) {
				num++;
			}
		}
		return num;
	}

	
	void report_void_entry_creation(long slot) {
		num_distinct_void_entries++;
		num_void_entries++;
	}
	
	public boolean expand() {
		if (is_full()) {
			return false;
		}
		int new_fingerprint_size = FingerprintGrowthStrategy.get_new_fingerprint_size(original_fingerprint_size, num_expansions, num_expansions_estimate, fprStyle);
		//System.out.println("FP size: " + new_fingerprint_size);
		//new_fingerprint_size = Math.max(new_fingerprint_size, fingerprintLength);
		
		// we can't currently remove more than one bit at a time from a fingerprint during expansion
		// This means we'd be losing bits from the mother hash and result in false negatives 
		if (new_fingerprint_size < fingerprintLength) {
			new_fingerprint_size = fingerprintLength - 1;
		}
	
		
		QuotientFilter new_qf = new QuotientFilter(power_of_two_size + 1, new_fingerprint_size + 3);
		Iterator it = new Iterator(this);		
		long unary_mask = prep_unary_mask(fingerprintLength, new_fingerprint_size);
		
		long current_empty_fingerprint = empty_fingerprint;
		set_empty_fingerprint(new_fingerprint_size);
		//print_long_in_binary(current_empty_fingerprint, 32);
		//print_long_in_binary(empty_fingerprint, 32);
		//num_void_entries = 0;
		
		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			if (it.fingerprint != current_empty_fingerprint) {
				long pivot_bit = (1 & fingerprint);	// getting the bit of the fingerprint we'll be sacrificing 
				long bucket_mask = pivot_bit << power_of_two_size; // setting this bit to the proper offset of the slot address field
				long updated_bucket = bucket | bucket_mask;	 // adding the pivot bit to the slot address field
				long chopped_fingerprint = fingerprint >> 1; // getting rid of this pivot bit from the fingerprint 
				long updated_fingerprint = chopped_fingerprint | unary_mask;				
				new_qf.insert(updated_fingerprint, updated_bucket, false);
				
				//print_long_in_binary(updated_fingerprint, 32);
				if (updated_fingerprint == empty_fingerprint) {
					report_void_entry_creation(updated_bucket);
				}
				
				
				//if (updated_fingerprint == empty_fingerprint) {
				//	num_void_entries++;
					//is_full = true;
				//}
				/*System.out.println(bucket); 
				System.out.print("bucket1      : ");
				print_long_in_binary( bucket, power_of_two_size);
				System.out.print("fingerprint1 : ");
				print_long_in_binary((int) fingerprint, fingerprintLength);
				System.out.print("pivot        : ");
				print_long_in_binary((int) pivot_bit, 1);
				System.out.print("mask        : ");
				print_long_in_binary((int) unary_mask, new_fingerprint_size);
				System.out.print("bucket2      : ");
				print_long_in_binary((int) updated_bucket, power_of_two_size + 1);
				System.out.print("fingerprint2 : ");
				print_long_in_binary((int) updated_fingerprint, new_fingerprint_size);
				System.out.println();
				System.out.println();*/
			}
			else {
				handle_empty_fingerprint(it.bucket_index, new_qf);
			}
		}
		//System.out.println("num_void_entries  " + num_void_entries);
		empty_fingerprint = (1L << new_fingerprint_size) - 2 ;
		fingerprintLength = new_fingerprint_size;
		bitPerEntry = new_fingerprint_size + 3;
		filter = new_qf.filter;
		num_physical_entries = new_qf.num_physical_entries;
		//num_void_entries = new_qf.num_void_entries;
		power_of_two_size++;
		num_extension_slots += 2;
		max_entries_before_full = (int)(Math.pow(2, power_of_two_size) * fullness_threshold);
		last_empty_slot = new_qf.last_empty_slot;
		last_cluster_start = new_qf.last_cluster_start;
		backward_steps = new_qf.backward_steps;
		if (num_void_entries > 0) {
			//is_full = true;
		}
		return true;
	}
	
	boolean widen() {
		/*if (is_full()) {
			return false;
		}*/
		//System.out.println("FP size: " + new_fingerprint_size);
		int new_fingerprint_size = fingerprintLength + 1;
		QuotientFilter new_qf = new QuotientFilter(power_of_two_size, new_fingerprint_size + 3);
		Iterator it = new Iterator(this);		
		long unary_mask = prep_unary_mask(fingerprintLength, new_fingerprint_size - 1 );
		unary_mask <<= 1;
		set_empty_fingerprint(new_fingerprint_size);
		
		//print_long_in_binary(unary_mask, 32);
		//print_long_in_binary(current_empty_fingerprint, 32);
		//print_long_in_binary(empty_fingerprint, 32);
		//num_void_entries = 0;
		
		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			
			long updated_fingerprint = fingerprint | unary_mask;				
			new_qf.insert(updated_fingerprint, bucket, false);

			//print_long_in_binary(updated_fingerprint, 32);
			//if (updated_fingerprint == empty_fingerprint) {
			//	num_void_entries++;
			//is_full = true;
			//}
			/*System.out.println(bucket); 
				System.out.print("bucket1      : ");
				print_int_in_binary( bucket, power_of_two_size);
				System.out.print("fingerprint1 : ");
				print_int_in_binary((int) fingerprint, fingerprintLength);
				System.out.print("pivot        : ");
				print_int_in_binary((int) pivot_bit, 1);
				System.out.print("mask        : ");
				print_int_in_binary((int) unary_mask, new_fingerprint_size);
				System.out.print("bucket2      : ");
				print_int_in_binary((int) updated_bucket, power_of_two_size + 1);
				System.out.print("fingerprint2 : ");
				print_int_in_binary((int) updated_fingerprint, new_fingerprint_size);
				System.out.println();
				System.out.println();*/


		}
		//System.out.println("num_void_entries  " + num_void_entries);
		empty_fingerprint = (1L << new_fingerprint_size) - 2 ;
		fingerprintLength = new_fingerprint_size;
		bitPerEntry = new_fingerprint_size + 3;
		filter = new_qf.filter;
		num_physical_entries = new_qf.num_physical_entries;
		//num_void_entries = new_qf.num_void_entries;
		//power_of_two_size++;
		//num_extension_slots += 2;
		//max_entries_before_expansion = (int)(Math.pow(2, power_of_two_size) * expansion_threshold);
		last_empty_slot = new_qf.last_empty_slot;
		last_cluster_start = new_qf.last_cluster_start;
		backward_steps = new_qf.backward_steps;

		return true;
	}
	
	boolean is_full() {
		return num_void_entries > 0;
	}
	

	public void print_filter_summary() {
		super.print_filter_summary();
		int num_void_entries = get_num_void_entries();
		System.out.println("void entries: " + num_void_entries);
		System.out.println("distinct void entries: " + num_distinct_void_entries);
		System.out.println("is full: " + is_full);
		System.out.println("original fingerprint size: " + original_fingerprint_size);
		System.out.println("num expansions : " + num_expansions);
	}
	
	public void print_age_histogram() {	
		
		TreeMap<Long, Long> histogram = new TreeMap<Long, Long>();
		int tombstones = 0;
		int empty = 0;
		for (long i = 0; i <= fingerprintLength; i++) {
			histogram.put(i, 0L);
		}
		
		//long anomalies = 0;
		for (int i = 0; i < get_logical_num_slots_plus_extensions(); i++) {
			if (!is_slot_empty(i)) {
				long fp = get_fingerprint(i);
				long age = parse_unary(i); 	
				//System.out.println();
				//print_long_in_binary(age, 16);
				//print_long_in_binary(fp, 16);
				if (age >= 0) { 

					long count = histogram.get(age);
					histogram.put(age, count + 1);
				}
				else {
					// entry is likely a deleted_void_fingerprint
					//System.out.println();
					tombstones++;
				}
			}
			else {
				empty++;
			}
		}
		
		System.out.println("fingerprint sizes histogram");
		System.out.println("\tFP size" + "\t" + "count");
		double num_slots = get_logical_num_slots_plus_extensions();
		double total_percentage = 0;
		for ( Entry<Long, Long> e : histogram.entrySet() ) {
			long fingerprint_size = fingerprintLength - e.getKey() - 1;
			if (fingerprint_size >= 0) {
				double percentage = (e.getValue() / num_slots) * 100.0;
				total_percentage += percentage;
				System.out.println("\t" + fingerprint_size + "\t" + e.getValue() + "\t" + String.format(java.util.Locale.US,"%.2f", percentage) + "%");
			}
		}
		double tombstones_percent = (tombstones / num_slots) * 100;
		total_percentage += tombstones_percent;
		System.out.println("\ttomb\t" + tombstones + "\t" + String.format(java.util.Locale.US,"%.2f", tombstones_percent) + "%");
		double empty_percent = (empty / num_slots) * 100;
		total_percentage += empty_percent;
		System.out.println("\tempt\t" + empty + "\t" + String.format(java.util.Locale.US,"%.2f", empty_percent) + "%");
		System.out.println("\ttotal\t" + num_slots + "\t" + String.format(java.util.Locale.US,"%.2f", total_percentage) + "%");
		
		
		
		
	}
	
}


