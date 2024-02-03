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

import java.util.ArrayList;

import filters.FingerprintGrowthStrategy.FalsePositiveRateExpansion;

/*
 * The following example assumes we begin with an InfiniFilter with 2^3, or 8 cells and 4 bits per fingerprint
 * The example assumes decreasing the FPR polynomially, or in other words
 * the fingerprint size for new entries is increasing at a rate of  2(log2(X)), where X is the number of expansions
 * that has taken place. 
 * This example shows us how to adjust the capacity of the secondary InfiniFilter in response, 
 * and how many bits / entry to assign its fingerprints 
 * This is based on the intuition that it takes longer for fingerprints   
 * 
 * expansions	size	bits / entry	Sec size 	sec bits /entry
 * 0			3		4			
 * 1			4		6			
 * 2			5		7			
 * 3			6		8			
 * 4			7		8				3			4
 * 5			8		9			
 * 6			9		9			
 * 7			10		10				4			6
 * 8			11		10			
 * 9			12		10				5			7
 * 10			13		10			
 * 11			14		11				6			8
 * 12			15		11				7			8
 * 13			16		11			
 * 14			17		11				8			9
 * 15			18		12				9			9
 * 16			19		12			
 * 17			20		12				10			10
 * 18			21		12				11			10
 * 19			22		12				12			10
 * 20			23		12				13			10
 * 21			24		12			
 * 22			25		13				14			11
*/

public class ChainedInfiniFilter extends BasicInfiniFilter implements Cloneable {

	ArrayList<BasicInfiniFilter> chain;
	BasicInfiniFilter secondary_IF = null;
	//int count_until_replacing_former = 0;
	//int count_until_expanding_former = 0;
	//int former_phase = 0;
	
	public ChainedInfiniFilter(int power_of_two, int bits_per_entry) {
		super(power_of_two, bits_per_entry);
		chain = new ArrayList<BasicInfiniFilter>();
		//num_expansions_left = Integer.MAX_VALUE;
	}
	
	@Override
	public Object clone() {
		ChainedInfiniFilter f = null;
		f = (ChainedInfiniFilter) super.clone();
		f.secondary_IF = secondary_IF == null ? null : (BasicInfiniFilter) secondary_IF.clone();
		f.chain = new ArrayList<BasicInfiniFilter>();
		for (BasicInfiniFilter i : chain) {
			BasicInfiniFilter cloned = (BasicInfiniFilter) i.clone();
			f.chain.add( cloned );
		}
		return f;
	}
	
	boolean is_full() {
		return false;
	}
	
	public boolean is_chain_empty() {
		return chain.size() == 0;
	}
	
	long slot_mask = 0;
	long fingerprint_mask = 0;
	long unary_mask = 0;
	
	void prep_masks() {
		if (secondary_IF == null) {
			return;
		}

		prep_masks(power_of_two_size, secondary_IF.power_of_two_size, secondary_IF.fingerprintLength);

	}
	
	public BasicInfiniFilter get_secondary() {
		return secondary_IF;
	}
	
	void prep_masks(long active_IF_power_of_two, long secondary_IF_power_of_two, long secondary_FP_length) {

		long _slot_mask = (1L << secondary_IF_power_of_two) - 1L;

		long actual_FP_length = active_IF_power_of_two - secondary_IF_power_of_two;
		long FP_mask_num_bits = Math.min(secondary_FP_length - 1, actual_FP_length);
		
		long _fingerprint_mask = (1L << FP_mask_num_bits ) - 1L;
		
		long num_padding_bits =  secondary_FP_length - FP_mask_num_bits;
		long _unary_mask = 0;
		long unary_mask1 = 0;

		if (num_padding_bits > 0) {
			unary_mask1 = (1L << num_padding_bits - 1) - 1L;
			_unary_mask = unary_mask1 << (actual_FP_length + 1);			
		}

		/*QuotientFilter.print_long_in_binary(_slot_mask, 32);
		QuotientFilter.print_long_in_binary(_fingerprint_mask, 32);
		QuotientFilter.print_long_in_binary(unary_mask1, 32);
		QuotientFilter.print_long_in_binary(_unary_mask, 32);*/
		
		unary_mask = _unary_mask;
		slot_mask = _slot_mask;
		fingerprint_mask = _fingerprint_mask;
		
		//System.out.println();
	}
	
	void handle_empty_fingerprint(long bucket_index, QuotientFilter current) {
		long bucket1 = bucket_index;
		long fingerprint = bucket_index >> secondary_IF.power_of_two_size;
		long slot = bucket1 & slot_mask;
		
		// In case the fingerprint is too long, we must chop it. This is here just for safety though, 
		// as the slot width of the secondary IF should generally be large enough
		long adjusted_fingerprint = fingerprint & fingerprint_mask; 
		
		// In case the fingerprint is too short, we must add unary padding
		adjusted_fingerprint = adjusted_fingerprint | unary_mask;
		
		//unary_mask = unary_mask <<			
		/*print_long_in_binary( bucket1, power_of_two_size + 1);
		print_long_in_binary( slot_mask, 32);
		print_long_in_binary( slot, secondary_IF.power_of_two_size + 2);
		print_long_in_binary( fingerprint_mask, 32);
		print_long_in_binary( unary_mask, 32);
		print_long_in_binary( fingerprint, 32);
		print_long_in_binary( adjusted_fingerprint, 32);
		System.out.println();*/

		num_physical_entries--;
		//secondary_IF.num_existing_entries++;
		boolean success = secondary_IF.insert(adjusted_fingerprint, slot, false);
		

		if (exceeding_secondary_threshold()) {
			//pretty_print();
			consider_expanding_secondary(false);
			prep_masks();
		}
		
		consider_widening();
		
		if (!success) {
			print_age_histogram();
			consider_expanding_secondary(false);
			//consider_widening();
			//secondary_IF.expand();
			prep_masks();
			
			 bucket1 = bucket_index;
			 fingerprint = bucket_index >> secondary_IF.power_of_two_size;
			 slot = bucket1 & slot_mask;
			 adjusted_fingerprint = fingerprint & fingerprint_mask; 
			adjusted_fingerprint = adjusted_fingerprint | unary_mask;
			
			success = secondary_IF.insert(adjusted_fingerprint, slot, false);
			
			//pretty_print();
			print_age_histogram();
			System.out.println();
		}
		
		if (!success) {
			
			print_filter_summary();
			print_age_histogram();
			pretty_print();
			System.out.println("didn't manage to insert entry to secondary");
			System.exit(1);
		}
	}
	
	// The hash function is being computed here for each filter 
	// However, it's not such an expensive function, so it's probably not a performance issue. 
	public boolean search(long input) {
		
		if (super.search(input)) {
			return true;
		}
		
		if (secondary_IF != null && secondary_IF.search(input)) {
			return true;
		}
		
		for (QuotientFilter qf : chain) {
			if (qf.search(input)) {
				return true;
			}
		}
		return false;
	}
	
	void create_secondary(int power, int FP_size) {
		power = Math.max(power, 3);
		secondary_IF = new BasicInfiniFilter(power, FP_size + 3);
		secondary_IF.hash_type = this.hash_type;
		secondary_IF.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		secondary_IF.original_fingerprint_size = original_fingerprint_size;
	}
	
	void consider_expanding_secondary(boolean force) {
		if (secondary_IF.num_void_entries > 0 && (exceeding_secondary_threshold())) { // our former filter is full 			
			chain.add(secondary_IF); 
			//int orig_FP = secondary_IF.fingerprintLength;
						int new_power_of_two = secondary_IF.power_of_two_size;
			if (fprStyle == FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL || 
				fprStyle == FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK) {
				new_power_of_two -= 2;
			}
			
			secondary_IF = new BasicInfiniFilter(new_power_of_two, secondary_IF.fingerprintLength + 3);
			secondary_IF.hash_type = this.hash_type;
			secondary_IF.original_fingerprint_size = original_fingerprint_size;
			secondary_IF.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		}
		// we expand the secondary infinifilter
		else {  // standard procedure
			expand_secondary_IF(force);
		}
	}
	
	public boolean expand() {	
		//print_filter_summary();
		// creating secondary IF for the first time 
		
		if (secondary_IF == null && num_void_entries > 0) { // first time we create a former filter
			int power = (int) Math.ceil( Math.log(num_void_entries) / Math.log(2) );
			int FP_size = power_of_two_size - power + 1; 
			create_secondary(power, FP_size);
		}
		// the secondary infinifilter is full, so we add it to the chain
		else if (secondary_IF != null) { // our former filter is full 
			consider_expanding_secondary(false);
			//consider_widening();
			//expand_secondary_IF();
		}
		prep_masks();
		super.expand();
		//System.out.println(num_expansions + "\t" + num_distinct_void_entries + "\t" + fingerprintLength + "\t" + num_existing_entries);

		return true;
	}
	
	boolean exceeding_secondary_threshold() {
		int num_entries = secondary_IF.num_physical_entries /* + num_void_entries*/;
		long logical_slots = secondary_IF.get_logical_num_slots();
		double secondary_fullness = num_entries / (double)logical_slots;
		return secondary_fullness > fullness_threshold / 0.95;
	}
	
	void expand_secondary_IF(boolean force) {
		// sometimes we may also want to widen the fingerprint bits, not just expand when we reach capacity
		// need to consider this 
		while (exceeding_secondary_threshold() || force)  {
			secondary_IF.num_expansions++;
			//System.out.println(secondary_IF.num_expansions);
			if (secondary_IF.num_expansions == 7) {
				exceeding_secondary_threshold();
			}
			secondary_IF.expand();
			force = false;
			//logical_slots = secondary_IF.get_logical_num_slots();
			//secondary_fullness = num_entries / (double)logical_slots;
			//expanded = true;
		}
		
		//int extra_bits_needed = power_of_two_size - ( secondary_IF.power_of_two_size + secondary_IF.fingerprintLength - 1);
		//consider_widening();
	}
	
	void consider_widening() {
		int info_bits_secondary = secondary_IF.power_of_two_size + secondary_IF.fingerprintLength;
		while (info_bits_secondary <= power_of_two_size + 1) {
			secondary_IF.widen();
			prep_masks();
			info_bits_secondary = secondary_IF.power_of_two_size + secondary_IF.fingerprintLength;
		}
	}
	
	// TODO if we rejuvenate a void entry, we should subtract from num_void_entries 
	// as if this count reaches zero, we can have shorter chains
	public boolean rejuvenate(long key) {
		boolean success = super.rejuvenate(key);
		if (success) {
			return true;
		}
		if (secondary_IF == null) {
			System.out.println("Warning: it seems the key to be rejuvenrated does not exist. We must only ever call rejuvenrate on keys that exist.");
			return false;
		}
		long removed_fp = secondary_IF.delete(key);
		if (removed_fp > -1) {
			success = insert(key, false);
			if (!success) {
				System.out.println("failed at rejuvenation");
				System.exit(1);
			}
			return true;
		}
		for (int i = chain.size() - 1; i >= 0; i--) {						
			removed_fp = chain.get(i).delete(key);
			if (removed_fp > -1) {
				success = insert(key, false);
				if (!success) {
					System.out.println("failed at rejuvenation");
					System.exit(1);
				}
				return true;
			}
		}
		return false;
	}
	
	
	public long delete(long input) {
		long large_hash = get_hash(input);
		long slot_index = get_slot_index(large_hash);
		long fp_long = gen_fingerprint(large_hash);
		//System.out.println("deleting  " + input + "\t b " + slot_index + " \t" + get_fingerprint_str(fp_long, fingerprintLength));
		long removed_fp = delete(fp_long, slot_index);
		if (removed_fp > -1) {
			num_physical_entries--;
			return removed_fp;
		}
		
		slot_index = secondary_IF.get_slot_index(large_hash);
		fp_long = secondary_IF.gen_fingerprint(large_hash);
		removed_fp = secondary_IF.delete(fp_long, slot_index);
		if (removed_fp > -1) {
			secondary_IF.num_physical_entries--;
			return removed_fp;
		}
		
		for (int i = chain.size() - 1; i >= 0; i--) {			
			slot_index = chain.get(i).get_slot_index(large_hash);
			fp_long = chain.get(i).gen_fingerprint(large_hash);
			removed_fp = chain.get(i).delete(fp_long, slot_index);
			if (removed_fp > -1) {
				chain.get(i).num_physical_entries--;
				return removed_fp;
			}
		}
		
		return removed_fp; 
	}
	
	public double measure_num_bits_per_entry() {
		ArrayList<QuotientFilter> filters = new ArrayList<QuotientFilter>(chain);
		if (secondary_IF != null) {
			filters.add(secondary_IF);
		}
		return measure_num_bits_per_entry(this, filters);
	}
	
	public void print_filter_summary() {
		System.out.println("----------------------------------------------------");
		super.print_filter_summary();
		System.out.println();
		if (secondary_IF != null) {
			secondary_IF.print_filter_summary();
		}
		System.out.println();
		for (BasicInfiniFilter f : chain) {
			f.print_filter_summary();
			System.out.println();
		}
	}
	
	public void pretty_print() {	
		System.out.println();
		System.out.println("Active IF");
		System.out.print(get_pretty_str(true));
		System.out.println();
		if (secondary_IF != null) {
			System.out.println("Secondary IF");
			System.out.print(secondary_IF.get_pretty_str(true));
			System.out.println();
		}
		for (int i = 0; i < chain.size(); i++) {
			System.out.println("Chain #" + i);
			System.out.print(chain.get(i).get_pretty_str(true));
			System.out.println();	
		}
		
	}
	
	public long get_num_occupied_slots(boolean include_all_internal_filters) {
		long num_entries = super.get_num_occupied_slots(false);
		if (!include_all_internal_filters) {
			return num_entries;
		}
		for (QuotientFilter q : chain) {
			num_entries += q.get_num_occupied_slots(false);
		}
		if (secondary_IF != null) {
			long former_num_entries = secondary_IF.get_num_occupied_slots(false);
			num_entries += former_num_entries;
		}
		return num_entries; 
	}
	
	
	public void print_age_histogram() {	
		
		super.print_age_histogram();
		
		//System.out.println("---------------------");
		
		if (secondary_IF != null) {
			secondary_IF.print_age_histogram();
		}
		
		for (int i = chain.size() - 1; i >= 0; i--) {
			chain.get(i).print_age_histogram();
		}
	}

	
}

