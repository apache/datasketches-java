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

public class FingerprintSacrifice extends QuotientFilter {

	public FingerprintSacrifice(int power_of_two, int bits_per_entry) {
		super(power_of_two, bits_per_entry);
		// TODO Auto-generated constructor stub
		max_entries_before_full = (int)(Math.pow(2, power_of_two_size) * fullness_threshold);
	}
	
	public boolean expand() {
		
		if (fingerprintLength == 0) {
			is_full = true;
			return false;
		}
		
		QuotientFilter new_qf = new QuotientFilter(power_of_two_size + 1, bitPerEntry - 1);
		Iterator it = new Iterator(this);
		//long start = System.nanoTime();
		
		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			long pivot_bit = (1 & fingerprint);
			long bucket_mask = pivot_bit << power_of_two_size;
			long updated_bucket = bucket | bucket_mask;
			long updated_fingerprint = fingerprint >> 1;
			
			/*System.out.println(bucket); 
			System.out.print("bucket1      : ");
			print_int_in_binary( bucket, power_of_two_size);
			System.out.print("fingerprint1 : ");
			print_int_in_binary((int) fingerprint, fingerprintLength);
			System.out.print("pivot        : ");
			print_int_in_binary((int) pivot_bit, 1);
			System.out.print("bucket2      : ");
			print_int_in_binary((int) updated_bucket, power_of_two_size + 1);
			System.out.print("fingerprint2 : ");
			print_int_in_binary((int) updated_fingerprint, fingerprintLength - 1);
			System.out.println();
			System.out.println();*/
			
			new_qf.insert(updated_fingerprint, (int)updated_bucket, false);
		}
		
		//long end = System.nanoTime();
		//double time = (end - start) / 1000;
		//System.out.println("time BS  " + time + "   " + new_qf.get_num_entries(false));
		last_empty_slot = new_qf.last_empty_slot;
		last_cluster_start = new_qf.last_cluster_start;
		backward_steps = new_qf.backward_steps;
		
		filter = new_qf.filter;
		power_of_two_size++;
		num_extension_slots += 2;
		bitPerEntry--;
		fingerprintLength--;
		max_entries_before_full = (int)(Math.pow(2, power_of_two_size) * fullness_threshold);
		return true;
	}
	
}
