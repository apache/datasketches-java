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
import java.util.TreeSet;

public class aleph_tests {
	
	static public void insert_all_and_then_delete_all(DuplicatingChainedInfiniFilter qf, int num_entries_power) {

		int max_key = (int)Math.pow(2, num_entries_power);

		for (int i = 0; i < max_key; i++) {		
			boolean success = qf.insert(i, false);
			Assert(success);
		}

		//qf.pretty_print();
		//System.out.println("before expansion " + qf.num_expansions + "\t" + qf.num_existing_entries + "\t" + qf.num_void_entries + "\t" + qf.num_distinct_void_entries);
		
		//qf.pretty_print();
		//qf.print_filter_summary();
		//qf.print_age_histogram();
		
		for (int i = 0; i < max_key; i++) {		
			//System.out.println("removing key " + i);
			boolean success = qf.search(i);	
			Assert(success);
			long removed_fp = qf.delete(i);
			Assert(removed_fp > -1);
			success = qf.search(i);
			//Assert(!success);
		}
		
		if (qf.lazy_void_deletes) {
			qf.expand(); // we need to expand to remove all void entries 
		}
		
		/*qf.pretty_print();
		qf.print_filter_summary();
		qf.print_age_histogram();
		System.out.println("num_existing_entries "  + qf.num_existing_entries);
		System.out.println("secondary_IF.num_existing_entries "  + qf.secondary_IF.num_existing_entries);*/
		
		for (int i = 0; i < max_key; i++) {		
			boolean success = qf.search(i);	
			Assert(!success);
		}

		// a key inserted before any expansions 
		Assert(qf.num_physical_entries == 0);
		Assert(qf.num_void_entries == 0);
		Assert(qf.num_distinct_void_entries == 0);
		if (qf.secondary_IF != null) {
			Assert(qf.secondary_IF.num_physical_entries == 0);
			Assert(qf.secondary_IF.num_void_entries == 0);
			Assert(qf.secondary_IF.num_distinct_void_entries == 0);
		}
		System.out.println("success");
	}
	
	static public void test3() {
		int bits_per_entry = 10;
		int num_entries_power = 3;	
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, false, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		insert_all_and_then_delete_all(qf, qf.power_of_two_size + qf.fingerprintLength + 1);
	}
	
	static public void test4() {
		int bits_per_entry = 10;
		int num_entries_power = 3;	
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, false, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		insert_all_and_then_delete_all(qf, qf.power_of_two_size + qf.fingerprintLength + 1);
	}
	
	static public void test5() {
		int bits_per_entry = 10;
		int num_entries_power = 3;	
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		insert_all_and_then_delete_all(qf, qf.power_of_two_size + qf.fingerprintLength + 1);
	}
	
	static public void test6() {
		int bits_per_entry = 10;
		int num_entries_power = 3;	
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		insert_all_and_then_delete_all(qf, qf.power_of_two_size + qf.fingerprintLength + 1);
	}

	
	public static void run_tests() {
		//test1();
		
		//test3();
		//test4();	
		test5();
		test6();
		
		test7();
		test8();	
		test9();
		test10();
		test11();
			
		test12();
		test13();
		test14();
		test15();
		
		//test16();
		
		//test17();
		
		//test7();
	}
	
	static public void test7() {
		int bits_per_entry = 10;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, false, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	
	static public void test8() {
		int bits_per_entry = 10;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, false, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	
	static public void test9() {
		int bits_per_entry = 10;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	
	static public void test10() {
		int bits_per_entry = 10;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	
	
	static public void many_insertions(ChainedInfiniFilter qf) {

		int max_key = (int)Math.pow(2, qf.power_of_two_size + 10);
		for (int i = 0; i < max_key; i++) {		
			boolean success = qf.insert(i, false);
			Assert(success);
		}

		//qf.pretty_print();
		
		for (int i = 0; i < max_key; i++) {		
			boolean success = qf.search(i);	
			Assert(success);
		}
		
		double false_positives = 0;
		for (long i = Long.MAX_VALUE; i >= Long.MAX_VALUE - 10000; i--) {		
			boolean success = qf.search(i);	
			if (success) {
				false_positives++;
			}
		}
		//System.out.println("FPR: " + false_positives / 10000.0);
		
		//qf.print_filter_summary();
		

		//System.out.println("new test finished");
	}
	
	static public void test11() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		ChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.expand_autonomously = true;
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		aleph_tests.many_insertions(qf);
		System.out.println("success");
	}
	
	static public void test12_() {
		int bits_per_entry = 13;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fp_size = DuplicatingChainedInfiniFilter.derive_init_fingerprint_size(bits_per_entry - 3, 10);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, fp_size + 3, true, 10);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK;
		aleph_tests.many_insertions(qf);
		System.out.println("success");
	}
	
	static public void test12() {
		int bits_per_entry = 13;
		int num_entries_power = 3;
		//int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fp_size = DuplicatingChainedInfiniFilter.derive_init_fingerprint_size(bits_per_entry - 3, 10);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, fp_size + 3, true, 10);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK;
		insert_all_and_then_delete_all(qf, 21);
	}
	
	static public void test13() {
		int bits_per_entry = 13;
		int num_entries_power = 3;
		//int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fp_size = DuplicatingChainedInfiniFilter.derive_init_fingerprint_size(bits_per_entry - 3, 10);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, fp_size + 3, true, 10);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	
	static public void test14() {
		int bits_per_entry = 13;
		int num_entries_power = 3;
		//int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fp_size = DuplicatingChainedInfiniFilter.derive_init_fingerprint_size(bits_per_entry - 3, 10);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, fp_size + 3, false, 10);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK;
		insert_all_and_then_delete_all(qf, 21);
	}
	
	static public void test15() {
		int bits_per_entry = 13;
		int num_entries_power = 3;
		//int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fp_size = DuplicatingChainedInfiniFilter.derive_init_fingerprint_size(bits_per_entry - 3, 10);
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, fp_size + 3, false, 10);
		//ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL_SHRINK;
		Tests.test_insertions_and_deletes(qf);
		System.out.println("success");
	}
	

	
	static public void test16() {
		int bits_per_entry = 10;
		int num_entries_power = 3;	
		DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
		int max_key = (int)Math.pow(2, num_entries_power + qf.fingerprintLength + 1);
		for (int i = 0; i < max_key; i++) {		
			boolean success = qf.insert(i, false);
			Assert(success);
		}

		//qf.pretty_print();
		
		boolean success = qf.search(0);
		
		long removed_fp = qf.delete(0);
		Assert(removed_fp > -1);
		
		qf.pretty_print();
		
		success = qf.search(0);
		Assert(!success);
		
		//qf.pretty_print();
		
		qf.expand();
		
		System.out.println("success");
	}
	
	// Here we're going to create a largish filter, and then perform insertions and rejuvenation operations
	// we'll test correctness by ensuring all keys we have inserted indeed still give positives
	static public void test17() {
 		int bits_per_entry = 8;
		int num_entries_power = 3;
		int seed = 5;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, false, -1);
		qf.expand_autonomously = true;
		TreeSet<Integer> added = new TreeSet<Integer>();
		Random rand = new Random(seed);
		double num_entries_to_insert = Math.pow(2, num_entries_power + 13); // we'll expand 3-4 times

		for (int i = 0; i < num_entries_to_insert; i++) {
			int rand_num = rand.nextInt();
			if (!added.contains(rand_num)) {
				System.out.println("insert key " + rand_num);
				if (rand_num == 1462809037) {
					System.out.println();
				}
				boolean success = qf.insert(rand_num, false);
				if (success) {
					added.add(rand_num);
					boolean found = qf.search(rand_num);
					if (!found) {
						System.out.println("failed on key " + rand_num);
						qf.pretty_print();
						System.exit(1);
					}

				}
			}

			if (i % 4 == 0 && i > Math.pow(2, num_entries_power)) {
				int to_del = rand.nextInt();
				if (to_del > added.first()) {
					
					int r = added.floor(to_del);
					added.remove(r);
					System.out.println("delete key " + r);
					long removed_fp =  qf.delete(r);
					if (removed_fp == -1) {
						System.out.println("not deleted");
						System.exit(1);
					}
				}
			}

			if (i % 2 == 0 && i > Math.pow(2, num_entries_power)) {
				int to_rejuv = rand.nextInt();
				if (to_rejuv > added.first()) {
					int key = added.floor(to_rejuv);
					//qf.pretty_print();
					qf.pretty_print();
					System.out.println("rejuv key " + key);
					if (key == -1157408321) {
						//System.out.println();
					}
					boolean rejuved = qf.rejuvenate(key);
					//qf.pretty_print();
					if (!rejuved) {
						System.out.println("not rejuvenated " + key);
						qf.pretty_print();
						qf.rejuvenate(key);
						
						System.exit(1);
					}
					boolean found = qf.search(key);
					if (!found) {
						System.out.println("failed to find key after rejuvenation  " + key);
						qf.pretty_print();
						System.exit(1);
					}
				}
			}

			int key = rand.nextInt();
			if (key > added.first()) {
				int to_query = added.floor(key);
				boolean found = qf.search(to_query);
				if (!found) {
					System.out.println("failed on key " + to_query);
					qf.pretty_print();
					System.exit(1);
				}
			}
		}

		for (Integer i : added) {
			boolean found = qf.search(i);
			if (!found) {
				System.out.println("test 18: something went wrong!! seem to have false negative " + i);
				qf.search(i);
				System.exit(1);
			}
		}
	}

	
	static void Assert(boolean condition) {
		if (!condition) {
			System.out.println("assertion failed");
			System.exit(1);
		}
	}

	
}
		
		
		
		

