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

import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import bitmap_implementations.Bitmap;
import infiniFilter_experiments.Experiment1;
import infiniFilter_experiments.ExperimentsBase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Tests {

	static public BitSet set_slot_in_test(BitSet result, int bits_per_entry, int slot, boolean is_occupied, boolean is_continuation, boolean is_shifted, long fingerprint) {
		int index = bits_per_entry * slot;
		result.set(index++, is_occupied); 
		result.set(index++, is_continuation); 
		result.set(index++, is_shifted); 
		for (int i = 0; i < bits_per_entry - 3; i++) {
			result.set(index++, Bitmap.get_fingerprint_bit(i, fingerprint) );
		}
		return result;
	}
	

	static public BitSet set_slot_in_test(BitSet result, int bits_per_entry, int slot, boolean is_occupied, boolean is_continuation, boolean is_shifted, String fingerprint) {
		long l_fingerprint = 0;
		for (int i = 0; i < fingerprint.length(); i++) {
			char c = fingerprint.charAt(i);
			if (c == '1') {
				l_fingerprint |= (1 << i);
			}
		}

		return set_slot_in_test(result, bits_per_entry, slot, is_occupied, is_continuation, is_shifted, l_fingerprint);
	}
	
	static public boolean check_equality(QuotientFilter qf, BitSet bs, boolean check_also_fingerprints) {
		for (int i = 0; i < bs.size(); i++) {
			if (check_also_fingerprints || (i % qf.bitPerEntry == 0 || i % qf.bitPerEntry == 1 || i % qf.bitPerEntry == 2)) {
				if (qf.get_bit_at_offset(i) != bs.get(i)) {
					System.out.println("failed test: bit " + i);
					System.exit(1);
				}
			}
		}
		return true;
	}
	

	
	// This test is based on the example from https://en.wikipedia.org/wiki/Quotient_filter
	// it performs the same insertions and query as the example and verifies that it gets the same results. 
	static public void test1() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		long fingerprint0 = 0;
		long fingerprint1 = (1 << bits_per_entry) - 1;
		//System.out.println(fingerprint1);

		qf.insert(fingerprint0, 1, false);
		qf.insert(fingerprint1, 4, false);
		qf.insert(fingerprint0, 7, false);
		qf.insert(fingerprint0, 1, false);
		qf.insert(fingerprint0, 2, false);
		qf.insert(fingerprint0, 1, false);

		// these are the expecting resulting is_occupied, is_continuation, and is_shifted bits 
		// for all slots contigously. We do not store the fingerprints here
		BitSet result = new BitSet(num_entries * bits_per_entry);		
		result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 3, false, true, true, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 4, true, false, true, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 5, false, false, true, fingerprint1);
		result = set_slot_in_test(result, bits_per_entry, 6, false, false, false, fingerprint0);
		result = set_slot_in_test(result, bits_per_entry, 7, true, false, false, fingerprint0);
		//qf.pretty_print();
		//qf.print_filter_summary();

		check_equality(qf, result, true);

		if (qf.num_physical_entries != 6) {
			System.out.print("counter not working well");
			System.exit(1);
		}
	}
	
	// This test is based on the example from the quotient filter paper 
	// it performs the same insertions as in Figure 2 and checks for the same result
	static public void test2() {
		int bits_per_entry = 8;
		int num_entries_power = 4;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(4, 8);

		qf.insert(0, 1, false);
		qf.insert(0, 1, false);
		qf.insert(0, 3, false);
		qf.insert(0, 3, false);
		qf.insert(0, 3, false);
		qf.insert(0, 4, false);
		qf.insert(0, 6, false);
		qf.insert(0, 6, false);

		BitSet result = new BitSet(num_entries * bits_per_entry);		
		result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 2, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 3, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 4, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 6, true, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 7, false, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 8, false, true, true, 0);
		check_equality(qf, result, false);

	}
	
	// insert some entries and make sure we get (true) positives for all entries we had inserted. 
	// This is to verify we do not get any false negatives. 
	// We then also check the false positive rate 
	static public void test_no_false_negatives(Filter filter, int num_entries) {
		HashSet<Integer> added = new HashSet<Integer>();
		int seed = 5;
		Random rand = new Random(seed);
		
		for (int i = 0; i < num_entries; i++) {
			int rand_num = rand.nextInt();
			boolean success = filter.insert(rand_num, false);
			if (success) {
				//System.out.println(i );
				added.add(rand_num);
			}
			else {
				System.out.println("insertion failed");
				filter.pretty_print();
				filter.insert(rand_num, false);
				System.exit(1);
			}

		}
		//qf.print_important_bits();
		//qf.pretty_print();

		for (Integer i : added) {
			//System.out.println("searching  " + i );
			boolean found = filter.search(i);
			if (!found) {
				System.out.println("something went wrong!! seem to have false negative " + i);
				filter.search(i);
				System.exit(1);
			}
		}
	}

	// test we don't get any false negatives for quotient filter
	static public void test3() {
		int bits_per_entry = 10;
		int num_entries_power = 10;
		QuotientFilter filter = new QuotientFilter(num_entries_power, bits_per_entry);
		int num_entries = (int)filter.max_entries_before_full;
		test_no_false_negatives(filter, num_entries);
	}
	
	// test we don't get any false negatives for bloom filter
	static public void test22() {
		int bits_per_entry = 11;
		int num_entries = 1024;
		Filter filter = new BloomFilter(num_entries, bits_per_entry);
		test_no_false_negatives(filter, num_entries);
	}
	
	static public void test23() {
		int bits_per_entry = 10;
		int power_entries = 10;
		Filter filter = new CuckooFilter(power_entries, bits_per_entry);
		int num_entries = (int) (Math.pow(2, power_entries) * 0.95);
		test_no_false_negatives(filter, num_entries);
	}

	// adds two entries to the end of the filter, causing an overflow
	// checks this can be handled
	static public void test4() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int num_entries = (int)Math.pow(2, num_entries_power);
		int fingerprint_size = bits_per_entry - 3;
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		long fp2 = 1 << fingerprint_size - 1;

		qf.insert(fp2, num_entries - 1, false);
		qf.insert(fp2, num_entries - 1, false);

		//qf.pretty_print();

		qf.delete(fp2, num_entries - 1);
		boolean found = qf.search(fp2, num_entries - 1);
		if (!found) {
			System.out.println("Should have found the entry");
			System.exit(1);
		}
	}
	
	
	
	// This is a test for deleting items. We insert many keys into one slot to create an overflow. 
	// we then remove them and check that the other keys are back to their canonical slots. 
	static public void test5() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		long fp1 = 1 << 4;
		long fp2 = 1 << 3;
		long fp3 = 1 << 2;
		long fp4 = 31;

		qf.insert(fp4, 1, false);
		qf.insert(fp1, 1, false);
		qf.insert(fp1, 1, false);
		qf.insert(fp2, 2, false);
		qf.insert(fp1, 1, false);
		qf.insert(fp1, 1, false);
		qf.insert(fp3, 4, false);


		//qf.pretty_print();
		qf.delete(31, 1);
		//qf.pretty_print();
		qf.delete(fp1, 1);
		//qf.pretty_print();
		qf.delete(fp1, 1);
		//qf.pretty_print();
		qf.delete(fp1, 1);
		//qf.pretty_print();
		qf.delete(fp1, 1);
		//qf.pretty_print();

		BitSet result = new BitSet(num_entries * bits_per_entry);	
		result = set_slot_in_test(result, bits_per_entry, 2, true, false, false, fp2);
		result = set_slot_in_test(result, bits_per_entry, 4, true, false, false, fp3);
		check_equality(qf, result, true);
		//qf.pretty_print();
	}
	
	// delete testing
	static public void test16() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		/*long fp1 = 1 << 4;
		long fp2 = 1 << 3;
		long fp3 = 1 << 2;
		long fp4 = 31;*/

		qf.insert(0, 1, false);
		qf.insert(0, 1, false);
		qf.insert(0, 2, false);
		qf.insert(0, 2, false);
		qf.insert(0, 3, false);
		qf.insert(0, 3, false);
		qf.insert(0, 3, false);
		qf.insert(0, 6, false);
		qf.insert(0, 6, false);
		qf.insert(0, 6, false);
		qf.insert(0, 7, false);

		//qf.pretty_print();
		//qf.delete(31, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		
		//qf.pretty_print();
		qf.delete(0, 2);
		//qf.pretty_print();
		qf.delete(0, 3);
		//qf.pretty_print();
		
		BitSet result = new BitSet(num_entries * bits_per_entry);	
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 3, true, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 4, false, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 6, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 7, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 8, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 9, false, false, true, 0);

		check_equality(qf, result, true);
		//qf.pretty_print();
	}
	
	// This is a test for deleting items. We insert many keys into one slot to create an overflow. 
	// we then remove them and check that the other keys are back to their canonical slots. 
	static public void test17() {
		int bits_per_entry = 8;
		int num_entries_power = 3;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		/*long fp1 = 1 << 4;
		long fp2 = 1 << 3;
		long fp3 = 1 << 2;
		long fp4 = 31;*/

		qf.insert(0, 1, false);
		qf.insert(0, 1, false);
		qf.insert(0, 2, false);
		qf.insert(0, 2, false);
		qf.insert(0, 3, false);
		qf.insert(0, 4, false);
		qf.insert(0, 4, false);
		qf.insert(0, 5, false);

		//qf.pretty_print();
		qf.delete(0, 3);
		//qf.pretty_print();
		
		BitSet result = new BitSet(num_entries * bits_per_entry);	
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 3, false, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 4, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 5, true, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 6, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 7, false, false, true, 0);
		check_equality(qf, result, true);
	}
	
	// This is a test for deleting items. We insert many keys into one slot to create an overflow. 
	// we then remove them and check that the other keys are back to their canonical slots. 
	/*static public void test18() {
		int bits_per_entry = 8;
		int num_entries_power = 5;
		int num_entries = (int)Math.pow(2, num_entries_power);
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		long fp1 = 1 << 4;
		long fp2 = 1 << 3;
		long fp3 = 1 << 2;
		long fp4 = 31;

		qf.insert(0, 1, false);
		qf.insert(0, 1, false);
		qf.insert(0, 2, false);
		qf.insert(0, 2, false);
		qf.insert(0, 3, false);
		qf.insert(0, 4, false);
		qf.insert(0, 4, false);
		qf.insert(0, 5, false);
		qf.insert(0, 6, false);
		qf.insert(0, 6, false);
		qf.insert(0, 7, false);
		qf.insert(0, 8, false);
		
		qf.insert(0, 10, false);
		qf.insert(0, 11, false);
		qf.insert(0, 12, false);
		
		qf.insert(0, 14, false);
		qf.insert(0, 15, false);
		
		qf.insert(0, 17, false);
		qf.insert(0, 17, false);
		qf.insert(0, 19, false);

		//qf.delete(31, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		//qf.delete(fp1, 1);
		//qf.pretty_print();
		
		qf.pretty_print();
		qf.delete(0, 3);
		qf.pretty_print();
		
		BitSet result = new BitSet(num_entries * bits_per_entry);	
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 3, true, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 4, false, false, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 6, true, false, false, 0);
		result = set_slot_in_test(result, bits_per_entry, 7, true, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 8, false, true, true, 0);
		result = set_slot_in_test(result, bits_per_entry, 9, false, false, true, 0);

		check_equality(qf, result, true);
		//qf.pretty_print();
	}*/

	static public void test6() {

		int bits_per_entry = 8;
		int num_entries_power = 4;
		//int num_entries = (int)Math.pow(2, num_entries_power);
		//int fingerprint_size = bits_per_entry - 3;
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		qf.insert(0, 2, false);
		qf.insert(0, 3, false);
		qf.insert(0, 3, false);
		qf.insert(0, 4, false);
		qf.insert(0, 23, false); // last key in the filter
		qf.insert(0, 24, false); // outside the bounds, logical slot 14 does not exist logically, even if it might exist physically 

		Iterator it = new Iterator(qf);
		int[] arr = new int[] {2, 3, 3, 4, 23};
		int arr_index = 0;


		while (it.next()) {
			//System.out.println(it.bucket_index);
			if (arr[arr_index++] != it.bucket_index) {
				System.out.print("error in iteration");
				System.exit(1);
			}
		}

	}

	static public void test7() {

		int bits_per_entry = 8;
		int num_entries_power = 4;
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

		qf.insert(0, 1, false);
		qf.insert(0, 4, false);
		qf.insert(0, 7, false);
		//qf.pretty_print();
		qf.insert(0, 1, false);
		qf.insert(0, 2, false);
		//qf.pretty_print();
		qf.insert(0, 1, false);
		qf.insert(0, 15, false);

		Iterator it = new Iterator(qf);
		int[] arr = new int[] {1, 1, 1, 2, 4, 7, 15};
		int arr_index = 0;


		//qf.pretty_print();
		while (it.next()) {
			//System.out.println(it.bucket_index);
			if (arr[arr_index++] != it.bucket_index) {
				System.out.print("error in iteration");
				System.exit(1);
			}
		}
	}

	// In this test, we create one FingerprintShrinkingQF and expand it once.
	// We also create an expanded Quotient Filter with the same data from the onset and make sure they are logically equivalent. 
	static public void test8() {

		int bits_per_entry = 10;
		int num_entries_power = 4;
		FingerprintSacrifice qf = new FingerprintSacrifice(num_entries_power, bits_per_entry);
		qf.max_entries_before_full = Integer.MAX_VALUE; // disable automatic expansion
		//qf.print_key(1);

		for (int i = 0; i < 12; i++) {
			qf.insert(i, false);
		}

		//qf.pretty_print();
		qf.expand();
		//qf.pretty_print();

		QuotientFilter qf2 = new QuotientFilter(num_entries_power + 1, bits_per_entry - 1);

		for (int i = 0; i < 12; i++) {
			qf2.insert(i, false);
		}

		//qf2.pretty_print();

		if (qf.filter.size() != qf2.filter.size()) {
			System.out.print("filters have different sizes");
			System.exit(1);
		}

		for (int i = 0; i < qf.get_logical_num_slots(); i++) {
			Set<Long> set1 = qf.get_all_fingerprints(i);
			Set<Long> set2 = qf2.get_all_fingerprints(i);

			if (!set1.equals(set2)) {
				System.out.print("fingerprints for bucket " + i + " not identical");
				System.exit(1);
			}
		}
	}

	// insert entries across two phases of expansion, and then check we can still find all of them
	static public void test9() {

		int bits_per_entry = 10;
		int num_entries_power = 3;
		Chaining qf = new Chaining(num_entries_power, bits_per_entry);
		qf.max_entries_before_full = Integer.MAX_VALUE; // disable automatic expansion

		int i = 0;
		while (i < Math.pow(2, num_entries_power) - 2) {
			boolean success = qf.insert(i, false);
			if (!success) {
				System.out.println("test9: insertion should have worked");
				System.exit(1);
			}
			i++;
		}
		qf.expand();
		
		//qf.pretty_print();
		while (i < Math.pow(2, num_entries_power + 1) - 2) {
			boolean success = qf.insert(i, false);
			if (!success) {
				System.out.println("test9: insertion should have worked");
				System.exit(1);
			}
			i++;
		}

		for (int j = 0; j < i; j++) {
			if ( !qf.search(j) ) {
				System.out.println("test9: false negative  " + j);
				System.exit(1);
			}
		}

	}

	static public void test10() {
		int bits_per_entry = 10;
		int num_entries_power = 3;		
		BasicInfiniFilter qf = new BasicInfiniFilter(num_entries_power, bits_per_entry);
		qf.expand_autonomously = false;
		qf.hash_type = HashType.arbitrary;
		int i = 1;
		while (i < Math.pow(2, num_entries_power) - 1) {
			boolean success = qf.insert(i, false);
			i++;
			if (!success) {
				System.out.println("insertion failed");
			}
		}

		//qf.pretty_print();
		qf.expand();
		//qf.pretty_print();

		int num_entries = 1 << ++num_entries_power;
		BitSet result = new BitSet(num_entries * bits_per_entry);		
		result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, "0000000");
		result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, "1100101");
		result = set_slot_in_test(result, bits_per_entry, 2, true, false, false, "1010101");
		result = set_slot_in_test(result, bits_per_entry, 3, false, false, false, "0000000");
		result = set_slot_in_test(result, bits_per_entry, 4, false, false, false, "0000000");
		result = set_slot_in_test(result, bits_per_entry, 5, true, false, false, "0010001");
		result = set_slot_in_test(result, bits_per_entry, 6, false, false, false, "0000000");
		result = set_slot_in_test(result, bits_per_entry, 7, true, false, false, "0101101");
		result = set_slot_in_test(result, bits_per_entry, 8, true, false, false, "1001001");
		result = set_slot_in_test(result, bits_per_entry, 9, false, true, true, "0111001");
		//qf.pretty_print();
		check_equality(qf, result, true);

		i = 1;
		while (i < Math.pow(2, num_entries_power - 1) - 1) {
			boolean found = qf.search(i);
			//qf.compare(0, 0);
			if (!found) {
				System.out.println("not found entry " + i + "   test 10");
				System.exit(1);
			}
			i++;
		}
	}

	// this test ensures we issue enough insertions until the fingerprints of at least some of the first entries inserted 
	// run out. This means that for these entries, we are going to try the chaining technique to avoid false negatives. 
	static public void test12() {
		int bits_per_entry = 7;
		int num_entries_power = 3;		
		ChainedInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.expand_autonomously = true;
		qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.POLYNOMIAL;
		//int max_key = (int)Math.pow(2, num_entries_power + qf.fingerprintLength * 4 + 1 );
		int max_key = (int)Math.pow(2, num_entries_power + qf.fingerprintLength * 3 + 7 );
		for (int i = 0; i < max_key; i++) {
			//System.out.println(i);

			if (i == 25) {
				//System.out.println();
			}
			
			boolean success = qf.insert(i, false);
			if (!success) {
				System.out.println("insertion " + i + " failing at test12");
				System.exit(1);
			}
			
			if (i == 6577) {
				//System.out.println();
			}
			if (qf.secondary_IF != null && !qf.search(25)) {
				//System.out.println("");
			}
			
			if (26320 == i) {
				//qf.pretty_print();
				//boolean found = qf.search(66);
				//System.out.println();
			}
			
			boolean found = qf.search(66);
			if (!found && i > 66) {
				System.out.println("not found entry " + i + " in test12");
				//qf.pretty_print();
				//qf.search(6);
				System.exit(1);
			}
		}

		for (int i = 0; i < max_key; i++) {

			boolean found = qf.search(i);
			if (!found) {
				System.out.println("not found entry " + i + " in test12");
				System.exit(1);
			}
		}

		int false_positives = 0;
		for (int i = max_key; i < max_key + 10000; i++) {
			boolean found = qf.search(i);
			if (found) {
				false_positives++;
			}
		}
		if (false_positives == 0) {
			System.out.println("should have had a few false positives");
			System.exit(1);
		}
		//qf.pretty_print();
		//qf.print_filter_summary();
	}

	// here we test the rejuvenation operation of InfiniFilter
	static public void test13() {
		int bits_per_entry = 7;
		int num_entries_power = 2;		
		BasicInfiniFilter qf = new BasicInfiniFilter(num_entries_power, bits_per_entry);
		qf.hash_type = HashType.arbitrary;
		qf.expand_autonomously = false;

		qf.insert(2, false);
		//qf.pretty_print();
		qf.expand();
		//qf.pretty_print();
		qf.rejuvenate(2);
		//qf.pretty_print();

		BitSet result = new BitSet((int)qf.get_logical_num_slots() * bits_per_entry);		
		result = set_slot_in_test(result, bits_per_entry, 0, true, false, false, 3);

		check_equality(qf, result, true);
	}
	
	
	// Testing the capability of InfiniFilter to delete the longest matching fingerprint
	static public void test14() {
		int bits_per_entry = 8;
		int num_entries_power = 2;
		int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new BasicInfiniFilter(num_entries_power, bits_per_entry);
		
		int fp1 = 1;
		int fp2 = 2;
		int fp3 = 0;
		
		qf.insert(fp1, 1, false);
		
		qf.expand();
		
		qf.insert(fp3, 5, false);
		
		qf.insert(fp2, 5, false);

		//qf.pretty_print();
		
		qf.delete(fp3, 5);  // we must delete the longest matching fingerprint, o
		//qf.pretty_print();


		BitSet result = new BitSet(num_entries * bits_per_entry);	
		
		result = set_slot_in_test(result, bits_per_entry, 5, true, false, false, 16);
		result = set_slot_in_test(result, bits_per_entry, 6, false, true, true, fp2);
		check_equality(qf, result, true);
		//qf.pretty_print();
	}
	
	static public void test15() {
		int bits_per_entry = 10;
		int num_entries_power = 3;
		int seed = 2;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.expand_autonomously = true;
		test_insertions_and_deletes(qf);
	}
	
	// Here we're going to create a largish filter, and then perform deletes and insertions
	// we want to make sure we indeed get a positive for every entry that we inserted and still not deleted
	// for every 2 insertions, we make one deletes, in order to still allow the filter to expand 
	static public void test_insertions_and_deletes(BasicInfiniFilter qf) {
		int num_entries_power = qf.power_of_two_size;
		int seed = 2;  // 10
		TreeSet<Integer> added = new TreeSet<Integer>();
		Random rand = new Random(seed);
		double num_entries_to_insert = Math.pow(2, num_entries_power + 10); // we'll expand 3-4 times
		
		for (int i = 0; i < num_entries_to_insert; i++) {
			int rand_num = rand.nextInt();
			if (!added.contains(rand_num)) {
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
					//boolean deleted = true;
					long removed_fp = qf.delete(r);
					if (removed_fp == -1) {
						System.out.println("not deleted");
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
				System.out.println("test 15: something went wrong!! seem to have false negative " + i);
				qf.search(i);
				System.exit(1);
			}
		}
	}
	

	// Here we're going to create a largish filter, and then perform insertions and rejuvenation operations
	// we'll test correctness by ensuring all keys we have inserted indeed still give positives
	static public void test18() {
 		int bits_per_entry = 16;
		int num_entries_power = 3;
		int seed = 5;  // 10
		//int num_entries = (int)Math.pow(2, num_entries_power);
		BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
		qf.expand_autonomously = true;
		TreeSet<Integer> added = new TreeSet<Integer>();
		Random rand = new Random(seed);
		double num_entries_to_insert = Math.pow(2, num_entries_power + 15); // we'll expand 3-4 times

		for (int i = 0; i < num_entries_to_insert; i++) {
			int rand_num = rand.nextInt();
			if (!added.contains(rand_num)) {
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

					boolean rejuved = qf.rejuvenate(key);
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
	
	
	//Testing xxhash
		static public void test19() {
			final long[] HASHES_OF_LOOPING_BYTES_WITHOUT_SEED = {
				-1205034819632174695L,
				-1642502924627794072L,
				5216751715308240086L,
				-1889335612763511331L,
				-13835840860730338L,
				-2521325055659080948L,
				4867868962443297827L,
				1498682999415010002L,
				-8626056615231480947L,
				7482827008138251355L,
				-617731006306969209L,
				7289733825183505098L,
				4776896707697368229L,
				1428059224718910376L,
				6690813482653982021L,
				-6248474067697161171L,
				4951407828574235127L,
				6198050452789369270L,
				5776283192552877204L,
				-626480755095427154L,
				-6637184445929957204L,
				8370873622748562952L,
				-1705978583731280501L,
				-7898818752540221055L,
				-2516210193198301541L,
				8356900479849653862L,
				-4413748141896466000L,
				-6040072975510680789L,
				1451490609699316991L,
				-7948005844616396060L,
				8567048088357095527L,
				-4375578310507393311L,
				-3749919242623962444L,
				888155921178136237L,
				-228195649085979072L,
				-521095004075279741L,
				-2458702038214709156L,
				-2792334161285995319L,
				7509323632532862410L,
				46046374822258777L,
				-731200582691896855L,
				933917387460394992L,
				5623144551929396680L,
				6456984547425914359L,
				-6398540474588876142L,
				1224372500617079775L,
				-931727396974525131L,
				979677643219401656L,
				-8078270932489049756L,
				-92767506898879473L,
				2379112167176776082L,
				2065719310945572007L,
				-4972682801816081667L,
				-7346559332994187462L,
				4674729779638751546L,
				5844780159702313017L,
				925606237565008152L,
				8164325403643669774L,
				5124005065773312983L,
				-4646462236086916483L,
				4733593776494364101L,
				-6408850806317360L,
				7405089268865026700L,
				-2131704682637193649L,
				-592659849139514384L,
				-4386868621773355429L,
				-2216833672566288862L,
				4022619316305276641L,
				-60464713570988944L,
				2416749694506796597L,
				3576590985110933976L,
				3368688771415645536L,
				-357157638897078259L,
				3484358739758473117L,
				2078888409435083535L,
				8053093288416703076L,
				-4934736471585554038L,
				-7784370683223414061L,
				-4109284735634941390L,
				5982490102027564625L,
				-4991107002810882893L,
				8664747912276562373L,
				8536879438728327651L,
				2358675440174594061L,
				5352236919104495867L,
				6340852522718110192L,
				5075606340464035668L,
				-6313168920073458239L,
				-6428599582591385786L,
				-7278654800402467208L,
				-6630626099856243581L,
				-7548742438664634646L,
				5514383762309532642L,
				-5996126265702944431L,
				4011116741319319261L,
				-7289240093981845088L,
				4975257207486779926L,
				-3945500877932691916L,
				1973955144068521079L,
				3884425912161913184L,
				7692681977284421015L,
				-1616730378439673826L,
				4799493270916844476L,
				-6107310582897997679L,
				3643294092300179537L,
				5406040598516899149L,
				-3032420409304067208L,
				5044227119457305622L,
				9165032773225506149L,
				7553488247682850248L,
				2247298339072845043L,
				7380491470304042584L,
				-456791943260357427L,
				-1906500292613319324L,
				-4025157985304129897L,
				6167829983725509611L,
				-8678196943431064825L,
				-636391087313417831L,
				5757999497725839182L,
				8999325347316115948L,
				-6042339776328081249L,
				7988836354190359013L,
				2818448030979902104L,
				-8484201484113382447L,
				-1140175406473847155L,
				3042776987426497381L,
				3147338037480432386L,
				5065714330193756569L,
				8827021486636772242L,
				838335823706922959L,
				481844220820054909L,
				5333474685474667077L,
				-3722898251196013565L,
				7909417627390150381L,
				7116148225996109646L,
				7520381989775811302L,
				6045444672904719015L,
				169039646730338133L,
				-2144629916252757106L,
				-3752608501798118554L,
				8374704774878780935L,
				-5830926781667225570L,
				3202139393110256022L,
				4400219135677717216L,
				-5663710220155589201L,
				-2589002340345751622L,
				-8240133511464343390L,
				-4036798392879835146L,
				501599054729008501L,
				-4851415719238782188L,
				7565157933617774080L,
				-6428091359957700043L,
				4081845077806300175L,
				-9016659258880122392L,
				7811786097015457596L,
				1357606791019752376L,
				6522211979684949668L,
				-3462397075047559451L,
				3075504459164148117L,
				3055992297861390732L,
				-7230492327399411047L,
				-1128103378253532506L,
				1834607408788151585L,
				7065978976369231860L,
				6566122632438908362L,
				-3440855531356735824L,
				6271453770746181891L,
				413365468403580071L,
				-8342682158827061522L,
				-3713303136987568731L,
				-8959326895824091541L,
				-2793862582117663595L,
				-184756427409317729L,
				-7052502019782453427L,
				3666196071825438258L,
				170204095295428634L,
				-1880693509859077843L,
				5179169206996749826L,
				2866097700453114958L,
				1859104195026275510L,
				3782323564639128125L,
				-6485194456269981193L,
				6761934873296236857L,
				5764605515941066448L,
				597754945258033208L,
				-4888986062036739232L,
				-6490228233091577705L,
				3234089784845854336L,
				-5506883591180767430L,
				1491493862343818933L,
				3232293217886687768L,
				-4079803366160739972L,
				4884134040093556099L,
				-7274733680156962461L,
				5265680254123454403L,
				1036855740788018258L,
				423439784169709263L,
				-3627743032115866622L,
				-6311378083791982305L,
				-3058076915688265687L,
				5826550132901840796L,
				8049712006832885455L,
				1707844692241288946L,
				-3293048440386932248L,
				-2458638193238955307L,
				943059295184967928L,
				3899561579431348819L,
				-1516862862245909493L,
				4448476568037673976L,
				8738531437146688925L,
				-1033913449611929894L,
				733668166271378558L,
				438686375775205249L,
				-4325889118346169305L,
				-238178883117433622L,
				-7972205050662019794L,
				-1263398103237492853L,
				-8333197763892905802L,
				7796341294364809534L,
				-1381767618016537445L,
				2892579485651013970L,
				-3376209887503828920L,
				-8575120126045607817L,
				-1609355362031172055L,
				-386138918275547508L,
				4598874691849543747L,
				-2961781601824749597L,
				-3032925351997820092L,
				-4256249198066449735L,
				6712291718681474012L,
				-4281614253751277086L,
				3727487933918100016L,
				-2744649548868700294L,
				8662377383917584333L,
				-9154398439761221404L,
				-6895275824272461794L,
				3394857180017540444L,
				2010825527298793302L,
				4894417464710366872L,
				-6879244364314087051L,
				83677167865178033L,
				-8258406393927169823L,
				5042126978317943321L,
				6485279223034053259L,
				4442956705009100620L,
				316801800427881731L,
				1381431847939703076L,
				5172932759041399062L,
				-69656533526213521L,
				-5302643413630076306L,
				-3956089084400440856L,
				372087412941022771L,
				4711314482928419386L,
				3255220726505012060L,
				8917854303046844847L,
				1116214654602499731L,
				2282408585429094475L,
				-9207590323584417562L,
				8881688165595519866L,
				1731908113181957442L,
				3847295165012256987L,
				4457829016858233661L,
				4944046822375522396L,
				3445091217248591320L,
				-5055680960069278553L,
				-399195423199498362L,
				-8109174165388156886L,
				4967185977968814820L,
				-5911973391056763118L,
				2239508324487797550L,
				-954783563382788523L,
				8523699184200726144L,
				932575865292832326L,
				-7491448407022023047L,
				1809887519026638446L,
				-8610524715250756725L,
				6158809695983348998L,
				4948400960714316843L,
				-4513370424175692831L,
				-3955280856263842959L,
				6440233015885550592L,
				8756942107256956958L,
				7895095834297147376L,
				370033091003609904L,
				948078545203432448L,
				-8523229038380945151L,
				100794871657160943L,
				-2186420796072284323L,
				-9221115378196347951L,
				8102537654803861332L,
				5857339063191690550L,
				-4554257374958739421L,
				6607496554818971053L,
				-778402196622557070L,
				-3817535277727878318L,
				3564122000469288769L,
				-44446230828995950L,
				1322708749649533240L,
				6150374672341998205L,
				-3300275952549095391L,
				5700833512536085850L,
				-8559358370491270937L,
				5434443260519512697L,
				-8031025173259990945L,
				7117462129248544172L,
				5425177419943569451L,
				-7215427371174054838L,
				-5728669976971194528L,
				-2096361446095323077L,
				-4247416835972286805L,
				4912769047482466787L,
				7755341152739082452L,
				6797061233443658471L,
				4089361562209715474L,
				5830701413838808929L,
				5514515889578551370L,
				609334005368729318L,
				177310574483850759L,
				-820431153866372784L,
				7188454041446661654L,
				7480194911613035473L,
				4564607884390103056L,
				888496928954372093L,
				-5480535802290619117L,
				9100964700413324707L,
				510523132632789099L,
				8249362675875046694L,
				5340321809639671537L,
				-4633081050124361874L,
				-839915092967986193L,
				-7377542419053401928L,
				1820485955145562839L,
				8517645770425584256L,
				-1877318739474090786L,
				7674371564231889244L,
				-3311130470964498678L,
				-880090321525066135L,
				-5670998531776225745L,
				-8828737503035152589L,
				-6029750416835830307L,
				-6535608738168818581L,
				-550872341393232043L,
				2831504667559924912L,
				-4613341433216920241L,
				502960879991989691L,
				576723875877375776L,
				-2575765564594953903L,
				-4642144349520453953L,
				7939746291681241029L,
				6486356905694539404L,
				-9086235573768687853L,
				5369903658359590823L,
				3199947475395774092L,
				8384948078622146995L,
				-3365598033653273878L,
				-2525526479099052030L,
				2648498634302427751L,
				3715448294999624219L,
				-4734466095330028983L,
				-8440427851760401644L,
				-371198022355334589L,
				8864079431738600817L,
				-4205600060099565684L,
				6617166152874298882L,
				-6515522971156180292L,
				7254251246745292298L,
				-420587237082849417L,
				1190495815435763349L,
				-474540026828753709L,
				-8150622114536376016L,
				-5790621848044235275L,
				-2780522220219318167L,
				-2991155855957250848L,
				1692932912262846366L,
				8814949734565782733L,
				-8746818869495012552L,
				7931250816026891600L,
				-7434629709560596700L,
				4388261932396122996L,
				7154847153195510802L,
				-2810154398655124882L,
				2601892684639182965L,
				7781574423676509607L,
				-6647000723020388462L,
				-8679132292226137672L,
				-2447013202020963672L,
				3658855631326217196L,
				2176620921764007759L,
				3654402165357492705L,
				4511989090021652156L,
				-3254638803798424003L,
				9050506214967102331L,
				922579360317805810L,
				609820949221381248L,
				5723875594772949290L,
				4637721466210023638L,
				6195303339320487374L,
				-38202587086649325L,
				-2142927092331878341L,
				5355751314914287101L,
				-7170892783575760055L,
				-7506612729078573199L,
				8645580445823695595L,
				3221950179890871958L,
				1638211443525398634L,
				7356718304253861777L,
				-296260062751271549L,
				-1790105985391377345L,
				-7004118620405119098L,
				7056012094479909462L,
				-7673357898031223798L,
				-8929502135696203556L,
				7527161467311997998L,
				6182865571027510002L,
				-2163310275402596869L,
				6285112477695252864L,
				3703909999924067987L,
				962491298117560533L,
				138936592567072793L,
				6094857527471100960L,
				5914305068838335718L,
				-8896724991235492552L,
				-2667562314507789198L,
				-7456492499188304500L,
				-3422709784851063201L,
				-1511644999824238281L,
				-7130158069449057322L,
				6243266426571961929L,
				2713895636371672711L,
				5765589573821453640L,
				2624585483746388367L,
				3933828437519859601L,
				-5664404238108533781L,
				7086393398544811684L,
				1322058227068490376L,
				-8232508114671021371L,
				-5963804389649678229L,
				-3318229976491806899L,
				-6261789542948241754L,
				199130260709663583L,
				7521707465510595039L,
				507353862067534334L,
				-7737968456769005928L,
				-8964687882992257099L,
				-7735003539801528311L,
				6989812739838460574L,
				-6986289777499051441L,
				1881562796144865699L,
				-6077719780113966592L,
				-5427071388091979746L,
				1660707436425817310L,
				-4338189980197421104L,
				5330934977599207307L,
				4461280425701571033L,
				-7426107478263746863L,
				4258305289832328199L,
				-8003283151332860979L,
				-2500604212764835216L,
				-8883941775298564436L,
				-5059709834257638733L,
				-4582947579039913741L,
				1371959565630689983L,
				-1925163414161391371L,
				-1180269729544278896L,
				-6603171789097590304L,
				8985062706306079731L,
				-3588748723254272836L,
				-6052032019910018725L,
				6200960040430493088L,
				2146343936795524980L,
				7785948646708747443L,
				4524411768393719400L,
				749211414228926779L,
				-163844243342465015L,
				1066801203344117463L,
				-3687825939602944988L,
				-4873811917429870500L,
				-3765115783578949524L,
				3344884226049804020L,
				-22793631121165636L,
				-5636541624133159076L,
				-6201449576244177151L,
				-4533734412127714050L,
				-2064657727206266594L,
				-1325853623186040989L,
				-2651306529045029511L,
				903264360879626406L,
				6082283797495873520L,
				6185446819995987847L,
				-5727850940826115079L,
				8356646143516726527L,
				-7705915341280821272L,
				9137633133909463406L,
				6613483969797411894L,
				8598514961735984460L,
				6805925079991408361L,
				6009403222422527608L,
				2216303622650116705L,
				-3736062178532154638L,
				-7139008962939637477L,
				-1537711200058404375L,
				8896755073380580322L,
				-6063426810787442347L,
				-3472064301690015285L,
				-4568131486464952371L,
				-8141256104294687045L,
				5627435360893599536L,
				1136003802967708029L,
				2730027518034735037L,
				1985287040172139729L,
				-3643431491383365431L,
				-9042919736106376701L,
				8879968900590373568L,
				8504486139877409399L,
				5832665747670146536L,
				4202923651402292496L,
				1738511892080946286L,
				4512683881549777042L,
				9200194457599870145L,
				-1948301178705617139L,
				8655715314401162523L,
				412698981651521600L,
				-1479274044808688580L,
				2688302549664693359L,
				-3059920027366623178L,
				-4275753325231806565L,
				-8321791698013769889L,
				-3678119714812414102L,
				-2500922551770832553L,
				9018541633115002061L,
				5713301371152396803L,
				4180584812840471799L,
				3062416401091271879L,
				-8125716681035757962L,
				-2076056159878596225L,
				8855540523533374738L,
				2402007906402689092L,
				2020584786288649542L,
				1707405964421070701L,
				-3681994462249973122L,
				-3982567775984742012L,
				7133200226358561844L,
				-5270514263562558963L,
				9060760368219219429L,
				-6967162372382490281L,
				-9094664463528453384L,
				-3968518633408880046L,
				8618660189330281694L,
				-4668946581954397558L,
				-8596433172676363407L,
				-1264942061713169049L,
				-5309493221793643795L,
				-1099320768477039529L,
				8925041285873295227L,
				-6809278181760513499L,
				-7039439984223885585L,
				6188209901527865226L,
				1487353394192637059L,
				2402097349430126337L,
				-3818359601525025681L,
				4123217079279439249L,
				-1424515143377220376L,
				1742298536803356877L,
				-2836832784751148874L,
				-4838603242771410698L,
				2383745618623084414L,
				-2790832243316548423L,
				-1176683649587660160L,
				1862928178605117401L,
				5208694030074527671L,
				4339841406618876548L,
				-7704801448691668472L,
				500068664415229033L,
				-2111184635274274347L,
				-1387769336519960517L,
				-2368660677263980293L,
				-4980481392402938776L,
				-6856361166068680884L,
				1708658704968066797L,
				-9013068514618931938L,
				-2616479975851677179L,
				7121103440247327570L,
				-7094192881960646061L,
				-4042342930006488618L,
				5294323611741266775L,
				5235545113690922502L,
				-2562011392475214878L,
				-4613304566070234734L,
				-3784386310583029381L,
				-4526148219816534267L,
				-8643470129031767968L,
				-4573761335510927866L,
				-8255399593563317902L,
				-1925488377092111963L,
				-1747797357090594237L,
				7292772921748919564L,
				3951718848780851600L,
				5339305877764077075L,
				7889570407201305102L,
				-8935437555550449315L,
				-1858205318388884024L,
				381779657795494278L,
				-3769854251228686168L,
				-7957724087073627355L,
				4349540075286824743L,
				-2476434494603040708L,
				-4506107235113109706L,
				-7120863144673563848L,
				-8534342596639587598L,
				2205658724629050493L,
				604438195864305027L,
				4530331938860561927L,
				-2074141653226683751L,
				-1114378227875974007L,
				3377301950002508302L,
				5369356700690664306L,
				-1747063224581819445L,
				-6320380781966280801L,
				-2075443262555773155L,
				1028541493355576591L,
				-4694402890123574860L,
				-5250660999767019003L,
				3847087895315315136L,
				-4448050214964317066L,
				-4591316307978008151L,
				4894820902772635901L,
				3088847887353411593L,
				-6699208183127463352L,
				4636731998354510780L,
				9095126525233209263L,
				4135373626035182291L,
				3835688804093949701L,
				-3490782692819028324L,
				-561453348486424761L,
				-3329283619698366365L,
				3251154327320814221L,
				-8006986328190314286L,
				5856651505286251345L,
				-8871425101391073L,
				7806993676637210959L,
				7586479850833664643L,
				-7091216108599847229L,
				-3410137297792125447L,
				-8251963871271100526L,
				-8849730915506517177L,
				8400334327557485676L,
				1676125861848906502L,
				-8480324002538122254L,
				-1402216371589796114L,
				5951911012328622382L,
				8596811512609928773L,
				-2266336480397111285L,
				-8840962712683931463L,
				4301675602445909557L,
				1843369157327547440L,
				2169755460218905712L,
				-1592865257954325910L,
				-8763867324602133653L,
				-4283855559993550994L,
				-7577702976577664015L,
				-5152834259238990784L,
				4596243922610406362L,
				-4326545138850544854L,
				1480440096894990716L,
				8548031958586152418L,
				6705615952497668303L,
				-2915454802887967935L,
				-6137002913510169520L,
				2908515186908319288L,
				5834242853393037250L,
				-6721431559266056630L,
				-7810820823419696676L,
				1954209413716096740L,
				6657013078387802473L,
				2214178984740031680L,
				8789512881373922013L,
				1240231669311237626L,
				8694612319028097761L,
				492180561068515854L,
				-6047127535609489112L,
				7436686740711762797L,
				-4520261623507558716L,
				938282189116272147L,
				3232025564608101134L,
				-5425498066931840551L,
				932123105892452494L,
				9054941090932531526L,
				8066693670021084601L,
				764877609198828864L,
				-489112437588815338L,
				4827691353685521957L,
				1948321254606741278L,
				6117773063719937712L,
				4645962658121906639L,
				-7846887104148029590L,
				4210795945791252618L,
				-8879516722990993098L,
				-2621063563373927241L,
				2094675051444850863L,
				-8681225697045319537L,
				6072534474938492189L,
				6181923696407824226L,
				5463607676777614919L,
				3708342890820711111L,
				8844501223821777366L,
				-1459359143442302680L,
				2225439088478089068L,
				-3866259492807347627L,
				5715020051188773955L,
				3922300588924895992L,
				-9142841818158905228L,
				2234845285375211931L,
				2466598091809457099L,
				-5086614780930363190L,
				-59740786891006359L,
				3484340182077240897L,
				5684798394905475931L,
				8492255409537329167L,
				5276601975076232447L,
				-723955912320185993L,
				9032937149732310432L,
				2226206333274026280L,
				5631303328800272036L,
				3943832708526382713L,
				-3756282686478033644L,
				-5407377327559185078L,
				2025162219823732106L,
				-8802502232162774782L,
				9039368856081455195L,
				663058667658971174L,
				3624269418844967319L,
				1835338408542062149L,
				6821836507221295281L,
				6273547355770435776L,
				-3104373869480308814L,
				1150888014781722836L,
				7638478751521711777L,
				-6407096352658729423L,
				-2242514077180426481L,
				-3181824045541296523L,
				-4562287221569080073L,
				-5550768647534615669L,
				-5786611484859469238L,
				-6147722345444149090L,
				3737249616177808079L,
				3401215612108618403L,
				-713522925214097648L,
				7938558781452631257L,
				-2822931074351003413L,
				-6484774850345918944L,
				3384659068511379086L,
				6976459554734427695L,
				4254162229878558339L,
				-3312164339867139602L,
				7263045146222903358L,
				4561625003713187235L,
				-3350421200373539593L,
				-6329267008823047447L,
				-6889593333717619051L,
				-6470291206680780949L,
				-1925391510610223335L,
				4955720513801530785L,
				-6515999401129420095L,
				-5146900596178823847L,
				2572121582663686783L,
				-4958678197003031937L,
				-1295912792184970105L,
				-8320363273488883198L,
				-8213501149234986129L,
				-3883775881968950160L,
				-8925953418077243474L,
				3199784299548492168L,
				-6836506744583692202L,
				-5007347279129330642L,
				7387675960164975441L,
				-5841389805259238070L,
				6263589037534776610L,
				3327727201189139791L,
				3673450414312153409L,
				-1563909967243907088L,
				-3758518049401683145L,
				6368282934319908146L,
				-6025191831649813215L,
				1223512633484628943L,
				-8540335264335924099L,
				-8569704496403127098L,
				-5712355262561236939L,
				-6468621715016340600L,
				7015005898276272746L,
				-1037164971883038884L,
				-6108649908647520338L,
				-6781540054819591698L,
				-2762739023866345855L,
				-270930832663123436L,
				-2387080926579956105L,
				-3984603512651136889L,
				2367015942733558542L,
				2997123688964990405L,
				-424413420483149165L,
				2906467516125124288L,
				7979917630945955701L,
				2879736983084566817L,
				558436267366797870L,
				6471658168855475843L,
				-3453803644372811678L,
				95470628886709014L,
				5666911245054448862L,
				1594133734978640945L,
				3790246368687946045L,
				8636400206261643605L,
				5901994795106283147L,
				-6774812279971490610L,
				-4622588246534854941L,
				5395884908872287278L,
				7381412950348018556L,
				5461775216423433041L,
				2851500852422732203L,
				1153428834012773824L,
				2567326223464897798L,
				6290362916558214218L,
				6095765709335097474L,
				-3526424734043456674L,
				-8411274175041022530L,
				7565408328520233290L,
				-1318636864706103626L,
				1261242784453012654L,
				-472643963000448611L,
				-7126293899612852456L,
				5072187962931197668L,
				4775251504230927816L,
				-1624676500499667689L,
				2252385971292411863L,
				7908437759266752884L,
				-8948829914565397845L,
				5258787823809553293L,
				3885696202809019506L,
				-4551784314460062669L,
				5315762970089305011L,
				7218180419200466576L,
				109471822471146966L,
				3901499100759315793L,
				-5613018173558603696L,
				5782419706003468119L,
				8285176821902721729L,
				-2944182278904878473L,
				8089487615165958290L,
				6934039118340963316L,
				8481603619533191729L,
				-6321491167299496492L,
				6441589800192421521L,
				6436057639713571196L,
				6819921695214365155L,
				1185928916708893611L,
				2597068862418243401L,
				-7637601550649263782L,
				9129303862479379164L,
				4047905726243458335L,
				6672087858539795207L,
				-4841432774404255351L,
				5501215987763227677L,
				-5300305896512100453L,
				1635946349436492617L,
				-5017459781050596604L,
				-7313558338536196566L,
				4625509831332846264L,
				-1241826701278444028L,
				2916178164108211239L,
				-6947453283344846915L,
				5520544791845620925L,
				5009241392834567026L,
				-630825152277572403L,
				6246654103747517292L,
				-5632205909016659384L,
				-5099826214945383802L,
				2466330894206710401L,
				-1463559257726812272L,
				4922422449110036517L,
				-4940410396057186660L,
				8835766963654337957L,
				-1984334093384497740L,
				5616151800825184227L,
				-8442970605804311782L,
				-5396399970392474268L,
				2711274356126287353L,
				-5090439840321959043L,
				6638617029380445409L,
				-6424875729377006548L,
				-7243574969986334324L,
				-904268348341193502L,
				-6196811069886893217L,
				-7742123331454617135L,
				1449632469607275832L,
				3212140938119717436L,
				8676942774083692265L,
				-6625590425417154859L,
				8720904664575676760L,
				9151723732605931383L,
				7642401923610349184L,
				-3454390566366389884L,
				-232373658792783206L,
				-8933620623437682010L,
				2514068248201398743L,
				6757007617821370359L,
				-2870340646674679163L,
				416331333845426881L,
				-5319172016123138702L,
				3294412564645954555L,
				2812538484970453169L,
				-9128349093860081905L,
				6784456254618976198L,
				-2861881330654872638L,
				3912429093271518508L,
				-2562542119887175820L,
				4835616088583228965L,
				427639171891209425L,
				2590582080178010045L,
				-6288067880951692635L,
				-3204510905067065501L,
				9008426291442999873L,
				-4085962609397876083L,
				-3786041297813905157L,
				-6006475053574578261L,
				-6174022276199807178L,
				7958957647277035097L,
				2915785807118517755L,
				2139592530283433011L,
				-8562048562533248017L,
				-4991735207930685025L,
				393144860250454082L,
				-5852177196425420458L,
				-2652303154023739579L,
				2079679586901234739L,
				-1386526064824772584L,
				1574420554361329695L,
				-855542130447493508L,
				8291940350733154044L,
				-5330200233059892402L,
				5140782607921164290L,
				-977254437067235218L,
				-261520846651909307L,
				-7369143208070837455L,
				-4728766390712852111L,
				-8572213434879266955L,
				-6754813768712497692L,
				7946121307356573089L,
				504268959085012646L,
				-5536654029698676818L,
				-6021520522792328781L,
				6968613512520500871L,
				4029920623217569312L,
				2738878342460920492L,
				4562432005481165726L,
				-1279037845195368028L,
				1746645308450474697L,
				2538150989161378915L,
				2012299649948738944L,
				-3997559675475377347L,
				-5939431505669672858L,
				2077103722387383456L,
				-6188261335534632204L,
				8772504603740967633L,
				-1653698997940568281L,
				1676948989756529271L,
				2377579815165102226L,
				-2667481192445387240L,
				-5498860615033631762L,
				-2490865541169744469L,
				-1233441883399707566L,
				5445263795307566596L,
				2288458809413275798L,
				-5908274826918996877L,
				2909363406069168415L,
				2376032171261335687L,
				-5215189045919902574L,
				-6083327007632847329L,
				2462785604224107327L,
				-6684045035730714275L,
				2409356208468676804L,
				2814747114160772803L,
				-4529204412661254980L,
				-8437511853472556883L,
				1819323657613892915L,
				6862685309651627151L,
				-9210337863564319258L,
				-3641041551811494963L,
				-6791020794026796740L,
				-5261661996953805298L,
				-1953516254626596632L,
				-5901591005960707793L,
				-7413695905040596911L,
				2952256922297384020L,
				-8427771021447591769L,
				-6920139339436245233L,
				2967149838604559395L,
				-3253499104068010353L,
				-8473804925120692039L,
				-3561285603521886085L,
				-4453849179065102447L,
				2050092642498054323L,
				-5626434133619314199L,
				7995075368278704248L,
				7685996432951370136L,
				-8037783900933102779L,
				4601459625295412851L,
				-4491938778497306775L,
				-9089886217821142309L,
				-3947191644612298897L,
				1364225714229764884L,
				2580394324892542249L,
				-3765315378396862242L,
				6023794482194323576L,
				-662753714084561214L,
				3080495347149127717L,
				911710215008202776L,
				-803705685664586056L,
				-6101059689379533503L,
				-2122356322512227634L,
				8012110874513406695L,
				-4158551223425336367L,
				8282080141813519654L,
				4172879384244246799L,
				708522065347490110L,
				-6997269001146828181L,
				1887955086977822594L,
				8014460039616323415L
			};

			if(HashFunctions.xxhash(ByteBuffer.allocate(0)) != HASHES_OF_LOOPING_BYTES_WITHOUT_SEED[0]) {
				System.out.println("something went wrong!! incorrect xxhash computation");
				System.exit(1);
			}

			byte[] bytes = new byte[1024];
			for(int i=1; i<1024; i++){
				bytes[i] = (byte)i;
				if(HashFunctions.xxhash(ByteBuffer.allocate(i).wrap(bytes, 0, i)) != HASHES_OF_LOOPING_BYTES_WITHOUT_SEED[i]) {
					System.out.println("something went wrong!! incorrect xxhash computation");
					System.exit(1);
				}
			}
		}

		static public void test20(int trials){
			Random rnd = new Random();
			long input;
			for(int i=0; i<trials;i++){
				input = rnd.nextLong();
				ByteBuffer buffer = ByteBuffer.allocate(8);
				buffer.order(ByteOrder.LITTLE_ENDIAN);
				if(HashFunctions.xxhash(input) != HashFunctions.xxhash(buffer.putLong(input))){
					System.out.format("TEST 20, Trial %d: Hashes not equal", i);
					System.exit(1);
				}
			}	
		}

		static public void test21(int trials){
			Random rnd = new Random();

			int input_int;
			long input_long;
			String input_string;
			byte[] input_bytes = new byte[16];

			int bits_per_entry = 16;
			int num_entries_power = 27;
			QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

			for(int i=0; i<trials;i++){

				input_int = rnd.nextInt();
				if(!(qf.insert(input_int, false)) ||
						!(qf.search(input_int)) || !(qf.delete(input_int) != -1) ||
						(qf.search(input_int)))
				{
						System.out.format("TEST 21, Trial %d: Fail with input_int", i);
						System.exit(1);
				}

				input_long = rnd.nextLong();
				if(!(qf.insert(input_long, false)) ||
						!(qf.search(input_long)) || !(qf.delete(input_long) != -1) ||
						(qf.search(input_long)))
				{
						System.out.format("TEST 21, Trial %d: Fail with input_long", i);
						System.exit(1);
				}
				
				rnd.nextBytes(input_bytes);
				input_string = new String(input_bytes, StandardCharsets.UTF_8);
				if(!(qf.insert(input_string, false)) ||
					!(qf.search(input_string)) || !(qf.delete(input_string) != -1) ||
					(qf.search(input_string)))
				{
						System.out.format("TEST 21, Trial %d: Fail with input_string", i);
						System.exit(1);
				}
				
				rnd.nextBytes(input_bytes);
				if(!(qf.insert(input_bytes, false)) ||
					!(qf.search(input_bytes)) || !(qf.delete(input_bytes) != -1) ||
					(qf.search(input_bytes)))
				{
						System.out.format("TEST 21, Trial %d: Fail with input_bytes", i);
						System.exit(1);
				}
				
			}
		}
		
		static public void test_FPR(Filter f, double model_FPR, long insertions) {
			ExperimentsBase.baseline results = new ExperimentsBase.baseline();
			Experiment1.scalability_experiment( f,  0, insertions, results);
			double FPR = results.metrics.get("FPR").get(0);
			//System.out.println(FPR + ", " + model_FPR);
			if (FPR > model_FPR * 1.1) {
				System.out.println("FPR is greater than expected");
				System.exit(1);
			}
			else if (FPR < model_FPR / 2) {
				System.out.println("FPR is lower than expected");
				System.exit(1);
			}
		}
		
		
		// testing the false positive rate is as expected
		static public void test24() {
			int num_entries_power = 15;
			long num_entries = (long)(Math.pow(2, num_entries_power) * 0.9);
			for (int i = 5; i <= 16; i++) {
				int bits_per_entry = i;
				QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
				qf.expand_autonomously = false;
				double model_FPR = Math.pow(2, - bits_per_entry + 3);
				test_FPR(qf, model_FPR, num_entries);
			}
		}
		
		// testing the false positive rate is as expected
		static public void measure_cluster_length_distribution() {
			int num_entries_power = 20;
			long num_entries = (long)(Math.pow(2, num_entries_power) * 0.95);
			int bits_per_entry = 10;
			QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
			qf.expand_autonomously = false;
			double model_FPR = Math.pow(2, - bits_per_entry + 3);
			test_FPR(qf, model_FPR, num_entries);
			
			qf.pretty_print();
			
			Map<Integer,Integer> histogram = qf.compute_statistics();
			
			for (Map.Entry<Integer, Integer> set : histogram.entrySet()) {
			    System.out.println(set.getKey() + ", " + set.getValue());
			}
			
			System.out.println("cluster length  " + qf.avg_cluster_length);
			
		}
		
		// testing the false positive rate is as expected
		static public void test25() {
			int num_entries_power = 15;
			long num_entries = (long)(Math.pow(2, num_entries_power) * 0.9);
			for (int i = 5; i <= 16; i++) {
				int bits_per_entry = i;
				Filter qf = new CuckooFilter(num_entries_power, bits_per_entry);
				double model_FPR = Math.pow(2, - bits_per_entry + 3);
				test_FPR(qf, model_FPR, num_entries);
			}
		}
		
		// testing the false positive rate is as expected
		static public void test26() {
			int num_entries = (int)Math.pow(2, 15);
			for (int i = 5; i <= 16; i++) {
				int bits_per_entry = i;
				Filter qf = new BloomFilter(num_entries, bits_per_entry);
				double model_FPR = Math.pow(2, - bits_per_entry * Math.log(2));
				test_FPR(qf, model_FPR, num_entries);
			}
		}
		
		// this test ensures the basic infinifilter stops expanding after F expansions, where F is the original fingerprint size
		static public void test27() {
			int bits_per_entry = 10;
			int num_entries_power = 3;		
			BasicInfiniFilter qf = new BasicInfiniFilter(num_entries_power, bits_per_entry);
			qf.expand_autonomously = true;
			qf.fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
			int max_key = (int)Math.pow(2, num_entries_power + qf.fingerprintLength * 4 + 1 );
			for (int i = 0; i < max_key; i++) {
				//System.out.println(i);
				boolean success = qf.insert(i, false);
				if (!success) {
					break;
				}
			}
			if (qf.num_expansions > qf.original_fingerprint_size) {
				System.out.println("too many expansions took place for basic IF");
				System.exit(1);
			}
			int num_void = qf.get_num_void_entries();
			if (num_void == 0) {
				System.out.println("too few expansions took place for basic IF as there should be at least some void entries");
				System.exit(1);
			}

		}


}
		
		
		
		

