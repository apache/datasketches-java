/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.filters.quotientfilter;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;


public class QuotientFilterTest {
    // this method had been in Bitmap, but was used only to test the QuotientFilter
    public static boolean get_fingerprint_bit(long index, long fingerprint) {
        long mask = 1 << index;
        long and = fingerprint & mask;
        return and != 0;
    }

    /*
     * This test is based on the example from https://en.wikipedia.org/wiki/Quotient_filter
     *  in "Algorithm Description" section.
     * It performs the same insertions and query as the example and verifies that it gets the same results.
     * The insertion keys are: b, e, f, c, d, a which are hashed into slots as:
     * (b,1), (e,4), (f, 7), (c,1), (d,2), (a,1)
     */
    @Test
    public void WikiInsertionTest() {
        int bits_per_entry = 6; // 6 bits per entry => 3 bits fingerprint, resolved internally in the filter.
        int num_entries_power = 3;
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        final int A = 1;
        final int B = 2;
        final int C = 3;
        final int D = 4;
        final int E = 5;
        final int F = 6;

        qf.insert(B, 1);
        qf.insert(E, 4);
        qf.insert(F, 7);
        qf.insert(C, 1);
        qf.insert(D, 2);
        qf.insert(A, 1);
        assertEquals(qf.get_num_entries(), 6);

        assertEquals(getState(qf, 0), 0);
        assertEquals(qf.get_fingerprint(0), 0);
        assertEquals(getState(qf, 1), 0b100);
        assertEquals(qf.get_fingerprint(1), A);
        assertEquals(getState(qf, 2), 0b111);
        assertEquals(qf.get_fingerprint(2), B);
        assertEquals(getState(qf, 3), 0b011);
        assertEquals(qf.get_fingerprint(3), C);
        assertEquals(getState(qf, 4), 0b101);
        assertEquals(qf.get_fingerprint(4), D);
        assertEquals(getState(qf, 5), 0b001);
        assertEquals(qf.get_fingerprint(5), E);
        assertEquals(getState(qf, 6), 0);
        assertEquals(qf.get_fingerprint(6), 0);
        assertEquals(getState(qf, 7), 0b100);
        assertEquals(qf.get_fingerprint(7), F);
    }

    public int getState(QuotientFilter filter, int slot) {
      return (filter.is_occupied(slot) ? 1 : 0) << 2
          | (filter.is_continuation(slot) ? 1 : 0) << 1
          | (filter.is_shifted(slot) ? 1 : 0);
    }

    /*
     * This test is based on the Figure 2. from https://vldb.org/pvldb/vol5/p1627_michaelabender_vldb2012.pdf.
     * It performs the same insertions as in Figure 2 and checks for the same result.
     */
    @Test
    public void PaperInsertionTest() {
        int bits_per_entry = 8;
        int num_entries_power = 4;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(4, 8);

        final int A = 1;
        final int B = 2;
        final int C = 3;
        final int D = 4;
        final int E = 5;
        final int F = 6;
        final int G = 7;
        final int H = 8;

        // (key, slot): {(a, 1), (b, 1), (c, 3), (d, 3), (e, 3), (f, 4), (g, 6), (h, 6)}
        qf.insert(A, 1);
        qf.insert(B, 1);
        qf.insert(C, 3);
        qf.insert(D, 3);
        qf.insert(E, 3);
        qf.insert(F, 4);
        qf.insert(G, 6);
        qf.insert(H, 6);

        BitSet result = new BitSet(num_entries * bits_per_entry);
        result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, 0);
        result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, A);
        result = set_slot_in_test(result, bits_per_entry, 2, false, true, true, B);
        result = set_slot_in_test(result, bits_per_entry, 3, true, false, false, C);
        result = set_slot_in_test(result, bits_per_entry, 4, true, true, true, D);
        result = set_slot_in_test(result, bits_per_entry, 5, false, true, true, E);
        result = set_slot_in_test(result, bits_per_entry, 6, true, false, true, F);
        result = set_slot_in_test(result, bits_per_entry, 7, false, false, true, G);
        result = set_slot_in_test(result, bits_per_entry, 8, false, true, true, H);
        assertTrue(check_equality(qf, result, false));
    }

    // test we don't get any false negatives for quotient filter
    @Test
    public void FalseNegativeTest() {
        int bits_per_entry = 10;
        int num_entries_power = 10;
        QuotientFilter filter = new QuotientFilter(num_entries_power, bits_per_entry);
        int num_entries = (int) (Math.pow(2, num_entries_power) * 0.9 );
        assertTrue(test_no_false_negatives(filter, num_entries));
    }


    /*
     * Adds two entries to the end of the filter, causing an overflow into the extension slots.
     * Checks this can be handled by the internal data structure and then deletes one of the keys from the filter.
     */
    @Test
    public void OverflowTest() {
        final int bits_per_entry = 8;
        final int num_entries_power = 3;
        final int num_entries = (int)Math.pow(2, num_entries_power);
        final int fingerprint_size = bits_per_entry - 3;
        final QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        final long fp1 = 1;
        final long fp2 = 1 << fingerprint_size - 1;
        qf.insert(fp1, num_entries - 1);
        assertEquals(qf.get_fingerprint(num_entries - 1), fp1);
        assertEquals(getState(qf, num_entries - 1), 0b100);
        qf.insert(fp2, num_entries - 1);
        assertEquals(qf.get_fingerprint(0), fp2);
        assertEquals(getState(qf, 0), 0b011);
        qf.delete(fp2, num_entries - 1);
        assertEquals(qf.get_fingerprint(0), 0);
        assertEquals(getState(qf, 0), 0b000);
        final boolean found = qf.search(fp1, num_entries - 1);
        assertTrue(found);
    }

    /**
     * This method tests the functionality of the QuotientFilter and Iterator classes. It creates a QuotientFilter and inserts
     * six entries into it. An Iterator is then used to traverse the entries in the QuotientFilter. The method checks if the
     * bucket index of each visited entry matches the expected bucket index. If there's a mismatch, an error message is printed
     * and the program exits, indicating a test failure.
     */
    @Test
    public void testQuotientFilterInsertionAndIteration() {

        int bits_per_entry = 8;
        int num_entries_power = 4;
        //int num_entries = (int)Math.pow(2, num_entries_power);
        //int fingerprint_size = bits_per_entry - 3;
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        qf.insert(0x1F, 2);
        qf.insert(0x1F, 3);
        qf.insert(0x1F, 3);
        qf.insert(0x1F, 4);
        qf.insert(0x1F, 15); // last slot in the filter
        qf.insert(0x1F, 16); // outside the bounds
        qf.pretty_print() ;

        Iterator it = new Iterator(qf);
        int[] arr = new int[] {2, 3, 4, 15};
        int arr_index = 0;
        while (it.next()) {assertEquals(it.bucket_index, arr[arr_index++]);}
    }

    @Test
    public void testQuotientFilterIterator() {

        int bits_per_entry = 8;
        int num_entries_power = 4;
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        qf.insert(0, 1);
        qf.insert(0, 4);
        qf.insert(0, 7);
        qf.insert(0, 1);
        qf.insert(0, 2);
        qf.insert(0, 1);
        qf.insert(0, 15);

        Iterator it = new Iterator(qf);
        int[] arr = new int[] {1, 2, 4, 7, 15};
        int arr_index = 0;
        while (it.next()) {assertEquals(arr[arr_index++], it.bucket_index);}
    }


    // Helper functions

    /**
     * This method sets the values of a slot in a BitSet based on the provided parameters.
     * The slot is defined by the number of bits per entry and the slot index.
     * The values to be set include whether the slot is occupied, whether it is a continuation of a previous entry,
     * whether it is shifted, and the fingerprint.
     *
     * @param result The BitSet where the slot values will be set.
     * @param bits_per_entry The number of bits per entry in the BitSet.
     * @param slot The index of the slot to be set.
     * @param is_occupied Whether the slot is occupied.
     * @param is_continuation Whether the slot is a continuation of a previous entry.
     * @param is_shifted Whether the slot is shifted.
     * @param fingerprint The fingerprint to be set in the slot.
     * @return The BitSet after setting the slot values.
     */
    static public BitSet set_slot_in_test(BitSet result, int bits_per_entry, int slot, boolean is_occupied, boolean is_continuation, boolean is_shifted, long fingerprint) {
        int index = bits_per_entry * slot;
        result.set(index++, is_occupied);
        result.set(index++, is_continuation);
        result.set(index++, is_shifted);
        for (int i = 0; i < bits_per_entry - 3; i++) {
            result.set(index++, get_fingerprint_bit(i, fingerprint) );
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
                    return false;
                }
            }
        }
        return true;
    }

    /*
    Helper function to test that no false negatives are returned.
     */
    static public boolean test_no_false_negatives(QuotientFilter filter, int num_entries) {
        HashSet<Integer> added = new HashSet<Integer>();
        int seed = 5;
        Random rand = new Random(seed);

        for (int i = 0; i < num_entries; i++) {
            int rand_num = rand.nextInt();
            boolean success = filter.insert(rand_num);
            if (success) {
                added.add(rand_num);
            }
            else {
                System.out.println("insertion failed");
            }
        }

        for (Integer i: added) {
            boolean found = filter.search((long)i);
            if (!found) {
                return false;
            }
        }
        return true;
    }
}
