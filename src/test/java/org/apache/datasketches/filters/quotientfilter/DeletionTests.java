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

import java.util.BitSet;

public class DeletionTests {

    /**
     * This test checks the functionality of deleting items from the QuotientFilter.
     * The test works by:
     * 1. Inserting multiple keys into a single slot to create an overflow.
     * 2. Removing these keys.
     * 3. Checking that the remaining keys have returned to their canonical slots.
     * The expected outcome is that after deletion, the remaining keys should be in their canonical slots.
     */
    @Test
    static public void BasicDeletions() {
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


        qf.delete(31, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);

        BitSet result = new BitSet(num_entries * bits_per_entry);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 2, true, false, false, fp2);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 4, true, false, false, fp3);
        assertTrue(QuotientFilterTest.check_equality(qf, result, true));
    }

    /**
     * This test checks the functionality of deleting items from the QuotientFilter.
     * The test works by:
     * 1. Inserting multiple keys into a single slot to create an overflow.
     * 2. Removing these keys.
     * 3. Checking that the remaining keys have returned to their canonical slots.
     * The expected outcome is that after deletion, the remaining keys should be in their canonical slots.
     */
    @Test
    static public void DeletionsWithSameFingerprint() {
        int bits_per_entry = 8;
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);


        // All keys have the same fingerprint but are mapped into (mostly) different slots
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


        qf.delete(0, 2);
        qf.delete(0, 3);

        BitSet result = new BitSet(num_entries * bits_per_entry);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 3, true, false, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 4, false, false, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 5, false, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 6, true, false, false, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 7, true, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 8, false, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 9, false, false, true, 0);

        assertTrue(QuotientFilterTest.check_equality(qf, result, true));
    }

    @Test
    /**
     * This is a test for deleting items from the QuotientFilter even when an overflow is caused
     * by multiple insertions.
     * The test works by:
     * 1. Inserting multiple keys into a single slot to create an overflow.
     * 2. Removing these keys.
     * 3. Checking that the remaining keys have returned to their canonical slots.
     *
     * The expected outcome is that after deletion, the remaining keys should be in their canonical slots.
     */
    static public void DeletionsWithOverflow() {
        int bits_per_entry = 8;
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

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
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 1, true, false, false, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 2, true, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 3, false, false, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 4, true, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 5, true, false, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 6, false, true, true, 0);
        result = QuotientFilterTest.set_slot_in_test(result, bits_per_entry, 7, false, false, true, 0);
        assertTrue(QuotientFilterTest.check_equality(qf, result, true));
    }
}
