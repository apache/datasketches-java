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
        int fingerprint_len_bits = 5;
        int num_entries_power = 3;
        int num_entries = 1 << num_entries_power;
        QuotientFilter qf = new QuotientFilter(num_entries_power, fingerprint_len_bits);

        long fp1 = 1 << 4;
        long fp2 = 1 << 3;
        long fp3 = 1 << 2;
        long fp4 = 31;

        qf.insert(fp4, 1);
        qf.insert(fp1, 1);
        qf.insert(fp1, 1);
        qf.insert(fp2, 2);
        qf.insert(fp1, 1);
        qf.insert(fp1, 1);
        qf.insert(fp3, 4);


        qf.delete(31, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);
        qf.delete(fp1, 1);

        BitSet result = new BitSet(num_entries * qf.getNumBitsPerEntry());
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 2, true, false, false, fp2);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 4, true, false, false, fp3);
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
    static public void Deletions() {
        int fingerprint_len_bits = 5;
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(num_entries_power, fingerprint_len_bits);

        qf.insert(1, 1);
        qf.insert(2, 1);
        qf.insert(3, 2);
        qf.insert(4, 2);
        qf.insert(5, 3);
        qf.insert(6, 3);
        qf.insert(7, 3);
        qf.insert(8, 6);
        qf.insert(9, 6); // these are ignored
        qf.insert(10, 6);
        qf.insert(11, 7);

        qf.delete(3, 2);
        qf.delete(5, 3);

        BitSet result = new BitSet(num_entries * qf.getNumBitsPerEntry());
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 0, false, false, false, 0);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 1, true, false, false, 1);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 2, true, true, true, 2);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 3, true, false, true, 4);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 4, false, false, true, 6);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 5, false, true, true, 7);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 6, true, false, false, 8);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 7, false, false, false, 0);

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
    static public void DeletionsWithWrap() {
        int fingerprint_len_bits = 5;
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(num_entries_power, fingerprint_len_bits);

        qf.insert(1, 1);
        qf.insert(2, 1);
        qf.insert(3, 2);
        qf.insert(4, 2);
        qf.insert(5, 3);
        qf.insert(6, 4);
        qf.insert(7, 4);
        qf.insert(8, 5);

        //qf.pretty_print();
        qf.delete(5, 3);
        //qf.pretty_print();

        BitSet result = new BitSet(num_entries * qf.getNumBitsPerEntry());
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 0, false, false, false, 0);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 1, true, false, false, 1);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 2, true, true, true, 2);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 3, false, false, true, 3);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 4, true, true, true, 4);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 5, true, false, true, 6);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 6, false, true, true, 7);
        result = QuotientFilterTest.set_slot_in_test(result, qf.getNumBitsPerEntry(), 7, false, false, true, 8);
        assertTrue(QuotientFilterTest.check_equality(qf, result, true));
    }
}
