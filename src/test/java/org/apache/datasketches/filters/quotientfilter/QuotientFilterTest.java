package org.apache.datasketches.filters.quotientfilter;
import org.testng.annotations.Test;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;


public class QuotientFilterTest {
    /*
     * This test is based on the example from https://en.wikipedia.org/wiki/Quotient_filter
     *  in "Algorithm Description" section.
     * It performs the same insertions and query as the example and verifies that it gets the same results.
     * The insertion keys are: b, e, f, c, d, a which are hashed into slots as:
     * (b,1), (e,4), (f, 7), (c,1), (d,2), (a,1)
     */
    @Test
    static public void WikiInsertionTest() {
        int bits_per_entry = 8; // 8 bits per entry => 5 bits fingerprints, resolved internally in the filter.
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        // this test does not need different fingerprints as it is testing the slot locations and metadata bits.
        long fingerprint0 = 0;
        long fingerprint1 = (1 << bits_per_entry) - 1;

        /*
        The expected sketch is
        0         	000 00000
        1         	100 00000
        2         	111 00000
        3         	011 00000
        4         	101 00000
        5         	001 11111
        6         	000 00000
        7         	100 00000
         */
        qf.insert(fingerprint0, 1, false);
        qf.insert(fingerprint1, 4, false); // 11111 is inserted at slot 45 but pushed to slot 5
        qf.insert(fingerprint0, 7, false);
        qf.insert(fingerprint0, 1, false);
        qf.insert(fingerprint0, 2, false);
        qf.insert(fingerprint0, 1, false);
        assertEquals(qf.num_existing_entries, 6);



        // these are the expected resulting is_occupied, is_continuation, and is_shifted bits
        // for all slots contiguously. We do not store the fingerprints here
        BitSet result = new BitSet(num_entries * bits_per_entry);
        result = set_slot_in_test(result, bits_per_entry, 0, false, false, false, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 1, true, false, false, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 2, true, true, true, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 3, false, true, true, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 4, true, false, true, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 5, false, false, true, fingerprint1);
        result = set_slot_in_test(result, bits_per_entry, 6, false, false, false, fingerprint0);
        result = set_slot_in_test(result, bits_per_entry, 7, true, false, false, fingerprint0);
        assertTrue(check_equality(qf, result, true));
    }

    /*
     * This test is based on the Figure 2. from https://vldb.org/pvldb/vol5/p1627_michaelabender_vldb2012.pdf.
     * It performs the same insertions as in Figure 2 and checks for the same result.
     */
    @Test
    static public void PaperInsertionTest() {
        int bits_per_entry = 8;
        int num_entries_power = 4;
        int num_entries = (int)Math.pow(2, num_entries_power);
        QuotientFilter qf = new QuotientFilter(4, 8);

        // (key, slot): {(a, 1), (b,1), (c ,3), (d, 3), (e, 3), (f, 4), (g, 6), (h, 6)}
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
        assertTrue(check_equality(qf, result, false));
    }

    // test we don't get any false negatives for quotient filter
    @Test
    static public void FalseNegativeTest() {
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
    static public void OverflowTest() {
        int bits_per_entry = 8;
        int num_entries_power = 3;
        int num_entries = (int)Math.pow(2, num_entries_power);
        int fingerprint_size = bits_per_entry - 3;
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        long fp2 = 1 << fingerprint_size - 1;
        qf.insert(fp2, num_entries - 1, false);
        qf.insert(fp2, num_entries - 1, false);
        qf.delete(fp2, num_entries - 1);
        boolean found = qf.search(fp2, num_entries - 1);
        assertTrue(found);
    }

    /**
     * This method tests the functionality of the QuotientFilter and Iterator classes. It creates a QuotientFilter and inserts
     * six entries into it. An Iterator is then used to traverse the entries in the QuotientFilter. The method checks if the
     * bucket index of each visited entry matches the expected bucket index. If there's a mismatch, an error message is printed
     * and the program exits, indicating a test failure.
     */
    @Test
    static public void testQuotientFilterInsertionAndIteration() {

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
        while (it.next()) {assertEquals(arr[arr_index++], it.bucket_index);}
    }

    @Test
    static public void testQuotientFilterIterator() {

        int bits_per_entry = 8;
        int num_entries_power = 4;
        QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);

        qf.insert(0, 1, false);
        qf.insert(0, 4, false);
        qf.insert(0, 7, false);
        qf.insert(0, 1, false);
        qf.insert(0, 2, false);
        qf.insert(0, 1, false);
        qf.insert(0, 15, false);

        Iterator it = new Iterator(qf);
        int[] arr = new int[] {1, 1, 1, 2, 4, 7, 15};
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
                    return false;
                }
            }
        }
        return true;
    }

    /*
    Helper functino to test that no false negatives are returned.
     */
    static public boolean test_no_false_negatives(QuotientFilter filter, int num_entries) {
        HashSet<Integer> added = new HashSet<Integer>();
        int seed = 5;
        Random rand = new Random(seed);

        for (int i = 0; i < num_entries; i++) {
            int rand_num = rand.nextInt();
            boolean success = filter.insert(rand_num, false);
            if (success) {
                added.add(rand_num);
            }
            else {
                System.out.println("insertion failed");
            }
        }

        for (Integer i : added) {
            boolean found = filter.search((long)i);
            if (!found) {
                return false ;
            }
        }
        return true;
    }

}
