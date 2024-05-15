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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

public class BitVectorTests {

    /**
     * This test method initializes a QuickBitVectorWrapper with various combinations of bits per entry and number of entries.
     * It then calculates the expected length of the bit vector and asserts that the actual size of the bit vector matches the expected length.
     *
     * Example Input-Output Pairs:
     * 1. Input: bitsPerEntry = 2, numEntries = 8 (1L << 3)
     *    Output: expectedLengthBits = 64
     *
     * 2. Input: bitsPerEntry = 3, numEntries = 16 (1L << 4)
     *    Output: 64
     *
     * 3. Input: bitsPerEntry = 33, numEntries = 8 (1L << 3)
     *    Output: expectedLengthBits = 320
     */
    @Test
    static public void testSize(){
        int[] bitsPerEntry = {2, 3, 4, 5, 6, 7, 8, 9, 10, 23, 24, 25, 31, 32, 33};
        long[] numEntries = {1L << 3, 1L<<4, 1L<<8, 1L << 16};
        long nBits ;
        long expectedLengthBits ;

        for (int i = 0; i < bitsPerEntry.length; i++){
            for (int j = 0; j < numEntries.length; j++) {
                QuickBitVectorWrapper bv = new QuickBitVectorWrapper(bitsPerEntry[i], numEntries[j]);
                nBits = bitsPerEntry[i] * numEntries[j];
                expectedLengthBits = 64 * ((nBits % 64 == 0) ? (nBits / 64) : (1 + nBits / 64));
                assertEquals(bv.size(), expectedLengthBits);
            }
        }
    }

 /*
 This test amends a few entries in the BitVector and checks that they are appropriately set.
  */
 @Test
 static public void testSettersAndGetters(){
     QuickBitVectorWrapper bv = new QuickBitVectorWrapper(6, 16);

     // All entries should be False before any updates
    for (int i = 0; i < bv.size(); i++){
        assertFalse(bv.get(i), "All entries should be False");
    }

    // Set some values
    bv.set(0, true);
    assertTrue(bv.get(0), "Value at index 0 should be True");

    bv.set(32, true) ;
    assertTrue(bv.get(32), "Value at index 32 should be True");

    bv.setFromTo(64, 128, ~0L);
    assertTrue(bv.getFromTo(64, 128) == -1L, "Values from 64 to 128 should be set to 1") ;
 }
}
