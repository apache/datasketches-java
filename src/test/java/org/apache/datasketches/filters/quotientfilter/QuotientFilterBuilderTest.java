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

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
public class QuotientFilterBuilderTest {

    @Test
    public void testSuggestFingerprintLengthFromFPP(){
        // invalid false positive rate
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestFingerprintLength(0.));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestFingerprintLength(1.));

        // manually computed values based on formula using ceil(log2(1/targetFalsePositiveProb))
        double[] fpps = {0.1, 0.01, 0.001, 0.0001, 1E-5, 1E-6, 1E-7, 1E-8};
        byte[] results = {4, 7, 10, 14, 17, 20, 24, 27, 30};
        for (int i = 0; i < fpps.length; i++) {
            assertEquals(QuotientFilterBuilder.suggestFingerprintLength(fpps[i]), results[i]);
        }
    }

    @Test
    public static void testSuggestLgNumSlots(){
        // invalid number of items
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(0,0.9f));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(-1, 0.9f));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(5000000000L, 0.9f));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(0));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(-1));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestLgNumSlots(5000000000L));

        long[] numItems = {1, 100, 1000, 1000000L};
        int[] results = {1, 7, 11, 21} ;

        for (int i = 0; i < numItems.length; i++) {
            long num = numItems[i];
            byte result = QuotientFilterBuilder.suggestLgNumSlots(num, 0.9f);
            assertEquals(result, results[i]);
            result = QuotientFilterBuilder.suggestLgNumSlots(num);
            assertEquals(result, results[i]);
        }
    }

    @Test
    public static void testSuggestMaxNumItems(){
        // invalid number of slots
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestMaxNumItemsFromNumSlots((byte)-127));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestMaxNumItemsFromNumSlots((byte)0));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestMaxNumItemsFromNumSlots((byte)32));

        int[] lgNumSlots = {1, 2, 3, 6, 10, 15, 25, 30,};
        long[] results_ninety_pc = {1, 3, 7, 57, 922, 29504, 30212096, 966787072} ;
        long[] results_eighty_pc = {1, 3, 6, 51, 820, 26240, 26869760, 859832320} ;

        // load capacities arbitrarily chosen using powers of two for exact arithmetic
        float ninety_pc_appx = 922f / 1024f; // ≈ 0.9
        float eighty_pc_appx = 820f / 1024f; // ≈ 0.8

        for (int i = 0; i < lgNumSlots.length; i++) {
            long result_ninety = QuotientFilterBuilder.suggestMaxNumItemsFromNumSlots(lgNumSlots[i], ninety_pc_appx);
            long result_eighty = QuotientFilterBuilder.suggestMaxNumItemsFromNumSlots(lgNumSlots[i], eighty_pc_appx);
            assertEquals(result_ninety, results_ninety_pc[i]);
            assertEquals(result_eighty, results_eighty_pc[i]);
        }
    }

    @Test
    public static void testSuggestParamsFromMaxDistinctsFPP(){
        // invalid number of slots
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(5000000000L, 0.0001));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(100000000, 0.));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(100000000, 1.5));
        assertThrows(SketchesArgumentException.class, () -> QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(5000000000L, -1.));

        byte lgNumSlots ;
        byte fingerprintLength ;
        long[] numItems = {1L, 900L, 500_000_000L} ;
        double[] fpp = {1E-10, 1E-2, 1e-7} ;

        // expected outcomes
        byte[] expected_lgNumSlotsNinety = {1, 10, 30} ;
        byte[] expected_lgNumSlotsEighty = {1, 11, 30} ;
        byte[] expected_fingerprintLength = {34, 7, 24} ;

        for (int i = 0; i < numItems.length; i++) {
            QuotientFilterBuilder.QFPair pair = QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(numItems[i], 0.9f, fpp[i]);
            lgNumSlots = pair.lgNumSlots;
            fingerprintLength = pair.fingerprintLength;
            assertEquals(expected_lgNumSlotsNinety[i], lgNumSlots);
            assertEquals(expected_fingerprintLength[i], fingerprintLength);

            // 80% load
            pair = QuotientFilterBuilder.suggestParamsFromMaxDistinctsFPP(numItems[i], fpp[i]);
            lgNumSlots = pair.lgNumSlots;
            fingerprintLength = pair.fingerprintLength;
            assertEquals(expected_lgNumSlotsEighty[i], lgNumSlots);
            assertEquals(expected_fingerprintLength[i], fingerprintLength);
        }
    }
}
