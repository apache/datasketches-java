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
import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * <p>This class provides methods to help estimate the correct parameters when
 * creating a Quotient filter, and methods to create the filter using those values.</p>
 *
 * <p>The underlying math is described in the
 *
 * Wikipedia article on Quotient filters</a>.</p>
 */
public final class QuotientFilterBuilder {

    /*
    This function is used to suggest the number of bits per entry for a given number of entries.
    The fingerprint length is related to the targetFalsePositiveProb roughly by 2^(-fingerprint_length).
    Hence, the length of the fingerprint can be stored in at most 8 bits.
    This, after rounding up, is the same as the more sophisticated expression which involves the capacity
    from https://en.wikipedia.org/wiki/Quotient_filter#Probability_of_false_positives.
    * @param targetFalsePositiveProb A desired false positive probability per item
    * @return The suggested fingerprint length in bits
     */
    public static byte suggestFingerprintLength(double targetFalsePositiveProb) {
        if (targetFalsePositiveProb <= 0. || targetFalsePositiveProb >= 1.) {

            throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
        }
        return (byte) Math.ceil(-Math.log(targetFalsePositiveProb) / Math.log(2));
    }

    /**
     * This method suggests the number of slots in the filter for a given input size, assuming 90% capacity.
     * There is no load factor checking internally within the filter, so this method is used to map between the
     * number of items we insert into a sketch and the number of slots we need to allocate.
     * A design feature of Niv's implementation is that 2^j +2*j slots are allocated. This asymptotically approaches
     * 2^j slots as j grows, and the canonical number of slots is 2^j. Therefore, we will only check against
     * 0.9*2^j slots.
     * The load factor is 0.9 to get some space-utility advantages over the bloom filter.
     */
    public static byte suggestLgNumSlots(long maxDistinctItems) {
        if (maxDistinctItems <= 0) {
            throw new SketchesArgumentException("maxDistinctItems must be strictly positive");
        }
        byte result = (byte) Math.ceil(Math.log(maxDistinctItems / 0.9) / Math.log(2));
        if (result < 31) {
            return result;
        } else {
            // Largest address space for a Java array is 2^31 - 1
            throw new SketchesArgumentException("Largest address space for a Java array is 2^31 - 1");
        }
    }

    /*
    Returns the largest number of unique items that can be inserted into the filter.
    We use a predefined load factor of 0.9 compared to the number of slots as 2^j.
    @param lgNumSlots The log-base-2 of the number of slots in the filter
    @return The maximum number of items that can be inserted into the filter
     */
    public static long suggestMaxNumItemsFromNumSlots(byte lgNumSlots) {
        if (lgNumSlots <= 0) {
            throw new SketchesArgumentException("lgNumSlots must be at least 1.");
        } else if (lgNumSlots >= 31) {
            throw new SketchesArgumentException("lgNumSlots cannot exceed 2^31 - 1.");
        }
        return (long) Math.floor(0.9 * Math.pow(2, lgNumSlots));
    }


    /**
     * This method suggests the parameters for a Quotient filter based on the maximum number of distinct items and the target false positive probability.
     * It first validates the inputs, then calculates the log-base-2 of the number of slots and the fingerprint length.
     * The results are returned as a QFPair object.
     *
     * @param maxDistinctItems The maximum number of distinct items that can be inserted into the filter.
     * @param targetFalsePositiveProb The desired false positive probability per item.
     * @return A QFPair object containing the suggested number of slots (lgNumSlots) and the suggested fingerprint length.
     * @throws SketchesArgumentException if the input parameters are not valid.
     */
    public static QFPair suggestParamsFromMaxDistinctsFPP(long maxDistinctItems, double targetFalsePositiveProb) {
        validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
        byte lgNumSlots = suggestLgNumSlots(maxDistinctItems);
        byte fingerprintLength = suggestFingerprintLength(targetFalsePositiveProb);
        return new QFPair(lgNumSlots, fingerprintLength);
    }

    private static void validateAccuracyInputs(final long maxDistinctItems, final double targetFalsePositiveProb) {
        if (maxDistinctItems <= 0) {
            throw new SketchesArgumentException("maxDistinctItems must be strictly positive");
        }
        if (targetFalsePositiveProb <= 0.0 || targetFalsePositiveProb > 1.0) {
            throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
        }
    }

    /**
     * Helper class to return a pair of parameters for a Quotient filter:
     * the log-base-2 of the number of slots (lgNumSlots) and the fingerprint length.
     * These parameters are used to configure the Quotient filter.
     */
    public static class QFPair {
        public final byte lgNumSlots;
        public final byte fingerprintLength;

        public QFPair(byte lgNumSlots, byte fingerprintLength) {
            this.lgNumSlots = lgNumSlots;
            this.fingerprintLength = fingerprintLength;
        }
    }

}
