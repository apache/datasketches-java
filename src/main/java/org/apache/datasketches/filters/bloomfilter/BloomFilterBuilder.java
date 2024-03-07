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

package org.apache.datasketches.filters.bloomfilter;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * <p>This class provides methods to help estimate the correct paramters to use when
 * creating a Bloom filter, and methods to create the filter using those values.</p>
 *
 * <p>The underlying math is described in the
 * <a href='https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions'>
 * Wikipedia article on Bloom filters</a>.</p>
 */
public final class BloomFilterBuilder {

  /**
   * Returns the optimal number of hash functions to given target numbers of distinct items
   * and the BloomFliter size in bits.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param numFilterBits The intended size of the Bloom Filter in bits
   * @return The suggested number of hash functions to use with the filter
   */
  public static short suggestNumHashes(final long maxDistinctItems, final long numFilterBits) {
    if (maxDistinctItems < 1 || numFilterBits < 1) {
      throw new SketchesArgumentException("maxDistinctItems and numFilterBits must be strictly positive");
    }
    // ceil to ensure we never average worse than the target
    return (short) Math.max(1, (int) Math.ceil((double) numFilterBits / maxDistinctItems * Math.log(2.0)));
  }

  /**
   * Returns the optimal number of hash functions to achieve a target false positive probability.
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return The suggested number of hash functions to use with the filter.
   */
  public static short suggestNumHashes(final double targetFalsePositiveProb) {
    if (targetFalsePositiveProb <= 0.0 || targetFalsePositiveProb > 1.0) {
      throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
    }
    // ceil to ensure we never average worse than the target
    return (short) Math.ceil((- Math.log(targetFalsePositiveProb) / Math.log(2)));
  }

  /**
   * Returns the optimal number of bits to use in a Bloom Filter given a target number of distinct
   * items and a target false positive probability.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return The suggested number of bits to use with the filter
   */
  public static long suggestNumFilterBits(final long maxDistinctItems, final double targetFalsePositiveProb) {
    if (maxDistinctItems <= 0) {
      throw new SketchesArgumentException("maxDistinctItems must be strictly positive");
    }
    if (targetFalsePositiveProb <= 0.0 || targetFalsePositiveProb > 1.0) {
      throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
    }
    return Math.round(-maxDistinctItems * Math.log(targetFalsePositiveProb) / (Math.log(2) * Math.log(2)));
  }

  /**
   * Creates a new BloomFilter with an optimal number of bits and hash functions for the given inputs.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createByAccuracy(final long maxDistinctItems, final double targetFalsePositiveProb) {
    return createByAccuracy(maxDistinctItems, targetFalsePositiveProb, ThreadLocalRandom.current().nextLong());
  }

  /**
   * Creates a new BloomFilter with an optimal number of bits and hash functions for the given inputs,
   * using the provided base seed for the hash function.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @param seed A base hash seed
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createByAccuracy(final long maxDistinctItems, final double targetFalsePositiveProb, final long seed) {
    if (maxDistinctItems <= 0) {
      throw new SketchesArgumentException("maxDistinctItems must be strictly positive");
    }
    if (targetFalsePositiveProb <= 0.0 || targetFalsePositiveProb > 1.0) {
      throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
    }
    final long numBits = suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
    final short numHashes = suggestNumHashes(maxDistinctItems, numBits);
    return new BloomFilter(numBits, numHashes, seed);
  }

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a random seed.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createBySize(final long numBits, final int numHashes) {
    return createBySize(numBits, numHashes, ThreadLocalRandom.current().nextLong());
  }

   /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * and a random seed.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed A base hash seed
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createBySize(final long numBits, final int numHashes, final long seed) {
    if (numBits > BloomFilter.MAX_SIZE) {
      throw new SketchesArgumentException("Size of BloomFilter must be <= "
      + BloomFilter.MAX_SIZE + ". Requested: " + numBits);
    }
    if (numHashes < 1) {
      throw new SketchesArgumentException("Must specify a strictly positive number of hash functions. "
      + "Requested: " + numHashes);
    }
    if (numHashes > Short.MAX_VALUE) {
      throw new SketchesArgumentException("Number of hashes cannot exceed " + Short.MAX_VALUE
      + ". Requested: " + numHashes);
    }

    return new BloomFilter(numBits, numHashes, seed);
  }
}
