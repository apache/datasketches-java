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

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This class provides methods to help estimate the correct parameters when
 * creating a Bloom filter, and methods to create the filter using those values.
 *
 * <p>The underlying math is described in the
 * <a href='https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions'>
 * Wikipedia article on Bloom filters</a>.</p>
 */
public final class BloomFilterBuilder {

  /**
   * Returns the optimal number of hash functions to given target numbers of distinct items
   * and the BloomFilter size in bits. This function will provide a result even if the input
   * values exceed the capacity of a single BloomFilter.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param numFilterBits The intended size of the Bloom Filter in bits
   * @return The suggested number of hash functions to use with the filter
   */
  public static short suggestNumHashes(final long maxDistinctItems, final long numFilterBits) {
    if ((maxDistinctItems < 1) || (numFilterBits < 1)) {
      throw new SketchesArgumentException("maxDistinctItems and numFilterBits must be strictly positive");
    }
    // ceil to ensure we never average worse than the target
    return (short) Math.max(1, (int) Math.ceil(((double) numFilterBits / maxDistinctItems) * Math.log(2.0)));
  }

  /**
   * Returns the optimal number of hash functions to achieve a target false positive probability.
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return The suggested number of hash functions to use with the filter.
   */
  public static short suggestNumHashes(final double targetFalsePositiveProb) {
    if ((targetFalsePositiveProb <= 0.0) || (targetFalsePositiveProb > 1.0)) {
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
    validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
    return (long) Math.ceil((-maxDistinctItems * Math.log(targetFalsePositiveProb)) / (Math.log(2) * Math.log(2)));
  }

  /**
   * Returns the minimum MemorySegment size, in bytes, needed for a serialized BloomFilter with an optimal number of bits
   * and hash functions for the given inputs. This is also the minimum size of a MemorySegment for
   * in-place filter initialization.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return The size, in bytes, required to hold the specified BloomFilter when serialized
   */
  public static long getSerializedFilterSizeByAccuracy(final long maxDistinctItems, final double targetFalsePositiveProb) {
    validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
    return BloomFilter.getSerializedSize(suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb));
  }

  /**
   * Returns the minimum MemorySegment size, in bytes, needed for a serialized BloomFilter with the given number of bits.
   * This is also the minimum size of a MemorySegment for in-place filter initialization.
   * @param numBits The number of bits in the target BloomFilter's bit array.
   * @return The size, in bytes, required to hold the specified BloomFilter when serialized
   */
  public static long getSerializedFilterSize(final long numBits) {
    validateSizeInputs(numBits, 1); // dummy numHashes value
    return BloomFilter.getSerializedSize(numBits);
  }

  /**
   * Creates a new BloomFilter with an optimal number of bits and hash functions for the given inputs,
   * using a random base seed for the hash function.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createByAccuracy(final long maxDistinctItems, final double targetFalsePositiveProb) {
    validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
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
    validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
    final long numBits = suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
    final short numHashes = suggestNumHashes(maxDistinctItems, numBits);
    return new BloomFilter(numBits, numHashes, seed);
  }

  /**
   * Creates a BloomFilter with given number of bits and number of hash functions,
   * using a rnadom base seed for the hash function.
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
   * using the provided base seed for the hash function.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed A base hash seed
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter createBySize(final long numBits, final int numHashes, final long seed) {
    validateSizeInputs(numBits, numHashes);
    return new BloomFilter(numBits, numHashes, seed);
  }

  /**
   * Creates a new BloomFilter with an optimal number of bits and hash functions for the given inputs,
   * using a random base seed for the hash function and writing into the provided MemorySegment.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @param dstSeg A MemorySegment to hold the initialized filter
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter initializeByAccuracy(
      final long maxDistinctItems, final double targetFalsePositiveProb, final MemorySegment dstSeg) {
    return initializeByAccuracy(maxDistinctItems, targetFalsePositiveProb, ThreadLocalRandom.current().nextLong(),
        dstSeg);
  }

  /**
   * Creates a new BloomFilter with an optimal number of bits and hash functions for the given inputs,
   * using the provided base seed for the hash function and writing into the provided MemorySegment.
   * @param maxDistinctItems The maximum expected number of distinct items to add to the filter
   * @param targetFalsePositiveProb A desired false positive probability per item
   * @param seed A base hash seed
   * @param dstSeg A MemorySegment to hold the initialized filter
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter initializeByAccuracy(
      final long maxDistinctItems, final double targetFalsePositiveProb, final long seed, final MemorySegment dstSeg) {
    validateAccuracyInputs(maxDistinctItems, targetFalsePositiveProb);
    final long numBits = suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
    final short numHashes = suggestNumHashes(maxDistinctItems, numBits);

    if (dstSeg.byteSize() < BloomFilter.getSerializedSize(numBits)) {
      throw new SketchesArgumentException("Provided MemorySegment is insufficient to hold requested filter");
    }

    return new BloomFilter(numBits, numHashes, seed, dstSeg);
  }

  /**
   * Initializes a BloomFilter with given number of bits and number of hash functions,
   * using a random base seed for the hash function and writing into the provided MemorySegment.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param dstSeg A MemorySegment to hold the initialized filter
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter initializeBySize(final long numBits, final int numHashes, final MemorySegment dstSeg) {
    return initializeBySize(numBits, numHashes, ThreadLocalRandom.current().nextLong(), dstSeg);
  }

  /**
   * Initializes a BloomFilter with given number of bits and number of hash functions,
   * using the provided base seed for the hash function and writing into the provided MemorySegment.
   *
   * @param numBits The size of the BloomFilter, in bits
   * @param numHashes The number of hash functions to apply to items
   * @param seed A base hash seed
   * @param dstSeg A MemorySegment to hold the initialized filter
   * @return A new BloomFilter configured for the given input parameters
   */
  public static BloomFilter initializeBySize(final long numBits, final int numHashes, final long seed, final MemorySegment dstSeg) {
    validateSizeInputs(numBits, numHashes);

    if (dstSeg.byteSize() < BloomFilter.getSerializedSize(numBits)) {
      throw new SketchesArgumentException("Provided MemorySegment is insufficient to hold requested filter");
    }

    return new BloomFilter(numBits, numHashes, seed, dstSeg);
  }

  private static void validateAccuracyInputs(final long maxDistinctItems, final double targetFalsePositiveProb) {
    if (maxDistinctItems <= 0) {
      throw new SketchesArgumentException("maxDistinctItems must be strictly positive");
    }
    if ((targetFalsePositiveProb <= 0.0) || (targetFalsePositiveProb > 1.0)) {
      throw new SketchesArgumentException("targetFalsePositiveProb must be a valid probability and strictly greater than 0");
    }
  }

  private static void validateSizeInputs(final long numBits, final int numHashes) {
    if (numBits < 0) {
      throw new SketchesArgumentException("Size of BloomFilter must be strictly positive. "
              + "Requested: " + numBits);
    }
    if (numBits > BloomFilter.MAX_SIZE_BITS) {
      throw new SketchesArgumentException("Size of BloomFilter must be <= "
              + BloomFilter.MAX_SIZE_BITS + ". Requested: " + numBits);
    }
    if (numHashes < 1) {
      throw new SketchesArgumentException("Must specify a strictly positive number of hash functions. "
              + "Requested: " + numHashes);
    }
    if (numHashes > Short.MAX_VALUE) {
      throw new SketchesArgumentException("Number of hashes cannot exceed " + Short.MAX_VALUE
              + ". Requested: " + numHashes);
    }
  }
}
