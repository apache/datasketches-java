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

public class BloomFilterBuilder {

  public static int suggestNumHashes(long maxDistinctItems, long numFilterBits) {
    return (int) Math.max(1, (int) Math.round((double) numFilterBits / maxDistinctItems * Math.log(2.0)));
  }

  public static int suggestNumHashes(double targetFalsePositiveProb) {
    // TODO: should this be Math.ceil of the value?
    return (int) (- Math.log(targetFalsePositiveProb) / Math.log(2));
  }

  public static long suggestNumFilterBits(long maxDistinctItems, double targetFalsePositiveProb) {
    return (long) Math.round(-maxDistinctItems * Math.log(targetFalsePositiveProb) / (Math.log(2) * Math.log(2)));
  }

  public static BloomFilter newBloomFilter(long maxDistinctItems, double targetFalsePositiveProb) {
    // TODO validate inputs
    final long numBits = suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
    final int numHashes = suggestNumHashes(maxDistinctItems, numBits);
    return new BloomFilter(numBits, numHashes);
  }

  public static BloomFilter newBloomFilter(long maxDistinctItems, double targetFalsePositiveProb, long seed) {
    // TODO validate inputs
    final long numBits = suggestNumFilterBits(maxDistinctItems, targetFalsePositiveProb);
    final int numHashes = suggestNumHashes(maxDistinctItems, numBits);
    return new BloomFilter(numBits, numHashes, seed);
  }
}
