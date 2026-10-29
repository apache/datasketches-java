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

package org.apache.datasketches.frequencies;

import org.apache.datasketches.common.SketchesArgumentException;

final class Util {

  private Util() {}

  /**
   * The following constant controls the size of the initial data structure for the 
   * frequencies sketches and its value is somewhat arbitrary.
   */
  static final int LG_MIN_MAP_SIZE = 3;

  /**
   * This constant is large enough so that computing the median of SAMPLE_SIZE
   * randomly selected entries from a list of numbers and outputting
   * the empirical median will give a constant-factor approximation to the 
   * true median with high probability.
   */
  static final int SAMPLE_SIZE = 1024;

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of 
   * Austin Appleby's MurmurHash3 algorithm. It is also used by the Trove for Java libraries.
   */
  static long hash(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }

  /**
   * Checks the map sizes and the number of active items read from the serialized image of a
   * non-empty sketch, and returns the log2 size of the map to rebuild the sketch into.
   *
   * <p>The returned size is just large enough to hold the active items, and no larger than
   * <i>lgCurMapSize</i>. The map then grows as needed while the items are inserted, so the memory
   * allocated is bounded by the size of the image, not by the declared map size.</p>
   *
   * @param lgMaxMapSize log2 of the maximum map size
   * @param lgCurMapSize log2 of the current map size
   * @param activeItems the number of active items
   * @param loadFactor the load factor of the hash map
   * @return log2 of the map size to rebuild into
   * @throws SketchesArgumentException if the values are not consistent
   */
  static int checkMapSizes(final int lgMaxMapSize, final int lgCurMapSize, final int activeItems,
      final double loadFactor) {
    if (lgCurMapSize < LG_MIN_MAP_SIZE) {
      throw new SketchesArgumentException(
          "Possible Corruption: LgCurMapSize must be at least " + LG_MIN_MAP_SIZE + ": " + lgCurMapSize);
    }
    if (lgCurMapSize > lgMaxMapSize) {
      throw new SketchesArgumentException(
          "Possible Corruption: LgCurMapSize must not exceed LgMaxMapSize: "
            + lgCurMapSize + " > " + lgMaxMapSize);
    }
    final long maxActive = (long) ((1L << Math.min(lgCurMapSize, 62)) * loadFactor);
    if ((activeItems < 0) || (activeItems > maxActive)) {
      throw new SketchesArgumentException(
          "Possible Corruption: ActiveItems exceeds map capacity: " + activeItems);
    }
    int lgStart = LG_MIN_MAP_SIZE;
    while ((lgStart < lgCurMapSize) && (((1L << lgStart) * loadFactor) < activeItems)) { lgStart++; }
    return lgStart;
  }

  /**
   * Checks the stream weight read from the serialized image of a non-empty sketch.
   * @param streamWeight the stream weight
   * @throws SketchesArgumentException if the stream weight is not positive
   */
  static void checkStreamWeight(final long streamWeight) {
    if (streamWeight <= 0) {
      throw new SketchesArgumentException(
          "Possible Corruption: stream weight of a non-empty sketch must be positive: " + streamWeight);
    }
  }

}
