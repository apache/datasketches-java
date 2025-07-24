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

package org.apache.datasketches.count;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.hash.MurmurHash3;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import java.util.Random;

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;


public class CountMinSketch {
  private final byte numHashes_;
  private final int numBuckets_;
  private final long seed_;
  private final long[] hashSeeds_;
  private final long[] sketchArray_;
  private long totalWeight_;

  // Thread-local MemorySegment to avoid allocations in hot paths with explicit endianness control
  private static final ThreadLocal<MemorySegment> LONG_SEGMENT =
          ThreadLocal.withInitial(() -> MemorySegment.ofArray(new byte[8]));

  private enum Flag {
    IS_EMPTY;

    int mask() {
      return 1 << ordinal();
    }
  }

  /**
   * Creates a CountMin sketch with given number of hash functions and buckets,
   * and a user-specified seed.
   *
   * @param numHashes The number of hash functions to apply to items
   * @param numBuckets Array size for each of the hashing function
   * @param seed The base hash seed
   */
  CountMinSketch(final byte numHashes, final int numBuckets, final long seed) {
    // Validate numHashes
    if (numHashes <= 0) {
      throw new SketchesArgumentException("Number of hash functions must be positive, got: " + numHashes);
    }

    // Validate numBuckets with clear mathematical justification
    if (numBuckets <= 0) {
      throw new SketchesArgumentException("Number of buckets must be positive, got: " + numBuckets);
    }
    if (numBuckets < 3) {
      throw new SketchesArgumentException("Number of buckets must be at least 3 to ensure relative error ≤ 1.0. " +
          "With " + numBuckets + " buckets, relative error would be " + String.format("%.3f", Math.exp(1.0) / numBuckets));
    }

    // Check for potential overflow in array size calculation
    // Use long arithmetic to detect overflow before casting
    final long totalSize = (long) numHashes * (long) numBuckets;
    if (totalSize > Integer.MAX_VALUE) {
      throw new SketchesArgumentException("Sketch array size would overflow: " + numHashes + " * " + numBuckets +
          " = " + totalSize + " > " + Integer.MAX_VALUE);
    }

    // This check is to ensure later compatibility with a Java implementation whose maximum size can only
    // be 2^31-1.  We check only against 2^30 for simplicity.
    if (totalSize >= (1L << 30)) {
      throw new SketchesArgumentException("Sketch would require excessive memory: " + numHashes + " * " + numBuckets +
          " = " + totalSize + " elements (~" + String.format("%.1f", totalSize * 8.0 / (1024 * 1024 * 1024)) + " GB). " +
          "Consider reducing numHashes or numBuckets.");
    }

    numHashes_ = numHashes;
    numBuckets_ = numBuckets;
    seed_ = seed;
    hashSeeds_ = new long[numHashes];
    sketchArray_ = new long[(int) totalSize];
    totalWeight_ = 0;

    Random rand = new Random(seed);
    for (int i = 0; i < numHashes; i++) {
      hashSeeds_[i] = rand.nextLong();
    }
  }

  /**
   * Efficiently converts a long to byte array using thread-local MemorySegment with explicit endianness.
   */
  private static byte[] longToBytes(final long value) {
    final MemorySegment segment = LONG_SEGMENT.get();
    segment.set(JAVA_LONG_UNALIGNED, 0, value);
    return segment.toArray(JAVA_BYTE);
  }


  private long[] getHashes(byte[] item) {
    long[] updateLocations = new long[numHashes_];

    for (int i = 0; i < numHashes_; i++) {
      long[] index = MurmurHash3.hash(item, hashSeeds_[i]);
      updateLocations[i] = i * (long)numBuckets_ + Math.floorMod(index[0], numBuckets_);
    }

    return updateLocations;
  }

  /**
   * Checks if the CountMinSketch has processed any items.
   * @return True if the sketch is empty, otherwise false.
   */
  public boolean isEmpty() {
    return totalWeight_ == 0;
  }

  /**
   * Returns the number of hash functions used in this sketch.
   * @return The number of hash functions.
   */
  public byte getNumHashes_() {
    return numHashes_;
  }

  /**
   * Returns the number of buckets per hash function.
   * @return The number of buckets.
   */
  public int getNumBuckets_() {
    return numBuckets_;
  }

  /**
   * Returns the hash seed used by this sketch.
   * @return The seed value.
   */
  public long getSeed_() {
    return seed_;
  }

  /**
   * Returns the total weight of all items inserted into the sketch.
   * @return The total weight.
   */
  public long getTotalWeight_() {
    return totalWeight_;
  }

  /**
   * Returns the relative error of the sketch.
   * @return The relative error.
   */
  public double getRelativeError() {
    return Math.exp(1.0) / (double)numBuckets_;
  }

  /**
   * Suggests an appropriate number of hash functions to use for a given confidence level.
   * @param confidence The desired confidence level between 0 and 1.
   * @return Suggested number of hash functions.
   */
  public static byte suggestNumHashes(double confidence) {
    if (confidence < 0 || confidence > 1) {
      throw new SketchesException("Confidence must be between 0 and 1.0 (inclusive).");
    }
    int value = (int) Math.ceil(Math.log(1.0 / (1.0 - confidence)));
    return (byte) Math.min(value, 127);
  }

  /**
   * Suggests an appropriate number of buckets per hash function for a given relative error.
   * @param relativeError The desired relative error.
   * @return Suggested number of buckets.
   */
  public static int suggestNumBuckets(double relativeError) {
    if (relativeError < 0.) {
      throw new SketchesException("Relative error must be at least 0.");
    }
    return (int) Math.ceil(Math.exp(1.0) / relativeError);
  }

  /**
   * Updates the sketch with the provided item and weight.
   * @param item The item to update.
   * @param weight The weight of the item.
   */
  public void update(final long item, final long weight) {
    update(longToBytes(item), weight);
  }

  /**
   * Updates the sketch with the provided item and weight.
   * @param item The item to update.
   * @param weight The weight of the item.
   */
  public void update(final String item, final long weight) {
    if (item == null || item.isEmpty()) {
      return;
    }
    final byte[] strByte = item.getBytes(StandardCharsets.UTF_8);
    update(strByte, weight);
  }

  /**
   * Updates the sketch with the provided item and weight.
   * @param item The item to update.
   * @param weight The weight of the item.
   */
  public void update(final byte[] item, final long weight) {
    if (item.length == 0) {
      return;
    }

    totalWeight_ += weight > 0 ? weight : -weight;
    long[] hashLocations = getHashes(item);
    for (long h : hashLocations) {
      sketchArray_[(int) h] += weight;
    }
  }

  /**
   * Returns the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Estimated frequency.
   */
  public long getEstimate(final long item) {
    return getEstimate(longToBytes(item));
  }

  /**
   * Returns the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Estimated frequency.
   */
  public long getEstimate(final String item) {
    if (item == null || item.isEmpty()) {
      return 0;
    }

    final byte[] strByte = item.getBytes(StandardCharsets.UTF_8);
    return getEstimate(strByte);
  }

  /**
   * Returns the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Estimated frequency.
   */
  public long getEstimate(final byte[] item) {
    if (item.length == 0) {
      return 0;
    }

    long[] hashLocations = getHashes(item);
    long res = sketchArray_[(int) hashLocations[0]];
    // Start from index 1 to avoid processing first element twice
    for (int i = 1; i < hashLocations.length; i++) {
      res = Math.min(res, sketchArray_[(int) hashLocations[i]]);
    }

    return res;
  }

  /**
   * Returns the upper bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Upper bound of estimated frequency.
   */
  public long getUpperBound(final long item) {
    return getUpperBound(longToBytes(item));
  }

  /**
   * Returns the upper bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Upper bound of estimated frequency.
   */
  public long getUpperBound(final String item) {
    if (item == null || item.isEmpty()) {
      return 0;
    }

    byte[] strByte = item.getBytes(StandardCharsets.UTF_8);
    return  getUpperBound(strByte);
  }

  /**
   * Returns the upper bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Upper bound of estimated frequency.
   */
  public long getUpperBound(final byte[] item) {
    if (item.length == 0) {
      return 0;
    }

    return getEstimate(item) + (long)(getRelativeError() * getTotalWeight_());
  }

  /**
   * Returns the lower bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Lower bound of estimated frequency.
   */
  public long getLowerBound(final long item) {
    return getLowerBound(longToBytes(item));
  }

  /**
   * Returns the lower bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Lower bound of estimated frequency.
   */
  public long getLowerBound(final String item) {
    if (item == null || item.isEmpty()) {
      return 0;
    }

    byte[] strByte = item.getBytes(StandardCharsets.UTF_8);
    return getLowerBound(strByte);
  }

  /**
   * Returns the lower bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Lower bound of estimated frequency.
   */
  public long getLowerBound(final byte[] item) {
    return getEstimate(item);
  }

  /**
   * Merges another CountMinSketch into this one. The sketches must have the same configuration.
   * @param other The other sketch to merge.
   */
  public void merge(final CountMinSketch other) {
    if (this == other) {
      throw new SketchesException("Cannot merge a sketch with itself");
    }

    boolean acceptableConfig = getNumBuckets_() == other.getNumBuckets_() &&
        getNumHashes_() == other.getNumHashes_() && getSeed_() == other.getSeed_();

    if (!acceptableConfig) {
      throw new SketchesException("Incompatible sketch configuration.");
    }

    for (int i = 0; i < sketchArray_.length; i++) {
      sketchArray_[i] += other.sketchArray_[i];
    }

    totalWeight_ += other.getTotalWeight_();
  }

  /**
   * Returns the serialized size in bytes.
   */
  private int getSerializedSizeBytes() {
    final int preambleBytes = Family.COUNTMIN.getMinPreLongs() * Long.BYTES;
    if (isEmpty()) {
      return preambleBytes;
    }
    return preambleBytes + Long.BYTES + (sketchArray_.length * Long.BYTES);
  }


  /**
   * Returns the sketch as a byte array.
   */
  public byte[] toByteArray() {
    final int serializedSizeBytes = getSerializedSizeBytes();
    final MemorySegment wseg = MemorySegment.ofArray(new byte[serializedSizeBytes]);

    long offset = 0;

    // Long 0
    final int preambleLongs = Family.COUNTMIN.getMinPreLongs();
    wseg.set(JAVA_BYTE, offset++, (byte) preambleLongs);
    final int serialVersion = 1;
    wseg.set(JAVA_BYTE, offset++, (byte) serialVersion);
    final int familyId = Family.COUNTMIN.getID();
    wseg.set(JAVA_BYTE, offset++, (byte) familyId);
    final int flagsByte = isEmpty() ? Flag.IS_EMPTY.mask() : 0;
    wseg.set(JAVA_BYTE, offset++, (byte) flagsByte);
    final int NULL_32 = 0;
    wseg.set(JAVA_INT_UNALIGNED, offset, NULL_32);
    offset += 4;

    // Long 1
    wseg.set(JAVA_INT_UNALIGNED, offset, numBuckets_);
    offset += 4;
    wseg.set(JAVA_BYTE, offset++, numHashes_);
    short hashSeed = Util.computeSeedHash(seed_);
    wseg.set(JAVA_SHORT_UNALIGNED, offset, hashSeed);
    offset += 2;
    final byte NULL_8 = 0;
    wseg.set(JAVA_BYTE, offset++, NULL_8);

    if (isEmpty()) {
      return wseg.toArray(JAVA_BYTE);
    }

    wseg.set(JAVA_LONG_UNALIGNED, offset, totalWeight_);
    offset += 8;

    for (long w: sketchArray_) {
      wseg.set(JAVA_LONG_UNALIGNED, offset, w);
      offset += 8;
    }

    return wseg.toArray(JAVA_BYTE);
  }

  /**
   * Deserializes a CountMinSketch from the provided byte array.
   * @param b The byte array containing the serialized sketch.
   * @param seed The seed used during serialization.
   * @return The deserialized CountMinSketch.
   */
  public static CountMinSketch deserialize(final byte[] b, final long seed) {
    final MemorySegment buf = MemorySegment.ofArray(b);
    long offset = 0;

    final byte preambleLongs = buf.get(JAVA_BYTE, offset++);
    final byte serialVersion = buf.get(JAVA_BYTE, offset++);
    final byte familyId = buf.get(JAVA_BYTE, offset++);
    final byte flagsByte = buf.get(JAVA_BYTE, offset++);
    final int NULL_32 = buf.get(JAVA_INT_UNALIGNED, offset);
    offset += 4;

    final int numBuckets = buf.get(JAVA_INT_UNALIGNED, offset);
    offset += 4;
    final byte numHashes = buf.get(JAVA_BYTE, offset++);
    final short seedHash = buf.get(JAVA_SHORT_UNALIGNED, offset);
    offset += 2;
    final byte NULL_8 = buf.get(JAVA_BYTE, offset++);

    if (seedHash != Util.computeSeedHash(seed)) {
      throw new SketchesArgumentException("Incompatible seed hashes: " + String.valueOf(seedHash) + ", "
          + String.valueOf(Util.computeSeedHash(seed)));
    }

    CountMinSketch cms = new CountMinSketch(numHashes, numBuckets, seed);
    final boolean empty = (flagsByte & Flag.IS_EMPTY.mask()) > 0;
    if (empty) {
      return cms;
    }
    long w = buf.get(JAVA_LONG_UNALIGNED, offset);
    offset += 8;
    cms.totalWeight_ = w;

    for (int i = 0; i < cms.sketchArray_.length; i++) {
      cms.sketchArray_[i] = buf.get(JAVA_LONG_UNALIGNED, offset);
      offset += 8;
    }

    return cms;
  }
}
