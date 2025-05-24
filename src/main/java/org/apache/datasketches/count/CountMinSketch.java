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
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.tuple.Util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;


public class CountMinSketch {
  private final byte numHashes_;
  private final int numBuckets_;
  private final long seed_;
  private final long[] hashSeeds_;
  private final long[] sketchArray_;
  private long totalWeight_;


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
    numHashes_ = numHashes;
    numBuckets_ = numBuckets;
    seed_ = seed;
    hashSeeds_ = new long[numHashes];
    sketchArray_ = new long[numHashes * numBuckets];
    totalWeight_ = 0;

    if (numBuckets < 3) {
      throw new SketchesException("Using fewer than 3 buckets incurs relative error greater than 1.");
    }

    // This check is to ensure later compatibility with a Java implementation whose maximum size can only
    // be 2^31-1.  We check only against 2^30 for simplicity.
    if (numBuckets * numHashes >= 1 << 30) {
      throw new SketchesException("These parameters generate a sketch that exceeds 2^30 elements. \n" +
          "Try reducing either the number of buckets or the number of hash functions.");
    }

    Random rand = new Random();
    for (int i = 0; i < numHashes; i++) {
      hashSeeds_[i] = rand.nextLong();
    }
  }

  private long[] getHashes(byte[] item) {
    long[] updateLocations = new long[numHashes_];

    for (int i = 0; i < numHashes_; i++) {
      long[] index = MurmurHash3.hash(item, hashSeeds_[i]);
      updateLocations[i] = i * (long)numBuckets_ + index[0] % numBuckets_;
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
  public byte suggestNumHashes(double confidence) {
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
  public int suggestNumBuckets(double relativeError) {
    return (int) Math.ceil(Math.exp(1.0) / relativeError);
  }

  /**
   * Updates the sketch with the provided item and weight.
   * @param item The item to update.
   * @param weight The weight of the item.
   */
  public void update(final long item, final long weight) {
    byte[] longByte = ByteBuffer.allocate(8).putLong(item).array();
    update(longByte, weight);
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
    byte[] longByte = ByteBuffer.allocate(8).putLong(item).array();
    return getEstimate(longByte);
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
    for (long h : hashLocations) {
      res = Math.min(res,  sketchArray_[(int) h]);
    }

    return res;
  }

  /**
   * Returns the upper bound of the estimated frequency for the given item.
   * @param item The item to estimate.
   * @return Upper bound of estimated frequency.
   */
  public long getUpperBound(final long item) {
    byte[] longByte = ByteBuffer.allocate(8).putLong(item).array();
    return getUpperBound(longByte);
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
    byte[] longByte = ByteBuffer.allocate(8).putLong(item).array();
    return getLowerBound(longByte);
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
   * Serializes the sketch into the provided ByteBuffer.
   * @param buf The ByteBuffer to write into.
   */
  public void serialize(ByteBuffer buf) {
    // Long 0
    final int preambleLongs = Family.COUNTMIN.getMinPreLongs();
    buf.put((byte) preambleLongs);
    final int serialVersion = 1;
    buf.put((byte) serialVersion);
    final int familyId = Family.COUNTMIN.getID();
    buf.put((byte) familyId);
    final int flagsByte = isEmpty() ? Flag.IS_EMPTY.mask() : 0;
    buf.put((byte)flagsByte);
    final int NULL_32 = 0;
    buf.putInt(NULL_32);

    // Long 1
    buf.putInt(numBuckets_);
    buf.putShort(numHashes_);
    buf.putShort(Util.computeSeedHash(seed_));
    final byte NULL_8 = 0;
    buf.put(NULL_8);
    if (isEmpty()) {
      return;
    }

    buf.putLong(totalWeight_);

    for (long estimate: sketchArray_) {
      buf.putLong(estimate);
    }
  }

  /**
   * Deserializes a CountMinSketch from the provided byte array.
   * @param b The byte array containing the serialized sketch.
   * @param seed The seed used during serialization.
   * @return The deserialized CountMinSketch.
   */
  public static CountMinSketch deserialize(final byte[] b, final long seed) {
    ByteBuffer buf = ByteBuffer.allocate(b.length);
    buf.put(b);

    final byte preambleLongs = buf.get();
    final byte serialVersion = buf.get();
    final byte familyId = buf.get();
    final byte flagsByte = buf.get();
    final int NULL_32 = buf.getInt();

    final int numBuckets = buf.getInt();
    final byte numHashes = buf.get();
    final short seedHash = buf.getShort();
    final byte NULL_8 = buf.get();

    if (seedHash != Util.computeSeedHash(seed)) {
      throw new SketchesException("Incompatible seed hashes: " + String.valueOf(seedHash) + ", "
          + String.valueOf(Util.computeSeedHash(seed)));
    }

    CountMinSketch cms = new CountMinSketch(numHashes, numBuckets, seed);
    final boolean empty = (flagsByte & Flag.IS_EMPTY.mask()) > 0;
    if (empty) {
      return cms;
    }

    int i = 0;
    while (buf.hasRemaining()) {
      cms.sketchArray_[i] = buf.getLong();
    }

    return cms;
  }
}
