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

package org.apache.datasketches.theta;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.theta.PreambleUtil.SINGLEITEM_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.checkMemorySeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * A CompactSketch that holds only one item hash.
 *
 * @author Lee Rhodes
 */
final class SingleItemSketch extends CompactSketch {
  private static final long DEFAULT_SEED_HASH = computeSeedHash(DEFAULT_UPDATE_SEED) & 0xFFFFL;

  // For backward compatibility, a candidate pre0_ long must have:
  // Flags (byte 5): Ordered, Compact, NOT Empty, Read Only, LittleEndian = 11010 = 0x1A.
  // Flags mask will be 0x1F.
  // SingleItem flag may not be set due to a historical bug, so we can't depend on it for now.
  // However, if the above flags are correct, preLongs == 1, SerVer >= 3, FamilyID == 3,
  // and the hash seed matches, it is virtually guaranteed that we have a SingleItem Sketch.

  private static final long PRE0_LO6_SI   = 0X00_00_3A_00_00_03_03_01L; //with SI flag
  private long pre0_ = 0;
  private long hash_ = 0;

  //Internal Constructor. All checking & hashing has been done, assumes default seed
  private SingleItemSketch(final long hash) {
    pre0_ = (DEFAULT_SEED_HASH << 48) | PRE0_LO6_SI;
    hash_ = hash;
  }

  //All checking & hashing has been done, given the relevant seed
  SingleItemSketch(final long hash, final long seed) {
    final long seedHash = computeSeedHash(seed) & 0xFFFFL;
    pre0_ = (seedHash << 48) | PRE0_LO6_SI;
    hash_ = hash;
  }

  //All checking & hashing has been done, given the relevant seedHash
  SingleItemSketch(final long hash, final short seedHash) {
    final long seedH = seedHash & 0xFFFFL;
    pre0_ = (seedH << 48) | PRE0_LO6_SI;
    hash_ = hash;
  }

  /**
   * Creates a SingleItemSketch on the heap given a SingleItemSketch Memory image and assumes the
   * DEFAULT_UPDATE_SEED.
   * @param srcMem the Memory to be heapified.  It must be a least 16 bytes.
   * @return a SingleItemSketch
   */ //does not override Sketch
  public static SingleItemSketch heapify(final Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Creates a SingleItemSketch on the heap given a SingleItemSketch Memory image and a seed.
   * Checks the seed hash of the given Memory against a hash of the given seed.
   * @param srcMem the Memory to be heapified.
   * @param seed a given hash seed
   * @return a SingleItemSketch
   */ //does not override Sketch
  public static SingleItemSketch heapify(final Memory srcMem, final long seed) {
    final short seedHashMem = checkMemorySeedHash(srcMem, seed);
    final boolean singleItem = otherCheckForSingleItem(srcMem);
    if (singleItem) { return new SingleItemSketch(srcMem.getLong(8), seedHashMem); }
    throw new SketchesArgumentException("Input Memory is not a SingleItemSketch.");
  }

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    if (dstMem == null) { return this; }
    else {
      dstMem.putLong(0, pre0_);
      dstMem.putLong(8, hash_);
      return new DirectCompactSketch(dstMem);
    }
  }

  //Create methods using the default seed

  /**
   * Create this sketch with a long.
   *
   * @param datum The given long datum.
   * @return a SingleItemSketch
   */
  static SingleItemSketch create(final long datum) {
    final long[] data = { datum };
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given double (or float) datum.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   * @return a SingleItemSketch
   */
  static SingleItemSketch create(final double datum) {
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given String.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no create attempt is made and the method returns null.
   *
   * <p>Note: this will not produce the same hash values as the {@link #create(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param datum The given String.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final String datum) {
    if ((datum == null) || datum.isEmpty()) { return null; }
    final byte[] data = datum.getBytes(UTF_8);
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given byte array.
   * If the byte array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given byte array.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final byte[] data) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given char array.
   * If the char array is null or empty no create attempt is made and the method returns null.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #create(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final char[] data) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given integer array.
   * If the integer array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given int array.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final int[] data) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  /**
   * Create this sketch with the given long array.
   * If the long array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given long array.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final long[] data) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, DEFAULT_UPDATE_SEED)[0] >>> 1);
  }

  //Updates with a user specified seed

  /**
   * Create this sketch with a long and a seed.
   *
   * @param datum The given long datum.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch
   */
  static SingleItemSketch create(final long datum, final long seed) {
    final long[] data = { datum };
    return new SingleItemSketch(hash(data, seed)[0] >>> 1);
  }

  /**
   * Create this sketch with the given double (or float) datum and a seed.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch
   */
  static SingleItemSketch create(final double datum, final long seed) {
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given String and a seed.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no create attempt is made and the method returns null.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #create(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param datum The given String.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final String datum, final long seed) {
    if ((datum == null) || datum.isEmpty()) { return null; }
    final byte[] data = datum.getBytes(UTF_8);
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given byte array and a seed.
   * If the byte array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given byte array.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final byte[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given char array and a seed.
   * If the char array is null or empty no create attempt is made and the method returns null.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #create(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final char[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given integer array and a seed.
   * If the integer array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given int array.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final int[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given long array (as an item) and a seed.
   * If the long array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given long array.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  static SingleItemSketch create(final long[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  //Sketch

  @Override //much faster
  public int getCountLessThanThetaLong(final long thetaLong) {
    return (hash_ < thetaLong) ? 1 : 0;
  }

  @Override
  public int getCurrentBytes() {
    return 16;
  }

  @Override
  public double getEstimate() {
    return 1.0;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(new long[] { hash_ }, 1, Long.MAX_VALUE);
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    return 1.0;
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return 1;
  }

  @Override
  public long getThetaLong() {
    return Long.MAX_VALUE;
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return 1.0;
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

  @Override
  public byte[] toByteArray() {
    final byte[] out = new byte[16];
    putLongLE(out, 0, pre0_);
    putLongLE(out, 8, hash_);
    return out;
  }

  //restricted methods

  @Override
  long[] getCache() {
    return new long[] { hash_ };
  }

  @Override
  int getCompactPreambleLongs() {
    return 1;
  }

  @Override
  int getCurrentPreambleLongs() {
    return 1;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return (short) (pre0_ >>> 48);
  }

  static final boolean otherCheckForSingleItem(final Memory mem) {
    return otherCheckForSingleItem(extractPreLongs(mem), extractSerVer(mem),
        extractFamilyID(mem), extractFlags(mem) );
  }

  static final boolean otherCheckForSingleItem(final int preLongs, final int serVer,
      final int famId, final int flags) {
    // Flags byte: SI=X, Ordered=T, Compact=T, Empty=F, ReadOnly=T, BigEndian=F = X11010 = 0x1A.
    // Flags mask will be 0x1F.
    // SingleItem flag may not be set due to a historical bug, so we can't depend on it for now.
    // However, if the above flags are correct, preLongs == 1, SerVer >= 3, FamilyID == 3,
    // and the hash seed matches (not done here), it is virtually guaranteed that we have a
    // SingleItem Sketch.
    final boolean numPreLongs = preLongs == 1;
    final boolean numSerVer = serVer >= 3;
    final boolean numFamId = famId == Family.COMPACT.getID();
    final boolean numFlags =  (flags & 0x1F) == 0x1A; //no SI, yet
    final boolean singleFlag = (flags & SINGLEITEM_FLAG_MASK) > 0;
    return (numPreLongs && numSerVer && numFamId && numFlags) || singleFlag;
  }

}
