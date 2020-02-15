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
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.checkSeedHashes;
import static org.apache.datasketches.Util.computeSeedHash;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static org.apache.datasketches.theta.PreambleUtil.SINGLEITEM_FLAG_MASK;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;

/**
 * @author Lee Rhodes
 */
public final class SingleItemSketch extends CompactSketch {
  private static final long DEFAULT_SEED_HASH = computeSeedHash(DEFAULT_UPDATE_SEED) & 0xFFFFL;
  private static final String LS = System.getProperty("line.separator");

  //For backward compatibility, a candidate pre0 long must have Flags= compact, read-only, ordered,
  //  empty=0; COMPACT-Family=3, SerVer=3, PreLongs=1, plus the relevant seedHash.
  private static final long PRE0_LO6 =  0X00_00_1A_00_00_03_03_01L; //no SI flag, requires not empty
  private static final long PRE0_MASK = 0XFF_FF_DF_FF_FF_FF_FF_FFL; //removes SI flag

  private final long[] arr = new long[2];

  //use to test a candidate pre0 given a seed
  static boolean testPre0Seed(final long candidate, final long seed) {
    final long seedHash = computeSeedHash(seed) & 0xFFFFL;
    return testPre0SeedHash(candidate, seedHash);
  }

  //use to test a candidate pre0 given a seedHash
  static boolean testPre0SeedHash(final long candidate, final long seedHash) {
    final long test1 = (seedHash << 48) | PRE0_LO6; //no SI bit
    final long test2 = test1 | ((long)SINGLEITEM_FLAG_MASK << 40); //adds the SI bit
    final long mask = PRE0_MASK; //ignores the SI flag
    final long masked = candidate & mask;
    return (masked == test1) || (candidate == test2);
  }

  //Internal Constructor. All checking has been done, assumes default seed
  private SingleItemSketch(final long hash) {
    arr[0] = (DEFAULT_SEED_HASH << 48) | PRE0_LO6 | ((long)SINGLEITEM_FLAG_MASK << 40);
    arr[1] = hash;
  }

  //Internal Constructor.All checking has been done, given the relevant seed
  SingleItemSketch(final long hash, final long seed) {
    final long seedHash = computeSeedHash(seed) & 0xFFFFL;
    arr[0] = (seedHash << 48) | PRE0_LO6 | ((long)SINGLEITEM_FLAG_MASK << 40);
    arr[1] = hash;
  }

  //All checking has been done, given the relevant seedHash
  SingleItemSketch(final long hash, final short seedHash) {
    final long seedH = seedHash & 0xFFFFL;
    arr[0] = (seedH << 48) | PRE0_LO6 | ((long)SINGLEITEM_FLAG_MASK << 40);
    arr[1] = hash;
  }

  /**
   * Creates a SingleItemSketch on the heap given a SingleItemSketch Memory image and assumes the
   * DEFAULT_UPDATE_SEED.
   * @param srcMem the Memory to be heapified.  It must be a least 16 bytes.
   * @return a SingleItemSketch
   */
  public static SingleItemSketch heapify(final Memory srcMem) {
    return heapify(srcMem, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Creates a SingleItemSketch on the heap given a SingleItemSketch Memory image and a seed.
   * Checks the seed hash of the given Memory against a hash of the given seed.
   * @param srcMem the Memory to be heapified
   * @param seed a given hash seed
   * @return a SingleItemSketch
   */
  public static SingleItemSketch heapify(final Memory srcMem, final long seed) {
    final long memPre0 = srcMem.getLong(0);
    final short seedHashMem = srcMem.getShort(6);
    final short seedHashCk = computeSeedHash(seed);
    checkSeedHashes(seedHashMem, seedHashCk);
    if (testPre0SeedHash(memPre0, seedHashCk)) {
      return new SingleItemSketch(srcMem.getLong(8), seedHashCk);
    }
    final long def = (((long)seedHashCk << 48) | PRE0_LO6);
    throw new SketchesArgumentException("Input Memory does not match required Preamble. " + LS
        + "Memory    Pre0 : " + Long.toHexString(memPre0) + LS
        + "Should be Pre0 : " + Long.toHexString(def));
  }

  //Create methods using the default seed

  /**
   * Create this sketch with a long.
   *
   * @param datum The given long datum.
   * @return a SingleItemSketch
   */
  public static SingleItemSketch create(final long datum) {
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
  public static SingleItemSketch create(final double datum) {
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
  public static SingleItemSketch create(final String datum) {
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
  public static SingleItemSketch create(final byte[] data) {
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
  public static SingleItemSketch create(final char[] data) {
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
  public static SingleItemSketch create(final int[] data) {
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
  public static SingleItemSketch create(final long[] data) {
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
  public static SingleItemSketch create(final long datum, final long seed) {
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
  public static SingleItemSketch create(final double datum, final long seed) {
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
  public static SingleItemSketch create(final String datum, final long seed) {
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
  public static SingleItemSketch create(final byte[] data, final long seed) {
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
  public static SingleItemSketch create(final char[] data, final long seed) {
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
  public static SingleItemSketch create(final int[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  /**
   * Create this sketch with the given long array and a seed.
   * If the long array is null or empty no create attempt is made and the method returns null.
   *
   * @param data The given long array.
   * @param seed used to hash the given value.
   * @return a SingleItemSketch or null
   */
  public static SingleItemSketch create(final long[] data, final long seed) {
    if ((data == null) || (data.length == 0)) { return null; }
    return new SingleItemSketch(hash(data, seed)[0] >>> 1, seed);
  }

  //Sketch

  @Override
  public int getCountLessThanTheta(final double theta) {
    return (arr[1] < (theta * MAX_THETA_LONG_AS_DOUBLE)) ? 1 : 0;
  }

  @Override
  public int getCurrentBytes(final boolean compact) {
    return 16;
  }

  @Override
  public double getEstimate() {
    return 1.0;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(new long[] { arr[1] }, 1, Long.MAX_VALUE);
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
    for (int i = 0; i < 8; i++) {
      out[i]     = (byte) (arr[0] >>> (i * 8));
      out[i + 8] = (byte) (arr[1] >>> (i * 8));
    }
    return out;
  }

  //restricted methods

  @Override
  long[] getCache() {
    return new long[] { arr[1] };
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    return 1;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return (short) (arr[0] >>> 48);
  }

}
