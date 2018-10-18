/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public final class SingleItemSketch extends CompactSketch {
  private static final long FLAGS =
      (READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK  | ORDERED_FLAG_MASK) & 0xFFL;
  private static final long LO6BYTES = (FLAGS << 40) | (3L << 16) | (3L << 8) | 1L;
  private static final long DEFAULT_SEED_HASH = computeSeedHash(DEFAULT_UPDATE_SEED) & 0xFFFFL;
  private static final long DEFAULT_PRE0 =  (DEFAULT_SEED_HASH << 48) | LO6BYTES;

  private final long[] arr = new long[2];

  private SingleItemSketch(final long hash) {
    arr[0] = DEFAULT_PRE0;
    arr[1] = hash;
  }

  private SingleItemSketch(final long hash, final long seed) {
    final long seedHash = computeSeedHash(seed) & 0xFFFFL;
    arr[0] = (seedHash << 48) | LO6BYTES;
    arr[1] = hash;
  }

  SingleItemSketch(final long hash, final short seedHash) {
    final long seedH = seedHash & 0xFFFFL;
    arr[0] = (seedH << 48) | LO6BYTES;
    arr[1] = hash;
  }

  /**
   * Creates a SingleItemSketch on the heap given a Memory and assumes the DEFAULT_UPDATE_SEED.
   * @param mem the Memory to be heapified.  It must be a least 16 bytes.
   * @return a SingleItemSketch
   */
  public static SingleItemSketch heapify(final Memory mem) {
    final long memPre0 = mem.getLong(0);
    checkDefaultBytes0to7(memPre0);
    return new SingleItemSketch(mem.getLong(8));
  }

  /**
   * Creates a SingleItemSketch on the heap given a Memory.
   * Checks the seed hash of the given Memory against a hash of the given seed.
   * @param mem the Memory to be heapified
   * @param seed a given hash seed
   * @return a SingleItemSketch
   */
  public static SingleItemSketch heapify(final Memory mem, final long seed) {
    final long memPre0 = mem.getLong(0);
    checkDefaultBytes0to5(memPre0);
    final short seedHashIn = mem.getShort(6);
    final short seedHashCk = computeSeedHash(seed);
    checkSeedHashes(seedHashIn, seedHashCk);
    return new SingleItemSketch(mem.getLong(8), seed);
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

  static void checkDefaultBytes0to7(final long memPre0) {
    if (memPre0 != DEFAULT_PRE0) {
      throw new SketchesArgumentException(
        "Input Memory does not match defualt Preamble bytes 0 through 7.");
    }
  }

  static void checkDefaultBytes0to5(final long memPre0) {
    final long mask = (1L << 48) - 1L;
    if ((memPre0 & mask) != LO6BYTES) {
      throw new SketchesArgumentException(
        "Input Memory does not match defualt Preamble bytes 0 through 5.");
    }
  }

}
