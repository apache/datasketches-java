/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.hll.HllUtil.KEY_BITS_26;
import static com.yahoo.sketches.hll.HllUtil.KEY_MASK_26;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class BaseHllSketch {


  /**
   * Returns the current mode of the sketch: LIST, SET, HLL
   * @return the current mode of the sketch: LIST, SET, HLL
   */
  abstract CurMode getCurMode();

  /**
   * Gets the size in bytes of the current sketch when serialized using <i>toByteArray()</i>.
   * @return the size in bytes of the current sketch when serialized using <i>toByteArray()</i>.
   */
  public abstract int getCurrentSerializationBytes();

  /**
   * Return the current cardinality estimate
   * @return the current cardinality estimate
   */
  public abstract double getEstimate();

  /**
   * Gets the <i>lgConfigK</i>. For the union operator, the returned value is the
   * current effective <i>lgK</i>, which may be less than <i>lgMaxK</i>.
   * @return the <i>lgConfigK</i>.
   */
  public abstract int getLgConfigK();

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public abstract double getUpperBound(double numStdDev);

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public abstract double getLowerBound(double numStdDev);

  /**
   * Return true if empty
   * @return true if empty
   */
  public abstract boolean isEmpty();

  /**
   * This HLL family of sketches is always estimating, even for very small values.
   * @return true
   */
  public boolean isEstimationMode() {
    return true;
  }

  /**
   * Gets the Out-of-order flag.
   * @return true if the current estimator is the non-HIP estimator, which is slightly less
   * accurate than the HIP estimator.
   */
  abstract boolean isOutOfOrderFlag();

  /**
   * Resets to empty, but does not change the configured values of lgK and tgtHllType.
   */
  public abstract void reset();

  /**
   * Gets the serialization of this sketch as a byte array in compact form, which is designed
   * to be read-only and is not directly updatable.
   * For the Union operator, this is the serialization of the internal state of
   * the union operator as a sketch.
   * @return the serialization of this sketch as a byte array.
   */
  public abstract byte[] toCompactByteArray();

  /**
   * Human readable summary as a string.
   * @return Human readable summary as a string.
   */
  @Override
  public abstract String toString();

  /**
   * Human readable summary with optional detail
   * @param detail if true, list out the internal arrays
   * @return human readable string
   */
  public abstract String toString(boolean detail);

  /**
   * Present the given long as a potential unique item.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given double (or float) datum as a potential unique item.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   */
  public void update(final double datum) {
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given String as a potential unique item.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower due to the complexity of the UTF8 encoding.
   * </p>
   *
   * @param datum The given String.
   */
  public void update(final String datum) {
    if ((datum == null) || datum.isEmpty()) { return; }
    final byte[] data = datum.getBytes(UTF_8);
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given byte array as a potential unique item.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   */
  public void update(final byte[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given char array as a potential unique item.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   */
  public void update(final char[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given integer array as a potential unique item.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   */
  public void update(final int[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given long array as a potential unique item.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   */
  public void update(final long[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, DEFAULT_UPDATE_SEED)));
  }

  private static final int coupon(final long[] hash) {
    final int addr26 = (int) ((hash[0] & KEY_MASK_26));
    final int lz = Long.numberOfLeadingZeros(hash[1]);
    final int value = ((lz > 62 ? 62 : lz) + 1);
    return (value << KEY_BITS_26) | addr26;
  }

  static final int getLow26(final int coupon) {
    return coupon & KEY_MASK_26;
  }

  static final int getValue(final int coupon) {
    return coupon >>> KEY_BITS_26;
  }

  static final String couponString(final int coupon) {
    return "Key: " + getLow26(coupon) + ", Value: " + getValue(coupon);
  }

  abstract void couponUpdate(int coupon);

}
