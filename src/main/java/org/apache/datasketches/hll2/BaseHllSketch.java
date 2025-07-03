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

package org.apache.datasketches.hll2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.hll2.HllUtil.HLL_HIP_RSE_FACTOR;
import static org.apache.datasketches.hll2.HllUtil.HLL_NON_HIP_RSE_FACTOR;
import static org.apache.datasketches.hll2.HllUtil.KEY_BITS_26;
import static org.apache.datasketches.hll2.HllUtil.KEY_MASK_26;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.Util;

/**
 * Although this class is package-private, it provides a single place to define and document
 * the common public API for both HllSketch and Union.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class BaseHllSketch implements MemorySegmentStatus {

  abstract void couponUpdate(int coupon);

  /**
   * Gets the size in bytes of the current sketch when serialized using
   * <i>toCompactByteArray()</i>.
   * @return the size in bytes of the current sketch when serialized using
   * <i>toCompactByteArray()</i>.
   */
  public abstract int getCompactSerializationBytes();

  /**
   * This is less accurate than the <i>getEstimate()</i> method and is automatically used
   * when the sketch has gone through union operations where the more accurate HIP estimator
   * cannot be used.
   * This is made public only for error characterization software that exists in separate
   * packages and is not intended for normal use.
   * @return the composite estimate
   */
  public abstract double getCompositeEstimate();

  /**
   * Returns the current mode of the sketch: LIST, SET, HLL
   * @return the current mode of the sketch: LIST, SET, HLL
   */
  abstract CurMode getCurMode();

  /**
   * Return the cardinality estimate
   * @return the cardinality estimate
   */
  public abstract double getEstimate();

  /**
   * Gets the {@link TgtHllType}
   * @return the TgtHllType enum value
   */
  public abstract TgtHllType getTgtHllType();

  /**
   * Gets the <i>lgConfigK</i>.
   * @return the <i>lgConfigK</i>.
   */
  public abstract int getLgConfigK();

  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations.
   *
   * @param numStdDev This must be an integer between 1 and 3, inclusive.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public abstract double getLowerBound(int numStdDev);

  /**
   * Returns the current serialization version.
   * @return the current serialization version.
   */
  public static final int getSerializationVersion() {
    return PreambleUtil.SER_VER;
  }

  /**
   * Returns the current serialization version of the given MemorySegment.
   * @param seg the given MemorySegment containing a serialized HllSketch image.
   * @return the current serialization version.
   */
  public static final int getSerializationVersion(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, PreambleUtil.SER_VER_BYTE) & 0XFF;
  }

  /**
   * Gets the current (approximate) Relative Error (RE) asymptotic values given several
   * parameters. This is used primarily for testing.
   * @param upperBound return the RE for the Upper Bound, otherwise for the Lower Bound.
   * @param oooFlag set true if the sketch is the result of a non qualifying union operation.
   * @param lgConfigK the configured value for the sketch.
   * @param numStdDev the given number of Standard Deviations. This must be an integer between
   * 1 and 3, inclusive.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">Number of Standard Deviations</a>
   * @return the current (approximate) RelativeError
   */
  public static double getRelErr(final boolean upperBound, final boolean oooFlag,
      final int lgConfigK, final int numStdDev) {
    HllUtil.checkLgK(lgConfigK);
    if (lgConfigK > 12) {
      final double rseFactor = oooFlag ? HLL_NON_HIP_RSE_FACTOR : HLL_HIP_RSE_FACTOR;
      final int configK = 1 << lgConfigK;
      return (numStdDev * rseFactor) / Math.sqrt(configK);
    }
    return Math.abs(RelativeErrorTables.getRelErr(upperBound, oooFlag, lgConfigK, numStdDev));
  }

  /**
   * Gets the size in bytes of the current sketch when serialized using
   * <i>toUpdatableByteArray()</i>.
   * @return the size in bytes of the current sketch when serialized using
   * <i>toUpdatableByteArray()</i>.
   */
  public abstract int getUpdatableSerializationBytes();

  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations.
   *
   * @param numStdDev This must be an integer between 1 and 3, inclusive.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public abstract double getUpperBound(int numStdDev);

  /**
   * Returns true if empty
   * @return true if empty
   */
  public abstract boolean isEmpty();

  /**
   * Returns true if the backing MemorySegment of this sketch is in compact form.
   * @return true if the backing MemorySegment of this sketch is in compact form.
   */
  public abstract boolean isCompact();

  /**
   * This HLL family of sketches and operators is always estimating, even for very small values.
   * @return true
   */
  public boolean isEstimationMode() {
    return true;
  }

  /**
   * Returns true if this sketch was created using MemorySegment.
   * @return true if this sketch was created using MemorySegment.
   */
  @Override
  public abstract boolean hasMemorySegment();

  /**
   * Returns true if the backing MemorySegment for this sketch is off-heap.
   * @return true if the backing MemorySegment for this sketch is off-heap.
   */
  @Override
  public abstract boolean isOffHeap();

  /**
   * Gets the Out-of-order flag.
   * @return true if the current estimator is the non-HIP estimator, which is slightly less
   * accurate than the HIP estimator.
   */
  abstract boolean isOutOfOrder();

  /**
   * Returns true if the given MemorySegment refers to the same underlying resource as this sketch.
   * The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   *
   * <p>This is only relevant for HLL_4 sketches that have been configured for off-heap
   * using MemorySegment.  For on-heap sketches or unions this will return false.
   *
   * <p>It is rare, but possible, the the off-heap MemorySegment that has been allocated to an HLL_4
   * sketch may not be large enough. If this should happen, the sketch makes a request for more
   * space from the owner of the resource and then moves itself to this new location. This all
   * happens transparently to the user. This method provides a means for the user to
   * inquire of the sketch if it has, in fact, moved itself.
   *
   * @param seg the given MemorySegment
   * @return true if the given MemorySegment refers to the same underlying resource as this sketch or
   * union.
   */
  @Override
  public abstract boolean isSameResource(MemorySegment seg);

  /**
   * Resets to empty, but does not change the configured values of lgConfigK and tgtHllType.
   */
  public abstract void reset();

  /**
   * Serializes this sketch as a byte array in compact form. The compact form is smaller in size
   * than the updatable form and read-only. It can be used in union operations as follows:
   * <pre>{@code
   *     Union union; HllSketch sk, sk2;
   *     int lgK = 12;
   *     sk = new HllSketch(lgK, TgtHllType.HLL_4); //can be 4, 6, or 8
   *     for (int i = 0; i < (2 << lgK); i++) { sk.update(i); }
   *     byte[] arr = HllSketch.toCompactByteArray();
   *     //...
   *     union = Union.heapify(arr); //initializes the union using data from the array.
   *     //OR, if used in an off-heap environment:
   *     union = Union.heapify(MemorySegment.ofArray(arr)); //same as above, except from MemorySegment object.
   *
   *     //To recover an updatable heap sketch:
   *     sk2 = HllSketch.heapify(arr);
   *     //OR, if used in an off-heap environment:
   *     sk2 = HllSketch.heapify(MemorySegment.ofArray(arr));
   * }</pre>
   *
   * <p>The sketch "wrapping" operation skips actual deserialization thus is quite fast. However,
   * any attempt to update the derived HllSketch will result in a Read-only exception.</p>
   *
   * <p>Note that in some cases, based on the state of the sketch, the compact form is
   * indistiguishable from the updatable form.  In these cases the updatable form is returned
   * and the compact flag bit will not be set.</p>
   * @return this sketch as a compact byte array.
   */
  public abstract byte[] toCompactByteArray();

  /**
   * Serializes this sketch as a byte array in an updatable form. The updatable form is larger than
   * the compact form. The use of this form is primarily in environments that support updating
   * sketches in off-heap MemorySegment. If the sketch is constructed using HLL_8, sketch updating and
   * union updating operations can actually occur in MemorySegment, which can be off-heap:
   * <pre>{@code
   *     Union union; HllSketch sk;
   *     int lgK = 12;
   *     sk = new HllSketch(lgK, TgtHllType.HLL_8) //must be 8
   *     for (int i = 0; i < (2 << lgK); i++) { sk.update(i); }
   *     byte[] arr = sk.toUpdatableByteArray();
   *     MemorySegment wseg = MemorySegment.wrap(arr);
   *     //...
   *     union = Union.writableWrap(wseg); //no deserialization!
   * }</pre>
   * @return this sketch as an updatable byte array.
   */
  public abstract byte[] toUpdatableByteArray();

  /**
   * Human readable summary as a string.
   * @return Human readable summary as a string.
   */
  @Override
  public String toString() {
    return toString(true, false, false, false);
  }

  /**
   * Human readable summary with optional detail. Does not list empty entries.
   * @param summary if true, output the sketch summary
   * @param detail if true, output the internal data array
   * @param auxDetail if true, output the internal Aux array, if it exists.
   * @return human readable string with optional detail.
   */
  public String toString(final boolean summary, final boolean detail, final boolean auxDetail) {
    return toString(summary, detail, auxDetail, false);
  }

  /**
   * Human readable summary with optional detail
   * @param summary if true, output the sketch summary
   * @param detail if true, output the internal data array
   * @param auxDetail if true, output the internal Aux array, if it exists.
   * @param all if true, outputs all entries including empty ones
   * @return human readable string with optional detail.
   */
  public abstract String toString(boolean summary, boolean detail, boolean auxDetail,
      boolean all);

  /**
   * Present the given long as a potential unique item.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
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
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN & +/- infinity forms
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given String as a potential unique item.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: About 2X faster performance can be obtained by first converting the String to a
   * char[] and updating the sketch with that. This bypasses the complexity of the Java UTF_8
   * encoding. This, of course, will not produce the same internal hash values as updating directly
   * with a String. So be consistent!  Unioning two sketches, one fed with strings and the other
   * fed with char[] will be meaningless.
   * </p>
   *
   * @param datum The given String.
   */
  public void update(final String datum) {
    if ((datum == null) || datum.isEmpty()) { return; }
    final byte[] data = datum.getBytes(UTF_8);
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given byte buffer as a potential unique item.
   * Bytes are read from the current position of the buffer until its limit.
   * If the byte buffer is null or has no bytes remaining, no update attempt is made and the method returns.
   *
   * <p>This method will not modify the position, mark, limit, or byte order of the buffer.</p>
   *
   * <p>Little-endian order is preferred, but not required. This method may perform better if the provided byte
   * buffer is in little-endian order.</p>
   *
   * @param data The given byte buffer.
   */
  public void update(final ByteBuffer data) {
    if ((data == null) || (data.remaining() == 0)) { return; }
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given byte array as a potential unique item.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   */
  public void update(final byte[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given char array as a potential unique item.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the <i>update(String)</i>
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   */
  public void update(final char[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given integer array as a potential unique item.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   */
  public void update(final int[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  /**
   * Present the given long array as a potential unique item.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   */
  public void update(final long[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    couponUpdate(coupon(hash(data, Util.DEFAULT_UPDATE_SEED)));
  }

  private static final int coupon(final long[] hash) {
    final int addr26 = (int) ((hash[0] & KEY_MASK_26));
    final int lz = Long.numberOfLeadingZeros(hash[1]);
    final int value = ((lz > 62 ? 62 : lz) + 1);
    return (value << KEY_BITS_26) | addr26;
  }

}
