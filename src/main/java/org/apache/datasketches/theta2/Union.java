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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * Compute the union of two or more theta sketches.
 * A new instance represents an empty set.
 *
 * @author Lee Rhodes
 */
public abstract class Union extends SetOperation {

  /**
   * Wrap a Union object around a Union MemorySegment object containing data.
   * This method assumes the <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * This does NO validity checking of the given MemorySegment.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg The source MemorySegment object.
   * @return this class
   */
  public static Union fastWrap(final MemorySegment srcSeg) {
    return fastWrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap a Union object around a Union MemorySegment object containing data.
   * This does NO validity checking of the given MemorySegment.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg The source MemorySegment object.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  public static Union fastWrap(final MemorySegment srcSeg, final long expectedSeed) {
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    return UnionImpl.fastWrapInstance(srcSeg, expectedSeed);
  }

  /**
   * Wrap a Union object around a Union MemorySegment object containing data.
   * This method assumes the <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg The source MemorySegment object.
   * @return this class
   */
  public static Union wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap a Union object around a Union MemorySegment object containing data.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg The source MemorySegment object.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  public static Union wrap(final MemorySegment srcSeg, final long expectedSeed) {
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    return UnionImpl.wrapInstance(srcSeg, expectedSeed);
  }

  /**
   * Returns the number of storage bytes required for this union in its current state.
   *
   * @return the number of storage bytes required for this union in its current state.
   */
  public abstract int getCurrentBytes();

  @Override
  public Family getFamily() {
    return Family.UNION;
  }

  /**
   * Returns the maximum required storage bytes for this union.
   * @return the maximum required storage bytes for this union.
   */
  public abstract int getMaxUnionBytes();

  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap.
   * This does not disturb the underlying data structure of the union.
   * Therefore, it is OK to continue updating the union after this operation.
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  public abstract CompactSketch getResult();

  /**
   * Gets the result of this operation as a CompactSketch of the chosen form.
   * This does not disturb the underlying data structure of the union.
   * Therefore, it is OK to continue updating the union after this operation.
   *
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstSeg destination MemorySegment
   *
   * @return the result of this operation as a CompactSketch of the chosen form
   */
  public abstract CompactSketch getResult(boolean dstOrdered, MemorySegment dstSeg);

  /**
   * Resets this Union. The seed remains intact, everything else reverts back to its virgin state.
   */
  public abstract void reset();

  /**
   * Returns a byte array image of this Union object
   * @return a byte array image of this Union object
   */
  public abstract byte[] toByteArray();

  /**
   * This implements a stateless, pair-wise union operation. The returned sketch will be cut back to
   * the smaller of the two k values if required.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param sketchA The first argument
   * @param sketchB The second argument
   * @return the result ordered CompactSketch on the heap.
   */
  public CompactSketch union(final Sketch sketchA, final Sketch sketchB) {
    return union(sketchA, sketchB, true, null);
  }

  /**
   * This implements a stateless, pair-wise union operation. The returned sketch will be cut back to
   * k if required, similar to the regular Union operation.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param sketchA The first argument
   * @param sketchB The second argument
   * @param dstOrdered If true, the returned CompactSketch will be ordered.
   * @param dstSeg If not null, the returned CompactSketch will be placed in this MemorySegment.
   * @return the result CompactSketch.
   */
  public abstract CompactSketch union(Sketch sketchA, Sketch sketchB, boolean dstOrdered,
      MemorySegment dstSeg);

  /**
   * Perform a Union operation with <i>this</i> union and the given on-heap sketch of the Theta Family.
   * This method is not valid for the older SetSketch, which was prior to Open Source (August, 2015).
   *
   * <p>This method can be repeatedly called.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param sketchIn The incoming sketch.
   */
  public abstract void union(Sketch sketchIn);

  /**
   * Perform a Union operation with <i>this</i> union and the given MemorySegment image of any sketch of the
   * Theta Family. The input image may be from earlier versions of the Theta Compact Sketch,
   * called the SetSketch (circa 2014), which was prior to Open Source and are compact and ordered.
   *
   * <p>This method can be repeatedly called.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param seg MemorySegment image of sketch to be merged
   */
  public abstract void union(MemorySegment seg);

  /**
   * Update <i>this</i> union with the given long data item.
   *
   * @param datum The given long datum.
   */
  public abstract void update(long datum);

  /**
   * Update <i>this</i> union with the given double (or float) data item.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * Each of the special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   */
  public abstract void update(double datum);

  /**
   * Update <i>this</i> union with the with the given String data item.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given string as a data item.</p>
   *
   * @param datum The given String.
   */
  public abstract void update(String datum);

  /**
   * Update <i>this</i> union with the given byte array item.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given byte array as a data
   * item.</p>
   *
   * @param data The given byte array.
   */
  public abstract void update(byte[] data);

  /**
   * Update <i>this</i> union with the given ByteBuffer item.
   * If the ByteBuffer is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given ByteBuffer as a data
   * item.</p>
   *
   * @param data The given ByteBuffer.
   */
  public abstract void update(ByteBuffer data);

  /**
   * Update <i>this</i> union with the given integer array item.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given integer array as a data
   * item.</p>
   *
   * @param data The given int array.
   */
  public abstract void update(int[] data);

  /**
   * Update <i>this</i> union with the given char array item.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given char array as a data
   * item.</p>
   *
   * @param data The given char array.
   */
  public abstract void update(char[] data);

  /**
   * Update <i>this</i> union with the given long array item.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this is not a Sketch Union operation. This treats the given char array as a data
   * item.</p>
   *
   * @param data The given long array.
   */
  public abstract void update(long[] data);

}
