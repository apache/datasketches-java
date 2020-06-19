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

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The API for Union operations
 *
 * @author Lee Rhodes
 */
public abstract class Union extends SetOperation {

  @Override
  public Family getFamily() {
    return Family.UNION;
  }

  /**
   * Gets the result of this operation as a CompactSketch of the chosen form.
   * This does not disturb the underlying data structure of the union.
   * Therefore, it is OK to continue updating the union after this operation.
   *
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @return the result of this operation as a CompactSketch of the chosen form
   */
  public abstract CompactSketch getResult(boolean dstOrdered, WritableMemory dstMem);

  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap.
   * This does not disturb the underlying data structure of the union.
   * Therefore, it is OK to continue updating the union after this operation.
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  public abstract CompactSketch getResult();

  /**
   * Resets this Union. The seed remains intact, otherwise reverts back to its virgin state.
   */
  public abstract void reset();

  /**
   * Returns a byte array image of this Union object
   * @return a byte array image of this Union object
   */
  public abstract byte[] toByteArray();

  /**
   * This implements a stateless, pair-wise union operation. The returned sketch will be cutback to
   * k if required, similar to the regular Union operation.
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
   * This implements a stateless, pair-wise union operation. The returned sketch will be cutback to
   * k if required, similar to the regular Union operation.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param sketchA The first argument
   * @param sketchB The second argument
   * @param dstOrdered If true, the returned CompactSketch will be ordered.
   * @param dstMem If not null, the returned CompactSketch will be placed in this WritableMemory.
   * @return the result CompactSketch.
   */
  public abstract CompactSketch union(Sketch sketchA, Sketch sketchB, boolean dstOrdered,
      WritableMemory dstMem);


  /**
   * Perform a Union operation with <i>this</i> union and the given on-heap sketch of the Theta Family.
   * This method is not valid for the older SetSketch, which was prior to Open Source (August, 2015).
   *
   * <p>This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.</p>
   *
   * @param sketchIn The incoming sketch.
   */
  public abstract void update(Sketch sketchIn);

  /**
   * Perform a Union operation with <i>this</i> union and the given Memory image of any sketch of the
   * Theta Family. The input image may be from earlier versions of the Theta Compact Sketch,
   * called the SetSketch (circa 2012), which was prior to Open Source and are compact and ordered.
   *
   * <p>This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.</p>
   *
   * @param mem Memory image of sketch to be merged
   */
  public abstract void update(Memory mem);

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
