/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

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
   * Union the given on-heap sketch.
   * Valid for the all of the Open Source, Theta Sketches.
   * Not valid for older (prior to Open Source) Theta Sketches.
   * This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.
   *
   * @param sketchIn The incoming sketch.
   */
  public abstract void update(Sketch sketchIn);

  /**
   * Union the given Memory image of the OpenSource Theta Sketch,
   * which may be ordered or unordered, or the earlier versions of SetSketch,
   * which must be compact and ordered.
   *
   * <p>This method can be repeatedly called.
   * If the given sketch is null it is interpreted as an empty sketch.
   * @param mem Memory image of sketch to be merged
   */
  public abstract void update(Memory mem);

  /**
   * Present this union with a long.
   *
   * @param datum The given long datum.
   */
  public abstract void update(long datum);

  /**
   * Present this union with the given double (or float) datum.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   */
  public abstract void update(double datum);

  /**
   * Present this union with the given String.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param datum The given String.
   */
  public abstract void update(String datum);

  /**
   * Present this union with the given byte array.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   */
  public abstract void update(byte[] data);

  /**
   * Present this union with the given integer array.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   */
  public abstract void update(int[] data);

  /**
   * Present this union with the given char array.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   */
  public abstract void update(char[] data);

  /**
   * Present this union with the given long array.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   */
  public abstract void update(long[] data);

}
