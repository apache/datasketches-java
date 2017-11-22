/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

/**
 * The API for intersection operations
 *
 * @author Lee Rhodes
 */
public abstract class Intersection extends SetOperation {

  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
  }

  /**
   * Gets the result of this operation as a CompactSketch of the chosen form.
   * The update method must have been called at least once, otherwise an exception will be
   * thrown. This is because a virgin Intersection object represents the Universal Set,
   * which would have an infinite number of values.
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
   * The update method must have been called at least once.
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  public abstract CompactSketch getResult();

  /**
   * Returns true if there is an intersection result available
   * @return true if there is an intersection result available
   */
  public abstract boolean hasResult();

  /**
   * Resets this Intersection. The seed remains intact, otherwise reverts to
   * the Universal Set, theta of 1.0 and empty = false.
   */
  public abstract void reset();

  /**
   * Serialize this intersection to a byte array form.
   * @return byte array of this intersection
   */
  public abstract byte[] toByteArray();

  /**
   * Intersect the given sketch with the internal state.
   * This method can be repeatedly called.
   * If the given sketch is null the internal state becomes the empty sketch.
   * Theta will become the minimum of thetas seen so far.
   * @param sketchIn the given sketch
   */
  public abstract void update(Sketch sketchIn);

}
