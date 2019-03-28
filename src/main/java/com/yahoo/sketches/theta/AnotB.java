/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

/**
 * The API for the set difference operation <i>A and not B</i> operations.
 * This is a stateless operation. However, to make the API
 * more consistent with the other set operations the intended use is:
 * <pre><code>
 * AnotB aNotB = SetOperationBuilder.buildAnotB();
 * aNotB.update(SketchA, SketchB); //Called only once.
 * CompactSketch result = aNotB.getResult();
 * </code></pre>
 *
 * <p>Calling the update function a second time essentially clears the internal state and updates
 * with the new pair of sketches.
 *
 * <p>As an alternative, one can use the aNotB method that returns the result immediately.
 *
 * @author Lee Rhodes
 */
public abstract class AnotB extends SetOperation {

  @Override
  public Family getFamily() {
    return Family.A_NOT_B;
  }

  /**
   * Gets the result of this operation as an ordered CompactSketch on the Java heap
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  public abstract CompactSketch getResult();

  /**
   * Gets the result of this set operation as a CompactSketch of the chosen form
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   *
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   *
   * @return the result of this set operation as a CompactSketch of the chosen form
   */
  public abstract CompactSketch getResult(boolean dstOrdered, WritableMemory dstMem);

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   *
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   */
  public abstract void update(Sketch a, Sketch b);

  /**
   * Perform A-and-not-B set operation on the two given sketches and return the result as an
   * ordered CompactSketch on the heap.
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   * @return an ordered CompactSketch on the heap
   */
  public CompactSketch aNotB(final Sketch a, final Sketch b) {
    return aNotB(a, b, true, null);
  }

  /**
   * Perform A-and-not-B set operation on the two given sketches and return the result as a
   * CompactSketch.
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return the result as a CompactSketch.
   */
  public abstract CompactSketch aNotB(Sketch a, Sketch b, boolean dstOrdered,
      WritableMemory dstMem);

}
