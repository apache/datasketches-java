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
import org.apache.datasketches.memory.WritableMemory;

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
   * which has an infinite number of values.
   *
   * <p>Note that presenting an intersection with an empty or null sketch sets the internal
   * state of the intersection to empty = true, and current count = 0. This is consistent with
   * the mathematical definition of the intersection of any set with the null set is always null.</p>
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

  /**
   * Perform intersect set operation on the two given sketch arguments and return the result as an
   * ordered CompactSketch on the heap.
   * @param a The first sketch argument
   * @param b The second sketch argument
   * @return an ordered CompactSketch on the heap
   */
  public CompactSketch intersect(final Sketch a, final Sketch b) {
    return intersect(a, b, true, null);
  }

  /**
   * Perform intersect set operation on the two given sketches and return the result as a
   * CompactSketch.
   * @param a The first sketch argument
   * @param b The second sketch argument
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   * @param dstMem
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * @return the result as a CompactSketch.
   */
  public abstract CompactSketch intersect(Sketch a, Sketch b, boolean dstOrdered,
      WritableMemory dstMem);

}
