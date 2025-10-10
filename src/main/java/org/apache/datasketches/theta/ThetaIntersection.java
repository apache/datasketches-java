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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * The API for intersection operations
 *
 * @author Lee Rhodes
 */
public abstract class ThetaIntersection extends ThetaSetOperation {

  @Override
  public Family getFamily() {
    return Family.INTERSECTION;
  }

  /**
   * Gets the result of this operation as an ordered CompactThetaSketch on the Java heap.
   * This does not disturb the underlying data structure of this intersection.
   * The {@link #intersect(ThetaSketch)} method must have been called at least once, otherwise an
   * exception will be thrown. This is because a virgin intersection represents the
   * Universal Set, which has an infinite number of values.
   * @return the result of this operation as an ordered CompactThetaSketch on the Java heap
   */
  public CompactThetaSketch getResult() {
    return getResult(true, null);
  }

  /**
   * Gets the result of this operation as a CompactThetaSketch in the given dstSeg.
   * This does not disturb the underlying data structure of this intersection.
   * The {@link #intersect(ThetaSketch)} method must have been called at least once, otherwise an
   * exception will be thrown. This is because a virgin intersection represents the
   * Universal Set, which has an infinite number of values.
   *
   * <p>Note that presenting an intersection with an empty sketch sets the internal
   * state of the intersection to empty = true, and current count = 0. This is consistent with
   * the mathematical definition of the intersection of any set with the empty set is
   * always empty.</p>
   *
   * <p>Presenting an intersection with a null argument will throw an exception.</p>
   *
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   *
   * @param dstSeg the destination MemorySegment.
   *
   * @return the result of this operation as a CompactThetaSketch stored in the given dstSeg,
   * which can be either on or off-heap..
   */
  public abstract CompactThetaSketch getResult(boolean dstOrdered, MemorySegment dstSeg);

  /**
   * Returns true if there is a valid intersection result available
   * @return true if there is a valid intersection result available
   */
  public abstract boolean hasResult();

  /**
   * Resets this ThetaIntersection for stateful operations only.
   * The seed remains intact, otherwise reverts to
   * the Universal Set: theta = 1.0, no retained data and empty = false.
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
  public abstract void intersect(ThetaSketch sketchIn);

  /**
   * Perform intersect set operation on the two given sketch arguments and return the result as an
   * ordered CompactThetaSketch on the heap.
   * @param a The first sketch argument
   * @param b The second sketch argument
   * @return an ordered CompactThetaSketch on the heap
   */
  public CompactThetaSketch intersect(final ThetaSketch a, final ThetaSketch b) {
    return intersect(a, b, true, null);
  }

  /**
   * Perform intersect set operation on the two given sketches and return the result as a
   * CompactThetaSketch.
   * @param a The first sketch argument
   * @param b The second sketch argument
   * @param dstOrdered
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>.
   * @param dstSeg the destination MemorySegment.
   * @return the result as a CompactThetaSketch.
   */
  public abstract CompactThetaSketch intersect(ThetaSketch a, ThetaSketch b, boolean dstOrdered,
      MemorySegment dstSeg);

  /**
   * Factory: Wrap a ThetaIntersection target around the given source MemorySegment containing intersection data.
   * This method assumes the <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * If the given source MemorySegment is read-only, the returned object will also be read-only.
   * @param srcSeg The source MemorySegment image.
   * @return a ThetaIntersection that wraps a source MemorySegment that contains a ThetaIntersection image
   */
  public static ThetaIntersection wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Factory: Wrap a ThetaIntersection target around the given source MemorySegment containing intersection data.
   * If the given source MemorySegment is read-only, the returned object will also be read-only.
   * @param srcSeg The source MemorySegment image.
   * @param expectedSeed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a ThetaIntersection that wraps a source MemorySegment that contains a ThetaIntersection image
   */
  public static ThetaIntersection wrap(final MemorySegment srcSeg, final long expectedSeed) {
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    return ThetaIntersectionImpl.wrapInstance(srcSeg, expectedSeed, srcSeg.isReadOnly() );
  }

}
