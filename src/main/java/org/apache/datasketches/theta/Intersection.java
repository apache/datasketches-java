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

import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.floorPowerOf2;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
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
   * Gets the result of this operation as an ordered CompactSketch on the Java heap.
   * The {@link #intersect(Sketch)} method must have been called at least once.
   * @return the result of this operation as an ordered CompactSketch on the Java heap
   */
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  /**
   * Gets the result of this operation as a CompactSketch of the chosen form.
   * The {@link #intersect(Sketch)} method must have been called at least once, otherwise an
   * exception will be thrown. This is because a virgin Intersection object represents the
   * Universal Set, which has an infinite number of values.
   *
   * <p>Note that presenting an intersection with an empty sketch sets the internal
   * state of the intersection to empty = true, and current count = 0. This is consistent with
   * the mathematical definition of the intersection of any set with the empty set is
   * always empty.</p>
   *
   * <p>Presenting an intersection with a null argument will throw an exception.</p>
   *
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
   * Returns true if there is an intersection result available
   * @return true if there is an intersection result available
   */
  public abstract boolean hasResult();

  /**
   * Resets this Intersection for stateful operations only.
   * The seed remains intact, otherwise reverts to
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
   * @deprecated Use {@link #intersect(Sketch)} instead.
   */
  @Deprecated
  public void update(final Sketch sketchIn) {
    intersect(sketchIn);
  }

  /**
   * Intersect the given sketch with the internal state.
   * This method can be repeatedly called.
   * If the given sketch is null the internal state becomes the empty sketch.
   * Theta will become the minimum of thetas seen so far.
   * @param sketchIn the given sketch
   */
  public abstract void intersect(Sketch sketchIn);

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

  // Restricted

  /**
   * Returns the maximum lgArrLongs given the capacity of the Memory.
   * @param dstMem the given Memory
   * @return the maximum lgArrLongs given the capacity of the Memory
   */
  protected static int getMaxLgArrLongs(final Memory dstMem) {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final long cap = dstMem.getCapacity();
    return Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
  }

  protected static void checkMinSizeMemory(final Memory mem) {
    final int minBytes = (CONST_PREAMBLE_LONGS << 3) + (8 << MIN_LG_ARR_LONGS);//280
    final long cap = mem.getCapacity();
    if (cap < minBytes) {
      throw new SketchesArgumentException(
          "Memory must be at least " + minBytes + " bytes. Actual capacity: " + cap);
    }
  }

  /**
   * Compact first 2^lgArrLongs of given array
   * @param srcCache anything
   * @param lgArrLongs The correct
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">lgArrLongs</a>.
   * @param curCount must be correct
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */ //Only used in IntersectionImpl & Test
  static final long[] compactCachePart(final long[] srcCache, final int lgArrLongs,
      final int curCount, final long thetaLong, final boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    final long[] cacheOut = new long[curCount];
    final int len = 1 << lgArrLongs;
    int j = 0;
    for (int i = 0; i < len; i++) {
      final long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) { continue; }
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }

  protected static void memChecks(final Memory srcMem) {
    //Get Preamble
    //Note: Intersection does not use lgNomLongs (or k), per se.
    //seedHash loaded and checked in private constructor
    final int preLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int famID = extractFamilyID(srcMem);
    final boolean empty = (extractFlags(srcMem) & EMPTY_FLAG_MASK) > 0;
    final int curCount = extractCurCount(srcMem);
    //Checks
    if (preLongs != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException(
          "Memory PreambleLongs must equal " + CONST_PREAMBLE_LONGS + ": " + preLongs);
    }
    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Serialization Version must equal " + SER_VER);
    }
    Family.INTERSECTION.checkFamilyID(famID);
    if (empty) {
      if (curCount != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: " + empty + "," + curCount);
      }
      //empty = true AND curCount_ = 0: OK
    } //else empty = false, curCount could be anything
  }

}
