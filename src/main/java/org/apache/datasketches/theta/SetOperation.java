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

import static java.lang.Math.max;
import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The parent API for all Set Operations
 *
 * @author Lee Rhodes
 */
public abstract class SetOperation {
  static final int CONST_PREAMBLE_LONGS = 3;

  SetOperation() {}

  /**
   * Makes a new builder
   *
   * @return a new builder
   */
  public static final SetOperationBuilder builder() {
    return new SetOperationBuilder();
  }

  /**
   * Heapify takes the SetOperations image in Memory and instantiates an on-heap
   * SetOperation using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting SetOperation will not retain any link to the source Memory.
   * @param srcMem an image of a SetOperation where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Heap-based SetOperation from the given Memory
   */
  public static SetOperation heapify(final Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the SetOperation image in Memory and instantiates an on-heap
   * SetOperation using the given seed.
   * The resulting SetOperation will not retain any link to the source Memory.
   * @param srcMem an image of a SetOperation where the hash of the given seed matches the image seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a Heap-based SetOperation from the given Memory
   */
  public static SetOperation heapify(final Memory srcMem, final long seed) {
    final byte famID = srcMem.getByte(FAMILY_BYTE);
    final Family family = idToFamily(famID);
    switch (family) { //TODO Do we need to add the stateful AnotB ?
      case UNION : {
        return UnionImpl.heapifyInstance(srcMem, seed);
      }
      case INTERSECTION : {
        return IntersectionImpl.heapifyInstance(srcMem, seed);
      }
      default: {
        throw new SketchesArgumentException("SetOperation cannot heapify family: "
            + family.toString());
      }
    }
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * Only "Direct" SetOperations that have been explicitly stored as direct can be wrapped.
   * This method assumes the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * @param srcMem an image of a SetOperation where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(final Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * Only "Direct" SetOperations that have been explicitly stored as direct can be wrapped.
   * @param srcMem an image of a SetOperation where the hash of the given seed matches the image seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(final Memory srcMem, final long seed) {
    final byte famID = srcMem.getByte(FAMILY_BYTE);
    final Family family = idToFamily(famID);
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    switch (family) { //TODO Do we need to add the stateful AnotB ?
      case UNION : {
        return UnionImpl.wrapInstance(srcMem, seed);
      }
      case INTERSECTION : {
        return IntersectionImplR.wrapInstance(srcMem, seed);
      }
      default:
        throw new SketchesArgumentException("SetOperation cannot wrap family: " + family.toString());
    }
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * Only "Direct" SetOperations that have been explicitly stored as direct can be wrapped.
   * This method assumes the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * @param srcMem an image of a SetOperation where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(final WritableMemory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * Only "Direct" SetOperations that have been explicitly stored as direct can be wrapped.
   * @param srcMem an image of a SetOperation where the hash of the given seed matches the image seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(final WritableMemory srcMem, final long seed) {
    final byte famID = srcMem.getByte(FAMILY_BYTE);
    final Family family = idToFamily(famID);
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    switch (family) {
      case UNION : {
        return UnionImpl.wrapInstance(srcMem, seed);
      }
      case INTERSECTION : {
        return IntersectionImpl.wrapInstance(srcMem, seed);
      }
      default:
        throw new SketchesArgumentException("SetOperation cannot wrap family: "
            + family.toString());
    }
  }

  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Union operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxUnionBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.UNION.getMaxPreLongs() << 3);
  }

  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Intersection
   * operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxIntersectionBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    final int bytes = (nomEnt << 4) + (Family.INTERSECTION.getMaxPreLongs() << 3);
    return bytes;
  }

  /**
   * Returns the maximum number of bytes for the returned CompactSketch, given the maximum
   * value of nomEntries of the first sketch A of AnotB.
   * @param maxNomEntries the given value
   * @return the maximum number of bytes.
   */
  public static int getMaxAnotBResultBytes(final int maxNomEntries) {
    return 24 + (15 * maxNomEntries);
  }


  /**
   * Gets the Family of this SetOperation
   * @return the Family of this SetOperation
   */
  public abstract Family getFamily();

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   */
  public abstract boolean isSameResource(Memory that);

  //restricted

  abstract long[] getCache();

  //intentionally not made public because behavior will be confusing to end user.
  abstract int getRetainedEntries(boolean valid);

  abstract short getSeedHash();

  abstract long getThetaLong();

  static short computeSeedHash(final long seed) {
    return Util.computeSeedHash(seed);
  }

  //intentionally not made public because behavior will be confusing to end user.
  abstract boolean isEmpty();

  /**
   * Computes minimum lgArrLongs from a current count.
   * @param count the given current count
   * @return the minimum lgArrLongs from a current count.
   */
  //Used by intersection and AnotB
  static final int computeMinLgArrLongsFromCount(final int count) {
    final int upperCount = (int) Math.ceil(count / REBUILD_THRESHOLD);
    final int arrLongs = max(ceilingPowerOf2(upperCount), 1 << MIN_LG_ARR_LONGS);
    final int newLgArrLongs = Integer.numberOfTrailingZeros(arrLongs);
    return newLgArrLongs;
  }

  /**
   * Returns true if given Family id is one of the set operations
   * @param id the given Family id
   * @return true if given Family id is one of the set operations
   */
  static boolean isValidSetOpID(final int id) {
    final Family family = Family.idToFamily(id);
    final boolean ret = ((family == Family.UNION) || (family == Family.INTERSECTION)
        || (family == Family.A_NOT_B));
    return ret;
  }

  /**
   * This corrects a temporary anomalous condition from an intersection of two exact, disjoint sets,
   * or AnotB of two exact, identical sets. Because there is no probability error distribuion,
   * the result is exactly empty. The empty flag is changed to true before returning a result.
   * This is used in the compaction step of SetOperation.
   * @param curCount the given curCount
   * @param thetaLong the given theta.
   * @return the empty state
   */
  private static final boolean correctEmptyOnSetResult(final int curCount, final long thetaLong) {
    return (curCount == 0) && (thetaLong == Long.MAX_VALUE);
  }

}
