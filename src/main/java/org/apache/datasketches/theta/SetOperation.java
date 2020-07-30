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

import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
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
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * heapified.</p>
   *
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
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * heapified.</p>
   *
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
   * This method assumes the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * wrapped.</p>
   *
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
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * wrapped.</p>
   *
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
    switch (family) {
      case UNION : {
        return UnionImpl.wrapInstance(srcMem, seed);
      }
      case INTERSECTION : {
        return IntersectionImpl.wrapInstance((WritableMemory)srcMem, seed, true);
      }
      default:
        throw new SketchesArgumentException("SetOperation cannot wrap family: " + family.toString());
    }
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * This method assumes the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * wrapped.</p>
   *
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
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * wrapped.</p>
   *
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
        return IntersectionImpl.wrapInstance(srcMem, seed, false);
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
   * Returns the maximum number of bytes for the returned CompactSketch, given the
   * value of nomEntries of the first sketch A of AnotB.
   * @param nomEntries this value must be a power of 2.
   * @return the maximum number of bytes.
   */
  public static int getMaxAnotBResultBytes(final int nomEntries) {
    final int ceil = ceilingPowerOf2(nomEntries);
    return 24 + (15 * ceil);
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
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized.
   * Only when they are stored into Memory will this be relevant.</p>
   *
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   */
  public abstract boolean isSameResource(Memory that);

  //restricted

  /**
   * Gets the hash array in compact form.
   * This is only useful during stateful operations.
   * This should never be made public.
   * @return the hash array
   */
  abstract long[] getCache();

  /**
   * Gets the current count of retained entries.
   * This is only useful during stateful operations.
   * Intentionally not made public because behavior will be confusing to end user.
   *
   * @return Gets the current count of retained entries.
   */
  abstract int getRetainedEntries();

  /**
   * Returns the seedHash established during class construction.
   * @return the seedHash.
   */
  abstract short getSeedHash();

  /**
   * Gets the current value of ThetaLong.
   * Only useful during stateful operations.
   * Intentionally not made public because behavior will be confusing to end user.
   * @return the current value of ThetaLong.
   */
  abstract long getThetaLong();

  /**
   * Returns true if this set operator is empty.
   * Only useful during stateful operations.
   * Intentionally not made public because behavior will be confusing to end user.
   * @return true if this set operator is empty.
   */
  abstract boolean isEmpty();

}
