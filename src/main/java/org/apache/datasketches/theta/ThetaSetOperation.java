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
import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

/**
 * The parent API for all Set Operations
 *
 * @author Lee Rhodes
 */
public abstract class ThetaSetOperation implements MemorySegmentStatus {
  static final int CONST_PREAMBLE_LONGS = 3;

  /**
   * Constructor
   */
  ThetaSetOperation() {}

  /**
   * Makes a new builder
   *
   * @return a new builder
   */
  public static final ThetaSetOperationBuilder builder() {
    return new ThetaSetOperationBuilder();
  }

  /**
   * Heapify takes the ThetaSetOperation image in MemorySegment and instantiates an on-heap
   * ThetaSetOperation using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting ThetaSetOperation will not retain any link to the source MemorySegment.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * heapified.</p>
   *
   * @param srcSeg an image of a ThetaSetOperation where the image seed hash matches the default seed hash.
   * @return a Heap-based ThetaSetOperation from the given MemorySegment
   */
  public static ThetaSetOperation heapify(final MemorySegment srcSeg) {
    return heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the ThetaSetOperation image in MemorySegment and instantiates an on-heap
   * ThetaSetOperation using the given expectedSeed.
   * The resulting ThetaSetOperation will not retain any link to the source MemorySegment.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus
   * heapified.</p>
   *
   * @param srcSeg an image of a ThetaSetOperation where the hash of the given expectedSeed matches the image seed hash.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a Heap-based ThetaSetOperation from the given MemorySegment
   */
  public static ThetaSetOperation heapify(final MemorySegment srcSeg, final long expectedSeed) {
    final byte famID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    final Family family = idToFamily(famID);
    switch (family) {
      case UNION : {
        return ThetaUnionImpl.heapifyInstance(srcSeg, expectedSeed);
      }
      case INTERSECTION : {
        return ThetaIntersectionImpl.heapifyInstance(srcSeg, expectedSeed);
      }
      default: {
        throw new SketchesArgumentException("ThetaSetOperation cannot heapify family: "
            + family.toString());
      }
    }
  }

  /**
   * Wrap takes the ThetaSetOperation image in MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * This method assumes the <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * If the given source MemorySegment is read-only, the returned object will also be read-only.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus wrapped.</p>
   *
   * @param srcSeg an image of a ThetaSetOperation where the image seed hash matches the default seed hash.
   * @return a ThetaSetOperation backed by the given MemorySegment
   */
  public static ThetaSetOperation wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the ThetaSetOperation image in MemorySegment and refers to it directly.
   * There is no data copying onto the java heap.
   * If the given source MemorySegment is read-only, the returned object will also be read-only.
   *
   * <p>Note: Only certain set operators during stateful operations can be serialized and thus wrapped.</p>
   *
   * @param srcSeg an image of a ThetaSetOperation where the hash of the given expectedSeed matches the image seed hash.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a ThetaSetOperation backed by the given MemorySegment
   */
  public static ThetaSetOperation wrap(final MemorySegment srcSeg, final long expectedSeed) {
    final byte famID = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    final Family family = idToFamily(famID);
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer != 3) {
      throw new SketchesArgumentException("SerVer must be 3: " + serVer);
    }
    switch (family) {
      case UNION : {
        return ThetaUnionImpl.wrapInstance(srcSeg, expectedSeed);
      }
      case INTERSECTION : {
        return ThetaIntersectionImpl.wrapInstance(srcSeg, expectedSeed, srcSeg.isReadOnly() );
      }
      default:
        throw new SketchesArgumentException("ThetaSetOperation cannot wrap family: " + family.toString());
    }
  }

  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for ThetaUnion operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxUnionBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.UNION.getMaxPreLongs() << 3);
  }

  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for intersection
   * operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxIntersectionBytes(final int nomEntries) {
    final int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.INTERSECTION.getMaxPreLongs() << 3);
  }

  /**
   * Returns the maximum number of bytes for the returned CompactThetaSketch, given the
   * value of nomEntries of the first sketch A of ThetaAnotB.
   * @param nomEntries this value must be a power of 2.
   * @return the maximum number of bytes.
   */
  public static int getMaxAnotBResultBytes(final int nomEntries) {
    final int ceil = ceilingPowerOf2(nomEntries);
    return 24 + (15 * ceil);
  }

  /**
   * Gets the Family of this ThetaSetOperation
   * @return the Family of this ThetaSetOperation
   */
  public abstract Family getFamily();

  //restricted

  /**
   * Gets the hash array in compact form.
   * This is only useful during stateful operations.
   * This should never be made public.
   * @return the hash array
   */
  abstract long[] getCache();

  /**
   * Returns the backing MemorySegment object if it exists, otherwise null.
   * @return the backing MemorySegment object if it exists, otherwise null.
   */
  MemorySegment getMemorySegment() { return null; }

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
