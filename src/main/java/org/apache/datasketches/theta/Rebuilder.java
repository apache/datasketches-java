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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.QuickSelect.selectExcludingZeros;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.HashOperations;

/**
 * This class performs resize, rebuild and move operations where the input and output are Theta sketch images in MemorySegments.
 *
 * <p><b>NOTE:</b> These operations copy data from the input MemorySegment into local arrays, perform the required operations on the
 * arrays, and then copies the result to the destination MemorySegment. Attempting to perform these operations directly on the
 * MemorySegments would be slower due to MemorySegment internal checks. Meanwhile, he bulk copies performed by the MemorySegments are
 * vectorized at the machine level and are quite fast. Measurements reveal that this is a good tradeoff.</p>
 *
 * @author Lee Rhodes
 */
final class Rebuilder {

  private Rebuilder() {}

  /**
   * Rebuild the hashTable in the given MemorySegment at its current size. Changes theta and thus count.
   * This assumes a MemorySegment preamble of standard form with correct values of curCount and thetaLong.
   * ThetaLong and curCount will change.
   * Afterwards, caller must update local class members curCount and thetaLong from MemorySegment.
   *
   * @param seg the given MemorySegment
   * @param preambleLongs size of preamble in longs
   * @param lgNomLongs the log_base2 of k, the configuration parameter of the sketch
   */
  static final void quickSelectAndRebuild(final MemorySegment seg, final int preambleLongs, final int lgNomLongs) {

    //Copy data from input segment into local buffer array for QS algorithm
    final int lgArrLongs = extractLgArrLongs(seg);
    final int arrLongs = 1 << lgArrLongs;
    final long[] tmpArr = new long[arrLongs];
    final int preBytes = preambleLongs << 3;
    MemorySegment.copy(seg, JAVA_LONG_UNALIGNED, preBytes, tmpArr, 0, arrLongs);

    //Do the QuickSelect on a tmp arr to create new thetaLong
    final int pivot = (1 << lgNomLongs) + 1; // (K+1) pivot for QS
    final long newThetaLong = selectExcludingZeros(tmpArr, extractCurCount(seg), pivot);
    insertThetaLong(seg, newThetaLong); //UPDATE thetaLong

    //Rebuild to clean up dirty data, update count
    final long[] tgtArr = new long[arrLongs];
    final int newCurCount =
        HashOperations.hashArrayInsert(tmpArr, tgtArr, lgArrLongs, newThetaLong);
    insertCurCount(seg, newCurCount); //UPDATE curCount

    //put the rebuilt array back into MemorySegment
    MemorySegment.copy(tgtArr, 0, seg, JAVA_LONG_UNALIGNED, preBytes, arrLongs);
  }

  /**
   * Moves me (the entire updatable sketch) to a new larger MemorySegment location and rebuilds the hash table.
   * This assumes a MemorySegment preamble of standard form with the correct value of thetaLong.
   * Afterwards, the caller must update the local MemorySegment reference, lgArrLongs
   * and hashTableThreshold from the destination MemorySegment and free the source MemorySegment.
   *
   * @param srcSeg the source MemorySegment
   * @param preambleLongs size of preamble in longs
   * @param srcLgArrLongs size (log_base2) of source hash table
   * @param dstSeg the destination MemorySegment, which may be garbage
   * @param dstLgArrLongs the destination hash table target size
   * @param thetaLong theta as a long
   */
  static final void moveAndResize(final MemorySegment srcSeg, final int preambleLongs,
      final int srcLgArrLongs, final MemorySegment dstSeg, final int dstLgArrLongs, final long thetaLong) {

    //Move Preamble to destination MemorySegment
    final int preBytes = preambleLongs << 3;
    MemorySegment.copy(srcSeg, 0, dstSeg, 0, preBytes);

    //Bulk copy source Hash Table to local buffer array
    final int srcHTLen = 1 << srcLgArrLongs;
    final long[] srcHTArr = new long[srcHTLen];
    MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, preBytes, srcHTArr, 0, srcHTLen);

    //Create destination buffer
    final int dstHTLen = 1 << dstLgArrLongs;
    final long[] dstHTArr = new long[dstHTLen];

    //Rebuild hash table in destination buffer
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, dstLgArrLongs, thetaLong);

    //Bulk copy to destination MemorySegment
    MemorySegment.copy(dstHTArr, 0, dstSeg, JAVA_LONG_UNALIGNED, preBytes, dstHTLen);
    dstSeg.set(JAVA_BYTE, LG_ARR_LONGS_BYTE, (byte)dstLgArrLongs); //update lgArrLongs in dstSeg
  }

  /**
   * Resizes existing hash array into a larger one within a single MemorySegment, assuming enough space.
   * This assumes a preamble of standard form with the correct value of thetaLong.
   * The lgArrLongs will change.
   * Afterwards, the caller must update the caller's local copies of lgArrLongs and hashTableThreshold
   * from the returned MemorySegment.
   *
   * @param seg the source and destination MemorySegment
   * @param preambleLongs the size of the preamble in longs
   * @param srcLgArrLongs the size of the source hash table
   * @param tgtLgArrLongs the LgArrLongs value for the new hash table
   */
  static final void resize(final MemorySegment seg, final int preambleLongs,
      final int srcLgArrLongs, final int tgtLgArrLongs) {

    //Preamble stays in place
    final int preBytes = preambleLongs << 3;

    //Bulk copy source to on-heap buffer
    final int srcHTLen = 1 << srcLgArrLongs; //current value
    final long[] srcHTArr = new long[srcHTLen]; //on-heap src buffer
    //seg.getLongArray(preBytes, srcHTArr, 0, srcHTLen);
    MemorySegment.copy(seg, JAVA_LONG_UNALIGNED, preBytes, srcHTArr, 0, srcHTLen);

    //Create destination on-heap buffer
    final int dstHTLen = 1 << tgtLgArrLongs;
    final long[] dstHTArr = new long[dstHTLen]; //on-heap dst buffer

    //Rebuild hash table in destination buffer
    HashOperations.hashArrayInsert(srcHTArr, dstHTArr, tgtLgArrLongs, extractThetaLong(seg));

    //Bulk copy to destination segment
    MemorySegment.copy(dstHTArr, 0, seg, JAVA_LONG_UNALIGNED, preBytes, dstHTLen);
    insertLgArrLongs(seg, tgtLgArrLongs); //update in mem
  }

  /**
   * Returns the actual log2 Resize Factor that can be used to grow the hash table. This will be
   * an integer value between zero and the given lgRF, inclusive;
   * @param capBytes the current MemorySegment capacity in bytes
   * @param lgArrLongs the current lg hash table size in longs
   * @param preLongs the current preamble size in longs
   * @param lgRF the configured lg Resize Factor
   * @return the actual log2 Resize Factor that can be used to grow the hash table
   */
  static final int actLgResizeFactor(final long capBytes, final int lgArrLongs, final int preLongs,
      final int lgRF) {
    final int maxHTLongs = Util.floorPowerOf2(((int)(capBytes >>> 3) - preLongs));
    final int lgFactor = Math.max(Integer.numberOfTrailingZeros(maxHTLongs) - lgArrLongs, 0);
    return (lgFactor >= lgRF) ? lgRF : lgFactor;
  }

}
