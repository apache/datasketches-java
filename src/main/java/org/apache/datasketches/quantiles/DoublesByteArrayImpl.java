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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.insertFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.insertK;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMaxDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertMinDouble;
import static org.apache.datasketches.quantiles.PreambleUtil.insertN;
import static org.apache.datasketches.quantiles.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.insertSerVer;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The doubles to byte array algorithms.
 *
 * @author Lee Rhodes
 * @author Jon Malkin
 */
final class DoublesByteArrayImpl {

  private DoublesByteArrayImpl() {}

  static byte[] toByteArray(final DoublesSketch sketch, final boolean ordered,
                            final boolean compact) {
    final boolean empty = sketch.isEmpty();

    //create the flags byte
    final int flags = (empty ? EMPTY_FLAG_MASK : 0)
        | (ordered ? ORDERED_FLAG_MASK : 0)
        | (compact ? (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK) : 0);

    if (empty && !sketch.isDirect()) { //empty & on-heap
      final byte[] outByteArr = new byte[Long.BYTES];
      final WritableMemory memOut = WritableMemory.wrap(outByteArr);
      final int preLongs = 1;
      insertPre0(memOut, preLongs, flags, sketch.getK());
      return outByteArr;
    }
    //not empty || direct; flags passed for convenience
    return convertToByteArray(sketch, flags, ordered, compact);
  }

  /**
   * Returns a byte array, including preamble, min, max and data extracted from the sketch.
   * @param sketch the given DoublesSketch
   * @param flags the Flags field
   * @param ordered true if the desired form of the resulting array has the base buffer sorted.
   * @param compact true if the desired form of the resulting array is in compact form.
   * @return a byte array, including preamble, min, max and data extracted from the Combined Buffer.
   */
  private static byte[] convertToByteArray(final DoublesSketch sketch, final int flags,
                                           final boolean ordered, final boolean compact) {
    final int preLongs = 2;
    final int extra = 2; // extra space for min and max values
    final int prePlusExtraBytes = (preLongs + extra) << 3;
    final int k = sketch.getK();
    final long n = sketch.getN();

    // If not-compact, have accessor always report full levels. Then use level size to determine
    // whether to copy data out.
    final DoublesSketchAccessor dsa = DoublesSketchAccessor.wrap(sketch, !compact);

    final int outBytes = (compact ? sketch.getCompactStorageBytes() : sketch.getUpdatableStorageBytes());

    final byte[] outByteArr = new byte[outBytes];
    final WritableMemory memOut = WritableMemory.wrap(outByteArr);

    //insert preamble-0, N, min, max
    insertPre0(memOut, preLongs, flags, k);
    if (sketch.isEmpty()) { return outByteArr; }

    insertN(memOut, n);
    insertMinDouble(memOut, sketch.getMinValue());
    insertMaxDouble(memOut, sketch.getMaxValue());

    long memOffsetBytes = prePlusExtraBytes;

    // might need to sort base buffer but don't want to change input sketch
    final int bbCnt = Util.computeBaseBufferItems(k, n);
    if (bbCnt > 0) { //Base buffer items only
      final double[] bbItemsArr = dsa.getArray(0, bbCnt);
      if (ordered) { Arrays.sort(bbItemsArr); }
      memOut.putDoubleArray(memOffsetBytes, bbItemsArr, 0, bbCnt);
    }
    // If n < 2k, totalLevels == 0 so ok to overshoot the offset update
    memOffsetBytes += (compact ? bbCnt : 2 * k) << 3;

    // If serializing from a compact sketch to a non-compact form, we may end up copying data for a
    // higher level one or more times into an unused level. A bit wasteful, but not incorrect.
    final int totalLevels = Util.computeTotalLevels(sketch.getBitPattern());
    for (int lvl = 0; lvl < totalLevels; ++lvl) {
      dsa.setLevel(lvl);
      if (dsa.numItems() > 0) {
        assert dsa.numItems() == k;
        memOut.putDoubleArray(memOffsetBytes, dsa.getArray(0, k), 0, k);
        memOffsetBytes += (k << 3);
      }
    }

    return outByteArr;
  }

  private static void insertPre0(final WritableMemory wmem,
      final int preLongs, final int flags, final int k) {
    insertPreLongs(wmem, preLongs);
    insertSerVer(wmem, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(wmem, Family.QUANTILES.getID());
    insertFlags(wmem, flags);
    insertK(wmem, k);
  }

}
