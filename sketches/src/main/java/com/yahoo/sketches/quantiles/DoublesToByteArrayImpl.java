/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;

/**
 * The doubles to byte array algorithms.
 *
 * @author Lee Rhodes
 */
class DoublesToByteArrayImpl {

  static byte[] toByteArray(DoublesSketch sketch, boolean ordered, boolean compact) {
    boolean empty = sketch.isEmpty();

    int flags = (empty ? EMPTY_FLAG_MASK : 0)
        | (ordered ? ORDERED_FLAG_MASK : 0)
        | (compact ? COMPACT_FLAG_MASK : 0);

    if (empty) {
      byte[] outByteArr = new byte[Long.BYTES];
      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);
      int preLongs = 1;
      insertPre0(outByteArr, cumOffset, preLongs, flags, sketch.getK());
      return outByteArr;
    }
    //not empty
    return combinedBufferToByteArray(sketch, ordered, compact);
  }

  /**
   * Returns a byte array, including preamble, min, max and data extracted from the Combined Buffer.
   * @param ordered true if the desired form of the resulting array has the base buffer sorted.
   * @param compact true if the desired form of the resulting array is in compact form.
   * @return a byte array, including preamble, min, max and data extracted from the Combined Buffer.
   */
  static byte[] combinedBufferToByteArray(DoublesSketch sketch, boolean ordered,
      boolean compact) {
    final int preLongs = 2;
    final int extra = 2; // extra space for min and max values
    int preBytes = (preLongs + extra) << 3;
    int flags = (ordered ? ORDERED_FLAG_MASK : 0) | (compact ? COMPACT_FLAG_MASK : 0);
    int k = sketch.getK();
    long n = sketch.getN();
    double[] combinedBuffer = sketch.getCombinedBuffer();
    double[] bbItemsArr = null;

    final int bbCnt = Util.computeBaseBufferItems(k, n);
    if (bbCnt > 0) {
      bbItemsArr = new double[bbCnt];
      System.arraycopy(combinedBuffer, 0, bbItemsArr, 0, bbCnt);
      if (ordered) { Arrays.sort(bbItemsArr); }
    }
    byte[] outByteArr = null;

    if (compact) {
      final int retainedItems = sketch.getRetainedItems();
      int outBytes = (retainedItems << 3) + preBytes;
      outByteArr = new byte[outBytes];

      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);

      //insert preamble, min, max
      insertPre0(outByteArr, cumOffset, preLongs, flags, k);
      insertN(outByteArr, cumOffset, n);
      insertMinDouble(outByteArr, cumOffset, sketch.getMinValue());
      insertMaxDouble(outByteArr, cumOffset, sketch.getMaxValue());

      //insert base buffer
      if (bbCnt > 0) {
        memOut.putDoubleArray(preBytes, bbItemsArr, 0, bbCnt);
      }
      //insert levels into compact dstMem (and array)
      long bits = sketch.getBitPattern();
      if (bits != 0) {
        long memOffset = preBytes + (bbCnt << 3); //bytes
        int combBufOffset = 2 * k; //doubles
        while (bits != 0L) {
          if ((bits & 1L) > 0L) {
            memOut.putDoubleArray(memOffset, combinedBuffer, combBufOffset, k);
            memOffset += (k << 3); //bytes, increment compactly
          }
          combBufOffset += k; //doubles, increment every level
          bits >>>= 1;
        }
      }

    } else { //not compact
      final int totLevels = Util.computeNumLevelsNeeded(k, n);
      int outBytes = (totLevels == 0)
          ? (bbCnt << 3) + preBytes
          : (((2 + totLevels) * k) << 3)  + preBytes;
      outByteArr = new byte[outBytes];

      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);

      //insert preamble, min, max
      insertPre0(outByteArr, cumOffset, preLongs, flags, k);
      insertN(outByteArr, cumOffset, n);
      insertMinDouble(outByteArr, cumOffset, sketch.getMinValue());
      insertMaxDouble(outByteArr, cumOffset, sketch.getMaxValue());

      //insert base buffer
      if (bbCnt > 0) {
        memOut.putDoubleArray(preBytes, bbItemsArr, 0, bbCnt);
      }
      //insert levels
      if (totLevels > 0) {
        long memOffset = preBytes + ((2L * k) << 3);
        int combBufOffset = 2 * k;
        memOut.putDoubleArray(memOffset, combinedBuffer, combBufOffset, totLevels * k);
      }
    }
    return outByteArr;
  }

  static void insertPre0(byte[] outArr, long cumOffset, int preLongs, int flags,
      int k) {
    insertPreLongs(outArr, cumOffset, preLongs);
    insertSerVer(outArr, cumOffset, DoublesSketch.DOUBLES_SER_VER);
    insertFamilyID(outArr, cumOffset, Family.QUANTILES.getID());
    insertFlags(outArr, cumOffset, flags);
    insertK(outArr, cumOffset, k);
  }

}
