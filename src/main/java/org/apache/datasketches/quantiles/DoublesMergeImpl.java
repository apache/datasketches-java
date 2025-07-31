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

import static org.apache.datasketches.common.Util.checkIfPowerOf2;
import static org.apache.datasketches.common.Util.clearBits;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.FLAGS_BYTE;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Down-sampling and merge algorithms for doubles quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesMergeImpl {

  private DoublesMergeImpl() {}

  /**
   * Merges the source sketch into the target sketch that can have a smaller K parameter.
   * However, it is required that the ratio of the two K parameters be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * <p>Note: It is easy to prove that the following simplified code which launches multiple waves of
   * carry propagation does exactly the same amount of merging work (including the work of
   * allocating fresh buffers) as the more complicated and seemingly more efficient approach that
   * tracks a single carry propagation wave through both sketches.
   *
   * <p>This simplified code probably does do slightly more "outer loop" work, but I am pretty
   * sure that even that is within a constant factor of the more complicated code, plus the
   * total amount of "outer loop" work is at least a factor of K smaller than the total amount of
   * merging work, which is identical in the two approaches.
   *
   * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented
   * by making a deep copy of the larger sketch and then merging the smaller one into it.
   * However, it was decided not to do this.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */
  static void mergeInto(final DoublesSketch src, final UpdateDoublesSketch tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();
    final long srcN = src.getN();
    final long tgtN = tgt.getN();

    if (srcK != tgtK) {
      downSamplingMergeInto(src, tgt);
      return;
    }
    //The remainder of this code is for the case where the k's are equal

    final DoublesSketchAccessor srcSketchBuf = DoublesSketchAccessor.wrap(src);
    final long nFinal = tgtN + srcN;

    for (int i = 0; i < srcSketchBuf.numItems(); i++) { // update only the base buffer
      tgt.update(srcSketchBuf.get(i));
    }

    final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(tgtK, nFinal);
    final int tgtCombBufItemCap = tgt.getCombinedBufferItemCapacity();
    if (spaceNeeded > tgtCombBufItemCap) { //copies base buffer plus current levels
      tgt.growCombinedBuffer(tgtCombBufItemCap, spaceNeeded);
    }

    final DoublesArrayAccessor scratch2KAcc = DoublesArrayAccessor.initialize(2 * tgtK);

    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));

    final DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(tgt, true);
    long newTgtBitPattern = tgt.getBitPattern();

    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
                srcLvl,
                srcSketchBuf.setLevel(srcLvl),
                scratch2KAcc,
                false,
                tgtK,
                tgtSketchBuf,
                newTgtBitPattern
        );
      }
    }

    if (tgt.hasMemorySegment() && (nFinal > 0)) {
      final MemorySegment seg = tgt.getMemorySegment();
      clearBits(seg, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }

    tgt.putN(nFinal);
    tgt.putBitPattern(newTgtBitPattern); // no-op if direct

    assert (tgt.getN() / (2L * tgtK)) == tgt.getBitPattern(); // internal consistency check

    double srcMax = src.getMaxItem();
    srcMax = Double.isNaN(srcMax) ? Double.NEGATIVE_INFINITY : srcMax;
    double srcMin = src.getMinItem();
    srcMin = Double.isNaN(srcMin) ? Double.POSITIVE_INFINITY : srcMin;

    double tgtMax = tgt.getMaxItem();
    tgtMax = Double.isNaN(tgtMax) ? Double.NEGATIVE_INFINITY : tgtMax;
    double tgtMin = tgt.getMinItem();
    tgtMin = Double.isNaN(tgtMin) ? Double.POSITIVE_INFINITY : tgtMin;

    tgt.putMaxItem(Math.max(srcMax, tgtMax));
    tgt.putMinItem(Math.min(srcMin, tgtMin));
  }

  /**
   * Merges the source sketch into the target sketch that can have a smaller K.
   * However, it is required that the ratio of the two K's be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */
  //also used by DoublesSketch, DoublesUnionImpl and HeapDoublesSketchTest
  static void downSamplingMergeInto(final DoublesSketch src, final UpdateDoublesSketch tgt) {
    final int sourceK = src.getK();
    final int targetK = tgt.getK();
    final long tgtN = tgt.getN();

    if ((sourceK % targetK) != 0) {
      throw new SketchesArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    final int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    if (src.isEmpty()) { return; }

    final DoublesSketchAccessor srcSketchBuf = DoublesSketchAccessor.wrap(src);
    final long nFinal = tgtN + src.getN();

    for (int i = 0; i < srcSketchBuf.numItems(); i++) { // update only the base buffer
      tgt.update(srcSketchBuf.get(i));
    }

    final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(targetK, nFinal);
    final int curCombBufCap = tgt.getCombinedBufferItemCapacity();
    if (spaceNeeded > curCombBufCap) { //copies base buffer plus current levels
      tgt.growCombinedBuffer(curCombBufCap, spaceNeeded);
    }

    //working scratch buffers
    final DoublesArrayAccessor scratch2KAcc = DoublesArrayAccessor.initialize(2 * targetK);
    final DoublesArrayAccessor downScratchKAcc = DoublesArrayAccessor.initialize(targetK);

    final DoublesSketchAccessor tgtSketchBuf = DoublesSketchAccessor.wrap(tgt, true);

    long srcBitPattern = src.getBitPattern();
    long newTgtBitPattern = tgt.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        justZipWithStride(
            srcSketchBuf.setLevel(srcLvl),
            downScratchKAcc,
            targetK,
            downFactor
        );
        newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
            srcLvl + lgDownFactor,    //starting level
            downScratchKAcc,       //optSrcKBuf,
            scratch2KAcc,          //size2KBuf,
            false,                    //do mergeInto version
            targetK,
            tgtSketchBuf,
            newTgtBitPattern
        );

        tgt.putBitPattern(newTgtBitPattern); //off-heap is a no-op
      }
    }
    if (tgt.hasMemorySegment() && (nFinal > 0)) {
      final MemorySegment seg = tgt.getMemorySegment();
      clearBits(seg, FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
    }
    tgt.putN(nFinal);

    assert (tgt.getN() / (2L * targetK)) == newTgtBitPattern; // internal consistency check

    double srcMax = src.getMaxItem();
    srcMax = Double.isNaN(srcMax) ? Double.NEGATIVE_INFINITY : srcMax;
    double srcMin = src.getMinItem();
    srcMin = Double.isNaN(srcMin) ? Double.POSITIVE_INFINITY : srcMin;

    double tgtMax = tgt.getMaxItem();
    tgtMax = Double.isNaN(tgtMax) ? Double.NEGATIVE_INFINITY : tgtMax;
    double tgtMin = tgt.getMinItem();
    tgtMin = Double.isNaN(tgtMin) ? Double.POSITIVE_INFINITY : tgtMin;

    if (srcMax > tgtMax) { tgt.putMaxItem(srcMax); }
    if (srcMin < tgtMin) { tgt.putMinItem(srcMin); }
  }

  private static void justZipWithStride(
          final DoublesBufferAccessor bufA, // input
          final DoublesBufferAccessor bufC, // output
          final int kC, // number of items that should be in the output
          final int stride) {
    final int randomOffset = DoublesSketch.rand.nextInt(stride);
    for (int a = randomOffset, c = 0; c < kC; a += stride, c++ ) {
      bufC.set(c, bufA.get(a));
    }
  }

}
