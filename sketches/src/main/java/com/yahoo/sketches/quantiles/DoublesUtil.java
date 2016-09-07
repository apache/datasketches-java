/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static java.lang.System.arraycopy;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Static methods that support the doubles quantiles algorithms.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
final class DoublesUtil {

  /**
   * Checks the sequential validity of the given array of values. 
   * They must be unique, monotonically increasing and not NaN.
   * @param values given array of values
   */
  static final void validateValues(final double[] values) {
    final int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if (values[j] < values[j + 1]) continue;
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }

  /**
   * Shared algorithm for both PMF and CDF functions. The splitPoints must be unique, monotonically
   * increasing values.
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * @param sketch the given quantiles sketch
   * @return the unnormalized, accumulated counts of <i>m + 1</i> intervals.
   */
  static long[] internalBuildHistogram(final double[] splitPoints, final HeapDoublesSketch sketch) {
    final double[] levelsArr  = sketch.getCombinedBuffer();
    final double[] baseBuffer = levelsArr;
    final int bbCount = sketch.getBaseBufferCount();
    validateValues(splitPoints);

    final int numSplitPoints = splitPoints.length;
    final int numCounters = numSplitPoints + 1;
    final long[] counters = new long[numCounters];

    long weight = 1;
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      DoublesUtil.bilinearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    } else {
      Arrays.sort(baseBuffer, 0, bbCount);
      // sort is worth it when many split points
      DoublesUtil.linearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    }

    long myBitPattern = sketch.getBitPattern();
    final int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight += weight; // *= 2
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        DoublesUtil.linearTimeIncrementHistogramCounters(
            levelsArr, (2 + lvl) * k, k, weight, splitPoints, counters);
      }
    }
    return counters;
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param sketch the given quantiles sketch
   */
  static void processFullBaseBuffer(final HeapDoublesSketch sketch) {
    final int bbCount = sketch.getBaseBufferCount();
    final long n = sketch.getN();
    assert bbCount == 2 * sketch.getK(); // internal consistency check

    // make sure there will be enough levels for the propagation
    DoublesUtil.maybeGrowLevels(n, sketch); // important: n_ was incremented by update before we got here

    // this aliasing is a bit dangerous; notice that we did it after the possible resizing
    final double[] baseBuffer = sketch.getCombinedBuffer(); 

    Arrays.sort(baseBuffer, 0, bbCount);
    inPlacePropagateCarry(
        0,
        null, 0,  // this null is okay
        baseBuffer, 0,
        true, sketch);
    sketch.baseBufferCount_ = 0;
    assert n / (2 * sketch.getK()) == sketch.getBitPattern(); // internal consistency check
  }

  static void inPlacePropagateCarry(
      final int startingLevel,
      final double[] sizeKBuf, final int sizeKStart,
      final double[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, final HeapDoublesSketch sketch
    ) { // else doMergeIntoVersion
    final double[] levelsArr = sketch.getCombinedBuffer();
    final int k = sketch.getK();
    final long bitPattern = sketch.bitPattern_; //the one prior to the last increment of n_
    final int endingLevel = Util.positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);
  
    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKbuf to be null in this case
      DoublesUtil.zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    } else { // mergeInto version of computation
      System.arraycopy(
          sizeKBuf, sizeKStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    }
  
    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      DoublesUtil.mergeTwoSizeKBuffers(
          levelsArr, (2 + lvl) * k,
          levelsArr, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k);
      DoublesUtil.zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (1L << startingLevel);
  }

  static void maybeGrowLevels(final long newN, final HeapDoublesSketch sketch) { // important: newN might not equal n_
    final int k = sketch.getK();
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      return; // don't need any levels yet, and might have small base buffer; this can happen during a merge
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0; 
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    if (spaceNeeded <= sketch.getCombinedBufferItemCapacity()) {
      return;
    }
    // copies base buffer plus old levels
    sketch.combinedBuffer_ = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded); 
    sketch.combinedBufferItemCapacity_ = spaceNeeded;
  }

  static void growBaseBuffer(final HeapDoublesSketch sketch) {
    final double[] baseBuffer = sketch.getCombinedBuffer();
    final int oldSize = sketch.getCombinedBufferItemCapacity();
    final int k = sketch.getK();
    assert oldSize < 2 * k;
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.combinedBufferItemCapacity_ = newSize;
    sketch.combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   * 
   * @param src The source sketch
   * @param tgt The target sketch
   */
  static void downSamplingMergeInto(final HeapDoublesSketch src, final HeapDoublesSketch tgt) {
    final int targetK = tgt.getK();
    final int sourceK = src.getK();

    if ((sourceK % targetK) != 0) {
      throw new SketchesArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    final int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    final double[] sourceLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    final double[] sourceBaseBuffer = src.getCombinedBuffer(); // aliasing is a bit dangerous

    final long nFinal = tgt.getN() + src.getN();

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(sourceBaseBuffer[i]);
    }

    maybeGrowLevels(nFinal, tgt); 

    final double[] scratchBuf = new double [2 * targetK];
    final double[] downBuf    = new double [targetK];

    long srcBitPattern = src.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        DoublesUtil.justZipWithStride(
            sourceLevels, ((2 + srcLvl) * sourceK),
            downBuf, 0,
            targetK,
            downFactor);
        inPlacePropagateCarry(
            srcLvl + lgDownFactor,
            downBuf, 0,
            scratchBuf, 0,
            false, tgt);
        // won't update target.n_ until the very end
      }
    }
    tgt.n_ = nFinal; 

    assert tgt.getN() / (2 * targetK) == tgt.getBitPattern(); // internal consistency check

    final double srcMax = src.getMaxValue();
    final double srcMin = src.getMinValue();
    final double tgtMax = tgt.getMaxValue();
    final double tgtMin = tgt.getMinValue();

    if (srcMax > tgtMax) tgt.maxValue_ = srcMax;
    if (srcMin < tgtMin) tgt.minValue_ = srcMin;
  }

  private static void zipSize2KBuffer(
      final double[] bufA, final int startA, // input
      final double[] bufC, final int startC, // output
      final int k) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  private static void justZipWithStride(
      final double[] bufA, final int startA, // input
      final double[] bufC, final int startC, // output
      final int kC, // number of items that should be in the output
      final int stride) {
    final int randomOffset = DoublesSketch.rand.nextInt(stride);
    final int limC = startC + kC;
    for (int a = startA + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufA[a];
    }
  }

  private static void mergeTwoSizeKBuffers(
      final double[] keySrc1, final int arrStart1,
      final double[] keySrc2, final int arrStart2,
      final double[] keyDst,  final int arrStart3,
      final int k) {
    final int arrStop1 = arrStart1 + k;
    final int arrStop2 = arrStart2 + k;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc2[i2] < keySrc1[i1]) { 
        keyDst[i3++] = keySrc2[i2++];
      } else { 
        keyDst[i3++] = keySrc1[i1++];
      } 
    }
  
    if (i1 < arrStop1) {
      System.arraycopy(keySrc1, i1, keyDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      System.arraycopy(keySrc1, i2, keyDst, i3, arrStop2 - i2);
    }
  }

  /**
   * Because of the nested loop, cost is O(numSamples * numSplitPoints), which is bilinear.
   * This method does NOT require the samples to be sorted.
   * @param samples array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 == counters.length.
   * @param counters array of counters
   */
  static void bilinearTimeIncrementHistogramCounters(final double[] samples, final int offset, final int numSamples,
      final long weight, final double[] splitPoints, final long[] counters) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) { 
      final double sample = samples[i + offset];
      int j = 0;
      for (j = 0; j < splitPoints.length; j++) {
        final double splitpoint = splitPoints[j];
        if (sample < splitpoint) {
          break;
        }
      }
      assert j < counters.length;
      counters[j] += weight;
    }
  }

  /**
   * This one does a linear time simultaneous walk of the samples and splitPoints. Because this
   * internal procedure is called multiple times, we require the caller to ensure these 3 properties:
   * <ol>
   * <li>samples array must be sorted.</li>
   * <li>splitPoints must be unique and sorted</li>
   * <li>number of SplitPoints + 1 == counters.length</li>
   * </ol>
   * @param samples sorted array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints + 1 = counters.length.
   * @param counters array of counters
   */
  static void linearTimeIncrementHistogramCounters(final double[] samples, final int offset, final int numSamples, 
      final long weight, final double[] splitPoints, final long[] counters) {
    int i = 0;
    int j = 0;
    while (i < numSamples && j < splitPoints.length) {
      if (samples[i + offset] < splitPoints[j]) {
        counters[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      } else {
        j++; // no more samples for this bucket. move on the next bucket.
      }
    }

    // now either i == numSamples(we are out of samples), or
    // j == numSplitPoints(out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case.
    if (j == splitPoints.length) {
      counters[j] += (weight * (numSamples - i));
    }
  }

  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed above.
   * @param keyArr array of keys
   * @param valArr array of values
   * @param arrLen length of keyArr and valArr 
   * @param blkSize size of internal sorted blocks
   */
  static void blockyTandemMergeSort(final double[] keyArr, final long[] valArr, final int arrLen, final int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) return;
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) numblks += 1;
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy. 
    final double[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    final long[] valTmp   = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
                                   0, numblks,
                                   blkSize, arrLen);
  }

  /**
   *  blockyTandemMergeSortRecursion() is called by blockyTandemMergeSort().
   *  In addition to performing the algorithm's top down recursion,
   *  it manages the buffer swapping that eliminates most copying.
   *  It also maps the input's pre-sorted blocks into the subarrays 
   *  that are processed by tandemMerge().
   * @param keySrc key source
   * @param valSrc value source
   * @param keyDst key destination
   * @param valDst value destination
   * @param grpStart group start, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param grpLen group length, refers to pre-sorted blocks such as block 0, block 1, etc.
   * @param blkSize block size
   * @param arrLim array limit
   */
  private static void blockyTandemMergeSortRecursion(final double[] keySrc, final long[] valSrc,
      final double[] keyDst, final long[] valDst, final int grpStart, final int grpLen, /* indices of blocks */
      final int blkSize, final int arrLim) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) return;
    final int grpLen1 = grpLen / 2;
    final int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    final int grpStart1 = grpStart;
    final int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2         = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if (arrStart2 + arrLen2 > arrLim) arrLen2 = arrLim - arrStart2;

    tandemMerge(keySrc, valSrc,
                arrStart1, arrLen1, 
                arrStart2, arrLen2,
                keyDst, valDst,
                arrStart1); // which will be arrStart3
  }

  /**
   *  Performs two merges in tandem. One of them provides the sort keys
   *  while the other one passively undergoes the same data motion.
   * @param keySrc key source
   * @param valSrc value source
   * @param arrStart1 Array 1 start offset
   * @param arrLen1 Array 1 length
   * @param arrStart2 Array 2 start offset
   * @param arrLen2 Array 2 length
   * @param keyDst key destination
   * @param valDst value destination
   * @param arrStart3 Array 3 start offset
   */
  private static void tandemMerge(final double[] keySrc, final long[] valSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final double[] keyDst, final long[] valDst,
                                  final int arrStart3) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;
  
    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc[i2] < keySrc[i1]) { 
        keyDst[i3] = keySrc[i2];
        valDst[i3] = valSrc[i2];
        i3++; i2++;
      } else { 
        keyDst[i3] = keySrc[i1];
        valDst[i3] = valSrc[i1];
        i3++; i1++;
      }
    }
  
    if (i1 < arrStop1) {
      arraycopy(keySrc, i1, keyDst, i3, arrStop1 - i1);
      arraycopy(valSrc, i1, valDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      arraycopy(keySrc, i2, keyDst, i3, arrStop2 - i2);
      arraycopy(valSrc, i2, valDst, i3, arrStop2 - i2);
    }
  }

  static String toString(final boolean sketchSummary, final boolean dataDetail, final HeapDoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    if (dataDetail) {
      sb.append(getDataDetail(sketch));
    }
    if (sketchSummary) {
      sb.append(getSummary(sketch));
    }
    return sb.toString();
  }
  
  static String getDataDetail(final HeapDoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    sb.append(LS).append("### ").append(thisSimpleName).append(" DATA DETAIL: ").append(LS);
    
    final int k = sketch.getK();
    final long n = sketch.getN();
    final int bbCount = sketch.getBaseBufferCount();
    final long bitPattern = sketch.getBitPattern();
    final double[] combBuf  = sketch.getCombinedBuffer();
    
    //output the base buffer
    
    sb.append("   BaseBuffer   : ");
    for (int i = 0; i < bbCount; i++) { 
      sb.append(String.format("%10.1f", combBuf[i]));
    }
    sb.append(LS);
    
    //output all the levels
    int combBufSize = combBuf.length;
    if (n >= 2 * k) {
      sb.append("   Valid | Level");
      for (int j = 2 * k; j < combBufSize; j++) { //output level data starting at 2K
        if (j % k == 0) { //start output of new level
          final int levelNum = j / k - 2;
          final String validLvl = ((1L << levelNum) & bitPattern) > 0 ? "    T  " : "    F  ";
          final String lvl = String.format("%5d", levelNum);
          sb.append(Util.LS).append("   ").append(validLvl).append(" ").append(lvl).append(": ");
        }
        sb.append(String.format("%10.1f", combBuf[j]));
      }
      sb.append(LS);
    }
    sb.append("### END DATA DETAIL").append(LS);
    return sb.toString();
  }
  
  static String getSummary(final HeapDoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    final int k = sketch.getK();
    final long n = sketch.getN();
    final String nStr = String.format("%,d", n);
    final int bbCount = sketch.getBaseBufferCount();
    final long bitPattern = sketch.getBitPattern();
    final int totLevels = Util.computeNumLevelsNeeded(k, n);
    final int validLevels = Util.computeValidLevels(bitPattern);
    final boolean empty = sketch.isEmpty();
    final int preBytes = empty ? Long.BYTES : 2 * Long.BYTES;
    final int retItems = sketch.getRetainedItems();
    final String retItemsStr = String.format("%,d", retItems);
    final int bytes = preBytes + (retItems + 2) * Double.BYTES;
    final double eps = Util.EpsilonFromK.getAdjustedEpsilon(k);
    final String epsPct = String.format("%.3f%%", eps * 100.0);

    sb.append(Util.LS).append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   K                            : ").append(k).append(LS);
    sb.append("   N                            : ").append(nStr).append(LS);
    sb.append("   Levels (Total, Valid)        : ").append(totLevels + ", " + validLevels).append(LS);
    sb.append("   Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern)).append(LS);
    sb.append("   BaseBufferCount              : ").append(bbCount).append(LS);
    sb.append("   Retained Items               : ").append(retItemsStr).append(LS);
    sb.append("   Storage Bytes                : ").append(String.format("%,d", bytes)).append(LS);
    sb.append("   Normalized Rank Error        : ").append(epsPct).append(LS);
    sb.append("   Min Value                    : ").append(String.format("%,.3f", sketch.getMinValue())).append(LS);
    sb.append("   Max Value                    : ").append(String.format("%,.3f", sketch.getMaxValue())).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }
  
  static String printMemData(Memory mem, int k, int n) {
    if (n == 0) return "";
    final StringBuilder sb = new StringBuilder();
    sb.append(LS).append("### ").append("MEM DATA DETAIL:").append(LS);
    String fmt1 = "%n%10.1f, ";
    String fmt2 = "%10.1f, ";
    int bbCount = Util.computeBaseBufferItems(k, n);
    int ret = Util.computeRetainedItems(k, n);
    sb.append("BaseBuffer Data:");
    for (int i = 0; i < bbCount; i++) {
      double d = mem.getDouble(32 + i * 8);
      if (i % k != 0) sb.append(String.format(fmt2, d));
      else sb.append(String.format(fmt1, d));
    }
    sb.append(LS + LS + "Level Data:");
    for (int i = 0; i < ret - bbCount; i++) {
      double d = mem.getDouble(32 + i * 8 + bbCount * 8);
      if (i % k != 0) sb.append(String.format(fmt2, d));
      else sb.append(String.format(fmt1, d));
    }
    sb.append(LS + "### END DATA DETAIL").append(LS);
    return sb.toString();
  }
  
}
