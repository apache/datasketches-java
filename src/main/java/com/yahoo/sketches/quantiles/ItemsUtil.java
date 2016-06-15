/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static java.lang.System.arraycopy;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Utility class for generic quantiles sketch.
 * 
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 */
final class ItemsUtil {

  /**
   * Checks the sequential validity of the given array of values. 
   * They must be unique, monotonically increasing and not null.
   * @param values given array of values
   */
  static final <T> void validateValues(final T[] values, final Comparator<? super T> comparator) {
    final int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if (values[j] != null && values[j + 1] != null &&
          comparator.compare(values[j], values[j + 1]) < 0) continue;
      throw new IllegalArgumentException(
          "Values must be unique, monotonically increasing and not null.");
    }
  }

  /**
   * Shared algorithm for both PMF and CDF functions. The splitPoints must be unique, monotonically
   * increasing values.
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing values
   * that divide the ordered domain into <i>m+1</i> consecutive disjoint intervals.
   * @param sketch the given quantiles sketch
   * @return the unnormalized, accumulated counts of <i>m + 1</i> intervals.
   */
  @SuppressWarnings("unchecked")
  static <T> long[] internalBuildHistogram(final T[] splitPoints, ItemsQuantilesSketch<T> sketch) {
    final Object[] levelsArr  = sketch.getCombinedBuffer();
    final Object[] baseBuffer = levelsArr;
    final int bbCount = sketch.getBaseBufferCount();
    validateValues(splitPoints, sketch.getComparator());

    final int numSplitPoints = splitPoints.length;
    final int numCounters = numSplitPoints + 1;
    final long[] counters = new long[numCounters];

    long weight = 1;
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      bilinearTimeIncrementHistogramCounters(
          (T[]) baseBuffer, 0, bbCount, weight, splitPoints, counters, sketch.getComparator());
    } else {
      Arrays.sort(baseBuffer, 0, bbCount);
      // sort is worth it when many split points
      linearTimeIncrementHistogramCounters(
          (T[]) baseBuffer, 0, bbCount, weight, splitPoints, counters, sketch.getComparator());
    }

    long myBitPattern = sketch.getBitPattern();
    final int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight += weight; // *= 2
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        linearTimeIncrementHistogramCounters(
            (T[]) levelsArr, (2 + lvl) * k, k, weight, splitPoints, counters, sketch.getComparator());
      }
    }
    return counters;
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param sketch the given quantiles sketch
   */
  @SuppressWarnings("unchecked")
  static <T> void processFullBaseBuffer(final ItemsQuantilesSketch<T> sketch) {
    final int bbCount = sketch.getBaseBufferCount();
    final long n = sketch.getN();
    assert bbCount == 2 * sketch.getK(); // internal consistency check

    // make sure there will be enough levels for the propagation
    maybeGrowLevels(n, sketch); // important: n_ was incremented by update before we got here

    // this aliasing is a bit dangerous; notice that we did it after the possible resizing
    final Object[] baseBuffer = sketch.getCombinedBuffer(); 

    Arrays.sort(baseBuffer, 0, bbCount);
    inPlacePropagateCarry(
        0,
        null, 0,  // this null is okay
        (T[]) baseBuffer, 0,
        true, sketch);
    sketch.baseBufferCount_ = 0;
    Arrays.fill(baseBuffer, 0, 2 * sketch.getK(), null); // to release the discarded objects
    assert n / (2 * sketch.getK()) == sketch.getBitPattern();  // internal consistency check
  }

  @SuppressWarnings("unchecked")
  static <T> void inPlacePropagateCarry(
      final int startingLevel,
      final T[] sizeKBuf, final int sizeKStart,
      final T[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion, final ItemsQuantilesSketch<T> sketch) { // else doMergeIntoVersion
    final Object[] levelsArr = sketch.getCombinedBuffer();
    final long bitPattern = sketch.getBitPattern();
    final int k = sketch.getK();

    final int endingLevel = Util.positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);

    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKbuf to be null in this case
      zipSize2KBuffer(
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
      mergeTwoSizeKBuffers(
          (T[]) levelsArr, (2+lvl) * k,
          (T[]) levelsArr, (2+endingLevel) * k,
          size2KBuf, size2KStart,
          k, sketch.getComparator());
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2+endingLevel) * k,
          k);
      // to release the discarded objects
      Arrays.fill(levelsArr, (2 + lvl) * k, (2 + lvl + 1) * k, null);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (1L << startingLevel);
  }

  static <T> void maybeGrowLevels(final long newN, final ItemsQuantilesSketch<T> sketch) { // important: newN might not equal n_
    final int k = sketch.getK();
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      return; // don't need any levels yet, and might have small base buffer; this can happen during a merge
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0; 
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    if (spaceNeeded <= sketch.getCombinedBufferAllocatedCount()) {
      return;
    }
    // copies base buffer plus old levels
    sketch.combinedBuffer_ = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded); 
    sketch.combinedBufferAllocatedCount_ = spaceNeeded;
  }

  static <T> void growBaseBuffer(final ItemsQuantilesSketch<T> sketch) {
    final Object[] baseBuffer = sketch.getCombinedBuffer();
    final int oldSize = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    assert oldSize < 2 * k;
    final int newSize = Math.max(Math.min(2 * k, 2 * oldSize), 1);
    sketch.combinedBufferAllocatedCount_ = newSize;
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
  @SuppressWarnings("unchecked")
  static <T> void downSamplingMergeInto(final ItemsQuantilesSketch<T> src, final ItemsQuantilesSketch<T> tgt) {
    final int targetK = tgt.getK();
    final int sourceK = src.getK();

    if ((sourceK % targetK) != 0) {
      throw new IllegalArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    final int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    final Object[] sourceLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    final Object[] sourceBaseBuffer = src.getCombinedBuffer(); // aliasing is a bit dangerous

    final long nFinal = tgt.getN() + src.getN();

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update((T) sourceBaseBuffer[i]);
    }

    maybeGrowLevels(nFinal, tgt);

    final Object[] scratchBuf = new Object[2 * targetK];
    final Object[] downBuf    = new Object[targetK];

    long srcBitPattern = src.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        justZipWithStride (
            sourceLevels, (2 + srcLvl) * sourceK,
            downBuf, 0,
            targetK,
            downFactor);
        inPlacePropagateCarry (
            srcLvl+lgDownFactor,
            (T[]) downBuf, 0,
            (T[]) scratchBuf, 0,
            false, tgt);
        // won't update target.n_ until the very end
      }
    }
    tgt.n_ = nFinal; 

    assert tgt.getN() / (2*targetK) == tgt.getBitPattern(); // internal consistency check

    final T srcMax = src.getMaxValue();
    final T srcMin = src.getMinValue();
    final T tgtMax = tgt.getMaxValue();
    final T tgtMin = tgt.getMinValue();

    if (src.getComparator().compare(srcMax, tgtMax) > 0) tgt.maxValue_ = srcMax;
    if (src.getComparator().compare(srcMin, tgtMin) < 0) tgt.minValue_ = srcMin;
  }

  private static void zipSize2KBuffer(
      final Object[] bufA, int startA, // input
      final Object[] bufC, int startC, // output
      final int k) {
    int randomOffset = ItemsQuantilesSketch.rand.nextBoolean() ? 1 : 0;
    int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  private static <T> void justZipWithStride(
      final T[] bufSrc, final int startSrc, // input
      final T[] bufC, final int startC, // output
      final int kC, // number of items that should be in the output
      final int stride) {
    final int randomOffset = ItemsQuantilesSketch.rand.nextInt(stride);
    final int limC = startC + kC;
    for (int a = startSrc + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufSrc[a];
    }
  }

  private static <T> void mergeTwoSizeKBuffers(
      final T[] keySrc1, final int startSrc1,
      final T[] keySrc2, final int arrStart2,
      final T[] keyDst,  final int arrStart3,
      final int k, final Comparator<? super T> comparator) {
    final int arrStop1 = startSrc1 + k;
    final int arrStop2 = arrStart2 + k;
  
    int i1 = startSrc1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (comparator.compare(keySrc2[i2], keySrc1[i1]) < 0) { 
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
  static <T> void bilinearTimeIncrementHistogramCounters(final T[] samples, final int offset, final int numSamples,
      final long weight, final T[] splitPoints, final long[] counters, final Comparator<? super T> comparator) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) { 
      final T sample = samples[i + offset];
      int j = 0;
      for (j = 0; j < splitPoints.length; j++) {
        final T splitpoint = splitPoints[j];
        if (comparator.compare(sample, splitpoint) < 0) { 
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
  static <T> void linearTimeIncrementHistogramCounters(final T[] samples, final int offset, final int numSamples,
      final long weight, final T[] splitPoints, final long[] counters, final Comparator<? super T> comparator) {
    int i = 0;
    int j = 0;
    while (i < numSamples && j < splitPoints.length) {
      if (comparator.compare(samples[i+offset], splitPoints[j]) < 0) {
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
  static <T> void blockyTandemMergeSort(final T[] keyArr, final long[] valArr, final int arrLen, final int blkSize, final Comparator<? super T> comparator) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) return;
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) numblks += 1;
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy. 
    final T[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    final long[] valTmp = Arrays.copyOf(valArr, arrLen);

    blockyTandemMergeSortRecursion(keyTmp, valTmp,
                                   keyArr, valArr,
                                   0, numblks,
                                   blkSize, arrLen, comparator);
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
   * @param comparator to compare keys
   */
  private static <T> void blockyTandemMergeSortRecursion(final T[] keySrc, final long[] valSrc,
      final T[] keyDst, final long[] valDst, final int grpStart, final int grpLen, /* indices of blocks */
      final int blkSize, final int arrLim, final Comparator<? super T> comparator) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) return;
    int grpLen1 = grpLen / 2;
    int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    final int grpStart1 = grpStart;
    final int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim, comparator);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim, comparator);

    // here we convert indices of blocks into positions in the underlying array.
    final int arrStart1 = grpStart1 * blkSize;
    final int arrStart2 = grpStart2 * blkSize;
    final int arrLen1   = grpLen1   * blkSize;
    int arrLen2   = grpLen2   * blkSize;

    // special case for the final block which might be shorter than blkSize.
    if (arrStart2 + arrLen2 > arrLim) arrLen2 = arrLim - arrStart2;

    tandemMerge(keySrc, valSrc,
                arrStart1, arrLen1, 
                arrStart2, arrLen2,
                keyDst, valDst,
                arrStart1, comparator); // which will be arrStart3
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
   * @param comparator to compare keys
   */
  private static <T> void tandemMerge(final T[] keySrc, final long[] valSrc,
                                  final int arrStart1, final int arrLen1,
                                  final int arrStart2, final int arrLen2,
                                  final T[] keyDst, final long[] valDst,
                                  final int arrStart3, final Comparator<? super T> comparator) {
    final int arrStop1 = arrStart1 + arrLen1;
    final int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (comparator.compare(keySrc[i2], keySrc[i1]) < 0) {
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

  static <T> String toString(final boolean sketchSummary, final boolean dataDetail, final ItemsQuantilesSketch<T> sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    final int bbCount = sketch.getBaseBufferCount();
    final int combAllocCount = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    final long bitPattern = sketch.getBitPattern();

    if (dataDetail) {
      sb.append(Util.LS).append("### ").append(thisSimpleName).append(" DATA DETAIL: ").append(Util.LS);
      final Object[] items  = sketch.getCombinedBuffer();

      //output the base buffer
      sb.append("   BaseBuffer   :");
      if (bbCount > 0) {
        for (int i = 0; i < bbCount; i++) { 
          sb.append(' ').append(items[i]);
        }
      }
      sb.append(Util.LS);
      //output all the levels
      final int numItems = combAllocCount;
      if (numItems > 2 * k) {
        sb.append("   Valid | Level");
        for (int j = 2 * k; j < numItems; j++) { //output level data starting at 2K
          if (j % k == 0) { //start output of new level
            final int levelNum = j > 2 * k ? (j - 2 * k) / k : 0;
            final String validLvl = ((1L << levelNum) & bitPattern) > 0 ? "    T  " : "    F  ";
            final String lvl = String.format("%5d", levelNum);
            sb.append(Util.LS).append("   ").append(validLvl).append(" ").append(lvl).append(":");
          }
          sb.append(' ').append(items[j]);
        }
        sb.append(Util.LS);
      }
      sb.append("### END DATA DETAIL").append(Util.LS);
    }

    if (sketchSummary) {
      final long n = sketch.getN();
      final String nStr = String.format("%,d", n);
      final int numLevels = Util.computeNumLevelsNeeded(k, n);
      final String bufCntStr = String.format("%,d", combAllocCount);
      final int preBytes = sketch.isEmpty() ? Long.BYTES : PreambleUtil.PREAMBLE_LONGS * Long.BYTES;
      final double eps = Util.EpsilonFromK.getAdjustedEpsilon(k);
      final String epsPct = String.format("%.3f%%", eps * 100.0);
      final int numSamples = sketch.getRetainedEntries();
      final String numSampStr = String.format("%,d", numSamples);
      sb.append(Util.LS).append("### ").append(thisSimpleName).append(" SUMMARY: ").append(Util.LS);
      sb.append("   K                            : ").append(k).append(Util.LS);
      sb.append("   N                            : ").append(nStr).append(Util.LS);
      sb.append("   BaseBufferCount              : ").append(bbCount).append(Util.LS);
      sb.append("   CombinedBufferAllocatedCount : ").append(bufCntStr).append(Util.LS);
      sb.append("   Total Levels                 : ").append(numLevels).append(Util.LS);
      sb.append("   Valid Levels                 : ").append(Util.numValidLevels(bitPattern)).append(Util.LS);
      sb.append("   Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern)).append(Util.LS);
      sb.append("   Valid Samples                : ").append(numSampStr).append(Util.LS);
      sb.append("   Preamble Bytes               : ").append(preBytes).append(Util.LS);
      sb.append("   Normalized Rank Error        : ").append(epsPct).append(Util.LS);
      sb.append("   Min Value                    : ").append(sketch.getMinValue()).append(Util.LS);
      sb.append("   Max Value                    : ").append(sketch.getMaxValue()).append(Util.LS);
      sb.append("### END SKETCH SUMMARY").append(Util.LS);
    }
    return sb.toString();
  }

}
