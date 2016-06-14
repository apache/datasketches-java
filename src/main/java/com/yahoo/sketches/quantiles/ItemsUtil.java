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
 * Utility class for quantiles sketches.
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
  static final <T> void validateValues(T[] values, Comparator<? super T> comparator) {
    int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if (comparator.compare(values[j], values[j+1]) < 0) continue;
      throw new IllegalArgumentException(
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
  @SuppressWarnings("unchecked")
  static <T> long[] internalBuildHistogram(T[] splitPoints, ItemsQuantilesSketch<T> sketch) {
    Object[] levelsArr  = sketch.getCombinedBuffer(); // aliasing is a bit dangerous
    Object[] baseBuffer = levelsArr;                  // aliasing is a bit dangerous
    int bbCount = sketch.getBaseBufferCount();
    ItemsUtil.validateValues(splitPoints, sketch.getComparator());
  
    int numSplitPoints = splitPoints.length;
    int numCounters = numSplitPoints + 1;
    long[] counters = new long[numCounters];
  
    //may need this off-heap
    //for (int j = 0; j < numCounters; j++) { counters[j] = 0; } 
  
    long weight = 1;
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      bilinearTimeIncrementHistogramCounters(
          (T[]) baseBuffer, 0, bbCount, weight, splitPoints, counters, sketch.getComparator());
    }
    else {
      Arrays.sort(baseBuffer, 0, bbCount);
      // sort is worth it when many split points
      linearTimeIncrementHistogramCounters(
          (T[]) baseBuffer, 0, bbCount, weight, splitPoints, counters, sketch.getComparator());
    }
  
    long myBitPattern = sketch.getBitPattern();
    int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight += weight; // *= 2
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        linearTimeIncrementHistogramCounters(
            (T[]) levelsArr, (2+lvl)*k, k, weight, splitPoints, counters, sketch.getComparator());
      }
    }
    return counters;
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param sketch the given quantiles sketch
   */
  @SuppressWarnings("unchecked")
  static <T> void processFullBaseBuffer(ItemsQuantilesSketch<T> sketch) {
    int bbCount = sketch.getBaseBufferCount();
    long n = sketch.getN();
    assert bbCount == 2 * sketch.getK();  // internal consistency check
  
    // make sure there will be enough levels for the propagation
    maybeGrowLevels(n, sketch); // important: n_ was incremented by update before we got here
  
    // this aliasing is a bit dangerous; notice that we did it after the possible resizing
    Object[] baseBuffer = sketch.getCombinedBuffer(); 
  
    Arrays.sort(baseBuffer, 0, bbCount);
    inPlacePropagateCarry(
        0,
        null, 0,  // this null is okay
        (T[]) baseBuffer, 0,
        true, sketch);
    sketch.baseBufferCount_ = 0;
    Arrays.fill(baseBuffer, 0, 2 * sketch.getK(), null); // to release the discarded objects
    assert n / (2*sketch.getK()) == sketch.getBitPattern();  // internal consistency check
  }

  @SuppressWarnings("unchecked")
  static <T> void inPlacePropagateCarry(
      int startingLevel,
      T[] sizeKBuf, int sizeKStart,
      T[] size2KBuf, int size2KStart,
      boolean doUpdateVersion, ItemsQuantilesSketch<T> sketch) { // else doMergeIntoVersion
    Object[] levelsArr = sketch.getCombinedBuffer();
    long bitPattern = sketch.getBitPattern();
    int k = sketch.getK();
  
    int endingLevel = Util.positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);
    //    assert endingLevel < levelsAllocated(); // was an internal consistency check
  
    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKbuf to be null in this case
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, ((2+endingLevel) * k),
          k);
    }
    else { // mergeInto version of computation
      System.arraycopy(
          sizeKBuf, sizeKStart,
          levelsArr, ((2+endingLevel) * k),
          k);
    }
  
    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0;  // internal consistency check
      mergeTwoSizeKBuffers(
          (T[]) levelsArr, ((2+lvl) * k),
          (T[]) levelsArr, ((2+endingLevel) * k),
          size2KBuf, size2KStart,
          k, sketch.getComparator());
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, ((2+endingLevel) * k),
          k);
      // to release the discarded objects
      Arrays.fill(levelsArr, ((2+lvl) * k), ((2+lvl+1) * k), null);
    } // end of loop over lower levels
  
    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (((long) 1) << startingLevel);
  }

  static <T> void maybeGrowLevels(long newN, ItemsQuantilesSketch<T> sketch) { // important: newN might not equal n_
    int k = sketch.getK();
    int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      return; // don't need any levels yet, and might have small base buffer; this can happen during a merge
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0; 
    int spaceNeeded = (2 + numLevelsNeeded) * k;
    if (spaceNeeded <= sketch.getCombinedBufferAllocatedCount()) {
      return;
    }
    // copies base buffer plus old levels
    Object[] newCombinedBuffer = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded); 

    sketch.combinedBufferAllocatedCount_ = spaceNeeded;
    sketch.combinedBuffer_ = newCombinedBuffer;
  }

  static <T> void growBaseBuffer(ItemsQuantilesSketch<T> sketch) {
    Object[] baseBuffer = sketch.getCombinedBuffer();
    int oldSize = sketch.getCombinedBufferAllocatedCount();
    int k = sketch.getK();
    assert oldSize < 2 * k;
    int newSize = Math.max(Math.min(2*k, 2*oldSize), 1);
    sketch.combinedBufferAllocatedCount_ = newSize;
    // Arrays.copyOf() fills the remainder with nulls
    Object[] newBuf = Arrays.copyOf(baseBuffer, newSize);
    sketch.combinedBuffer_ = newBuf;
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
  static <T> void downSamplingMergeInto(ItemsQuantilesSketch<T> src, ItemsQuantilesSketch<T> tgt) {
    int targetK = tgt.getK();
    int sourceK = src.getK();

    if ((sourceK % targetK) != 0) {
      throw new IllegalArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }

    int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);

    Object[] sourceLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    Object[] sourceBaseBuffer = src.getCombinedBuffer(); // aliasing is a bit dangerous

    long nFinal = tgt.getN() + src.getN();

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update((T) sourceBaseBuffer[i]);
    }

    ItemsUtil.maybeGrowLevels(nFinal, tgt); 

    Object[] scratchBuf = new Object[2 * targetK];
    Object[] downBuf    = new Object[targetK];

    long srcBitPattern = src.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        justZipWithStride (
            sourceLevels, ((2+srcLvl) * sourceK),
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

    T srcMax = src.getMaxValue();
    T srcMin = src.getMinValue();
    T tgtMax = tgt.getMaxValue();
    T tgtMin = tgt.getMinValue();

    if (src.getComparator().compare(srcMax, tgtMax) > 0) { tgt.maxValue_ = srcMax; }
    if (src.getComparator().compare(srcMin,tgtMin) < 0) { tgt.minValue_ = srcMin; }
  }

  static void zipSize2KBuffer(
      Object[] bufA, int startA, // input
      Object[] bufC, int startC, // output
      int k) {
    int randomOffset = ItemsQuantilesSketch.rand.nextBoolean() ? 1 : 0;
    int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  static <T> void justZipWithStride(
      T[] bufSrc, int startSrc, // input
      T[] bufC, int startC, // output
      int kC, // number of items that should be in the output
      int stride) {
    int randomOffset = (ItemsQuantilesSketch.rand.nextInt(stride));
    int limC = startC + kC;
  
    for (int a = startSrc + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufSrc[a];
    }
  }

  static <T> void mergeTwoSizeKBuffers(
      T[] keySrc1, int startSrc1,
      T[] keySrc2, int arrStart2,
      T[] keyDst,  int arrStart3,
      int k, Comparator<? super T> comparator) {
    int arrStop1 = startSrc1 + k;
    int arrStop2 = arrStart2 + k;
  
    int i1 = startSrc1;
    int i2 = arrStart2;
    int i3 = arrStart3;
  
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (comparator.compare(keySrc2[i2], keySrc1[i1]) < 0) { 
        keyDst[i3++] = keySrc2[i2++];
      }     
      else { 
        keyDst[i3++] = keySrc1[i1++];
      } 
    }
  
    if (i1 < arrStop1) {
      System.arraycopy(keySrc1, i1, keyDst, i3, arrStop1 - i1);
    }
    else {
      assert i2 < arrStop2;
      System.arraycopy(keySrc1, i2, keyDst, i3, arrStop2 - i2);
    }
  }

  static boolean sameStructurePredicate( HeapDoublesQuantilesSketch mq1, HeapDoublesQuantilesSketch mq2) {
    return (
            (mq1.getK() == mq2.getK()) &&
            (mq1.getN() == mq2.getN()) &&
            (mq1.getCombinedBufferAllocatedCount() == mq2.getCombinedBufferAllocatedCount()) &&
            (mq1.getBaseBufferCount() == mq2.getBaseBufferCount()) &&
            (mq1.getBitPattern() == mq2.getBitPattern()) &&
            (mq1.getMinValue() == mq2.getMinValue()) &&
            (mq1.getMaxValue() == mq2.getMaxValue())
           );
  }

  /**
   * Because of the nested loop, cost is O(numSamples * numSplitPoints), which is bilinear.
   * This method does NOT require the samples to be sorted.
   * @param samples array of samples
   * @param offset into samples array
   * @param numSamples number of samples in samples array
   * @param weight of the samples
   * @param splitPoints must be unique and sorted. Number of splitPoints +1 == counters.length.
   * @param counters array of counters
   */ //used by HeapQS and MQS
  static <T> void bilinearTimeIncrementHistogramCounters(T[] samples, int offset, int numSamples, 
      long weight, T[] splitPoints, long[] counters, Comparator<? super T> comparator) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) { 
      T sample = samples[i+offset];
      int j = 0;

      for (j = 0; j < splitPoints.length; j++) {
        T splitpoint = splitPoints[j];
        if (comparator.compare(sample, splitpoint) < 0) { 
          break;
        }
      }
      assert j < counters.length;
      // System.out.printf("%.2f in bucket %d\n", sample, j);
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
   * @param splitPoints must be unique and sorted. Number of splitPoints +1 = counters.length.
   * @param counters array of counters
   */ //used by HeapQS and MQS
  static <T> void linearTimeIncrementHistogramCounters(T[] samples, int offset, int numSamples, 
      long weight, T[] splitPoints, long[] counters, Comparator<? super T> comparator) {
    int numSplitPoints = splitPoints.length;

    int i = 0;
    int j = 0;

    while (i < numSamples && j < numSplitPoints) {
      if (comparator.compare(samples[i+offset], splitPoints[j]) < 0) {
        counters[j] += weight; // this sample goes into this bucket
        i++; // move on to next sample and see whether it also goes into this bucket
      }
      else {
        j++; // no more samples for this bucket. move on the next bucket.
      }
    }

    // now either i == numSamples(we are out of samples), or
    // j == numSplitPoints(out of buckets, but there are more samples remaining)
    // we only need to do something in the latter case.
    if (j == numSplitPoints) {
      counters[numSplitPoints] += (weight * (numSamples - i));
    }
  }
  
  //****************************************************
  /**
   * blockyTandemMergeSort() is an implementation of top-down merge sort specialized
   * for the case where the input contains successive equal-length blocks
   * that have already been sorted, so that only the top part of the
   * merge tree remains to be executed. Also, two arrays are sorted in tandem,
   * as discussed above. Used by Aux constructors for both Heap QS and MQS.
   * @param keyArr array of keys
   * @param valArr array of values
   * @param arrLen length of keyArr and valArr 
   * @param blkSize size of internal sorted blocks
   */ //used by Auxiliary, so far
  static <T> void blockyTandemMergeSort(T[] keyArr, long[] valArr, int arrLen, int blkSize, Comparator<? super T> comparator) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) return;
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) numblks += 1;
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy. 
    T[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    long[] valTmp   = Arrays.copyOf(valArr, arrLen);

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
   */
  private static <T> void blockyTandemMergeSortRecursion(T[] keySrc, long[] valSrc,
      T[] keyDst, long[] valDst, int grpStart, int grpLen, /* indices of blocks */
      int blkSize, int arrLim, Comparator<? super T> comparator) {
    // Important note: grpStart and grpLen do NOT refer to positions in the underlying array.
    // Instead, they refer to the pre-sorted blocks, such as block 0, block 1, etc.

    assert (grpLen > 0);
    if (grpLen == 1) return;
    int grpLen1 = grpLen / 2;
    int grpLen2 = grpLen - grpLen1;
    assert (grpLen1 >= 1);
    assert (grpLen2 >= grpLen1);

    int grpStart1 = grpStart;
    int grpStart2 = grpStart + grpLen1;

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart1, grpLen1, blkSize, arrLim, comparator);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim, comparator);

    // here we convert indices of blocks into positions in the underlying array.
    int arrStart1 = grpStart1 * blkSize;
    int arrStart2 = grpStart2 * blkSize;
    int arrLen1   = grpLen1   * blkSize;
    int arrLen2   = grpLen2   * blkSize;

    // special code for the final block which might be shorter than blkSize.
    if (arrStart2 + arrLen2 > arrLim) { arrLen2 = arrLim - arrStart2; } 
 
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
   */
  private static <T> void tandemMerge(T[] keySrc, long[] valSrc,
                                  int arrStart1, int arrLen1,
                                  int arrStart2, int arrLen2,
                                  T[] keyDst, long[] valDst,
                                  int arrStart3, Comparator<? super T> comparator) {
    int arrStop1 = arrStart1 + arrLen1;
    int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;

    while (i1 < arrStop1 && i2 < arrStop2) {
      if (comparator.compare(keySrc[i2], keySrc[i1]) < 0) {
        keyDst[i3] = keySrc[i2];
        valDst[i3] = valSrc[i2];
        i3++; i2++;
      }     
      else { 
        keyDst[i3] = keySrc[i1];
        valDst[i3] = valSrc[i1];
        i3++; i1++;
      } 
    }

    if (i1 < arrStop1) {
      arraycopy(keySrc, i1, keyDst, i3, arrStop1 - i1);
      arraycopy(valSrc, i1, valDst, i3, arrStop1 - i1);
    }
    else {
      assert i2 < arrStop2;
      arraycopy(keySrc, i2, keyDst, i3, arrStop2 - i2);
      arraycopy(valSrc, i2, valDst, i3, arrStop2 - i2);
    }
  }

}
