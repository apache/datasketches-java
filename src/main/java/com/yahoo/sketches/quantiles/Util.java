/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static java.lang.System.arraycopy;
import static com.yahoo.sketches.quantiles.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER;
import static com.yahoo.sketches.quantiles.QuantilesSketch.*;

import java.util.Arrays;

import com.yahoo.sketches.Family;

/**
 * Utility class for quantiles sketches.
 * 
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 */
final class Util {
  /**
   * The java line separator character as a String.
   */
  public static String LS = System.getProperty("line.separator");
  
  /**
   * The tab character
   */
  public static final char TAB = '\t';
  
  /**
   * Checks the validity of the given value k
   * @param k must be greater than or equal to 2 and less than 65536.
   */
  static void checkK(int k) {
    if ((k < 1) || (k > ((1 << 16)-1))) {
      throw new IllegalArgumentException("K must be >= 1 and < 65536");
    }
  }

  /**
   * Check the validity of the given serialization version
   * @param serVer the given serialization version
   */
  static void checkSerVer(int serVer) {
    if (serVer != SER_VER) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Serialization Version: "+serVer);
    }
  }

  /**
   * Checks the validity of the given family ID
   * @param familyID the given family ID
   */
  static void checkFamilyID(int familyID) {
    Family family = Family.idToFamily(familyID);
    if (!family.equals(Family.QUANTILES)) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
  }

  /**
   * Checks the validity of the memory buffer allocation and the memory capacity assuming
   * n and k.
   * @param k the given value of k
   * @param n the given value of n
   * @param memBufAlloc the memory buffer allocation
   * @param memCapBytes the memory capacity
   */
  static void checkBufAllocAndCap(int k, long n, int memBufAlloc, long memCapBytes) {
    int computedBufAlloc = bufferElementCapacity(k, n);
    if (memBufAlloc != computedBufAlloc) {
      throw new IllegalArgumentException("Possible corruption: Invalid Buffer Allocated Count: "
          + memBufAlloc +" != " +computedBufAlloc);
    }
    int maxPre = Family.QUANTILES.getMaxPreLongs();
    int reqBufBytes = (maxPre + memBufAlloc) << 3;
    if (memCapBytes < reqBufBytes) {
      throw new IllegalArgumentException("Possible corruption: Memory capacity too small: "+ 
          memCapBytes + " < "+ reqBufBytes);
    }
  }

  /**
   * Checks the consistency of the flag bits and the state of preambleLong and the memory
   * capacity and returns the empty state.
   * @param preambleLongs the size of preamble in longs 
   * @param flags the flags field
   * @param memCapBytes the memory capacity
   * @return the value of the empty state
   */
  static boolean checkPreLongsFlagsCap(int preambleLongs, int flags, long memCapBytes) {
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    int minPre = Family.QUANTILES.getMinPreLongs();
    int maxPre = Family.QUANTILES.getMaxPreLongs();
    boolean valid = ((preambleLongs == minPre) && empty) || ((preambleLongs == maxPre) && !empty);
    if (!valid) {
      throw new IllegalArgumentException(
          "Possible corruption: PreambleLongs inconsistent with empty state: " +preambleLongs);
    }
    checkFlags(flags);
    if (!empty && (memCapBytes < (maxPre<<3))) {
      throw new IllegalArgumentException(
          "Possible corruption: Insufficient capacity for preamble: " +memCapBytes);
    }
    return empty;
  }

  /**
   * Checks just the flags field of the preamble
   * @param flags the flags field
   */ //only used by checkPreLongsFlagsCap and test
  static void checkFlags(int flags) {
    int flagsMask = 
        ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new IllegalArgumentException(
         "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }
  }

  /**
   * Checks the sequential validity of the given array of fractions. 
   * They must be unique, monotonically increasing and not NaN, not &lt; 0 and not &gt; 1.0.
   * @param fractions array
   */
  static final void validateFractions(double[] fractions) {
    if (fractions == null) {
      throw new IllegalArgumentException("Fractions array may not be null.");
    }
    double flo = fractions[0];
    double fhi = fractions[fractions.length - 1];
    if ((flo < 0.0) || (fhi > 1.0)) {
      throw new IllegalArgumentException("A fraction cannot be less than zero or greater than 1.0");
    }
    validateValues(fractions);
  }

  /**
   * Checks the sequential validity of the given array of values. 
   * They must be unique, monotonically increasing and not NaN.
   * @param values given array of values
   */
  static final void validateValues(double[] values) {
    int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if (values[j] < values[j+1]) { continue; }
      throw new IllegalArgumentException(
          "Values must be unique, monotonically increasing and not NaN.");
    }
  }
  
  
  /**
   * Computes a checksum of all the samples in the sketch. Used in testing the Auxiliary
   * @param sketch the given quantiles sketch
   * @return a checksum of all the samples in the sketch
   */ //Used by test
  static double sumOfSamplesInSketch(HeapQuantilesSketch sketch) {
    double[] combinedBuffer = sketch.getCombinedBuffer();
    int bbCount = sketch.getBaseBufferCount();
    double total = sumOfDoublesInSubArray(combinedBuffer, 0, bbCount);
    long bits = sketch.getBitPattern();
    int k = sketch.getK();
    assert bits == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; bits != 0L; lvl++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        total += sumOfDoublesInSubArray(combinedBuffer, ((2+lvl) * k), k);
      }
    }
    return total;
  }

  /**
   * Shared algorithm for both PMF and CDF functions. The splitPoints must be unique, monotonically
   * increasing values.
   * @param splitPoints an array of <i>m</i> unique, monotonically increasing doubles
   * that divide the real number line into <i>m+1</i> consecutive disjoint intervals.
   * @param sketch the given quantiles sketch
   * @return the unnormalized, accumulated counts of <i>m + 1</i> intervals.
   */
  static long[] internalBuildHistogram(double[] splitPoints, HeapQuantilesSketch sketch) {
    double[] levelsArr  = sketch.getCombinedBuffer(); // aliasing is a bit dangerous
    double[] baseBuffer = levelsArr;                  // aliasing is a bit dangerous
    int bbCount = sketch.getBaseBufferCount();
    Util.validateValues(splitPoints);
  
    int numSplitPoints = splitPoints.length;
    int numCounters = numSplitPoints + 1;
    long[] counters = new long[numCounters];
  
    //may need this off-heap
    //for (int j = 0; j < numCounters; j++) { counters[j] = 0; } 
  
    long weight = 1;
    if (numSplitPoints < 50) { // empirically determined crossover
      // sort not worth it when few split points
      bilinearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    }
    else {
      Arrays.sort(baseBuffer, 0, bbCount);
      // sort is worth it when many split points
      linearTimeIncrementHistogramCounters(
          baseBuffer, 0, bbCount, weight, splitPoints, counters);
    }
  
    long myBitPattern = sketch.getBitPattern();
    int k = sketch.getK();
    assert myBitPattern == sketch.getN() / (2L * k); // internal consistency check
    for (int lvl = 0; myBitPattern != 0L; lvl++, myBitPattern >>>= 1) {
      weight += weight; // *= 2
      if ((myBitPattern & 1L) > 0L) { //valid level exists
        // the levels are already sorted so we can use the fast version
        linearTimeIncrementHistogramCounters(
            levelsArr, (2+lvl)*k, k, weight, splitPoints, counters);
      }
    }
    return counters;
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param sketch the given quantiles sketch
   */
  static void processFullBaseBuffer(HeapQuantilesSketch sketch) {
    int bbCount = sketch.getBaseBufferCount();
    long n = sketch.getN();
    assert bbCount == 2 * sketch.getK();  // internal consistency check
  
    // make sure there will be enough levels for the propagation
    maybeGrowLevels(n, sketch); // important: n_ was incremented by update before we got here
  
    // this aliasing is a bit dangerous; notice that we did it after the possible resizing
    double[] baseBuffer = sketch.getCombinedBuffer(); 
  
    Arrays.sort(baseBuffer, 0, bbCount);
    inPlacePropagateCarry(
        0,
        null, 0,  // this null is okay
        baseBuffer, 0,
        true, sketch);
    sketch.baseBufferCount_ = 0;
    // just while debugging
    //Arrays.fill(baseBuffer, 0, 2*k_, DUMMY_VALUE);
    assert n / (2*sketch.getK()) == sketch.getBitPattern();  // internal consistency check
  }

  static void inPlacePropagateCarry(
      int startingLevel,
      double[] sizeKBuf, int sizeKStart,
      double[] size2KBuf, int size2KStart,
      boolean doUpdateVersion, HeapQuantilesSketch sketch) { // else doMergeIntoVersion
    double[] levelsArr = sketch.getCombinedBuffer();
    long bitPattern = sketch.getBitPattern();
    int k = sketch.getK();
  
    int endingLevel = positionOfLowestZeroBitStartingAt(bitPattern, startingLevel);
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
          levelsArr, ((2+lvl) * k),
          levelsArr, ((2+endingLevel) * k),
          size2KBuf, size2KStart,
          k);
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, ((2+endingLevel) * k),
          k);
      // just while debugging
      //Arrays.fill(levelsArr, ((2+lvl) * k_), ((2+lvl+1) * k_), DUMMY_VALUE);
    } // end of loop over lower levels
  
    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (((long) 1) << startingLevel);
  }

  static void maybeGrowLevels(long newN, HeapQuantilesSketch sketch) { // important: newN might not equal n_
    int k = sketch.getK();
    int numLevelsNeeded = computeNumLevelsNeeded(k, newN);
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
    double[] newCombinedBuffer = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded); 
    //    just while debugging
    //for (int i = combinedBufferAllocatedCount_; i < spaceNeeded; i++) {
    //  newCombinedBuffer[i] = DUMMY_VALUE;
    //}
  
    sketch.combinedBufferAllocatedCount_ = spaceNeeded;
    sketch.combinedBuffer_ = newCombinedBuffer;
  }

  static void growBaseBuffer(HeapQuantilesSketch sketch) {
    double[] baseBuffer = sketch.getCombinedBuffer();
    int oldSize = sketch.getCombinedBufferAllocatedCount();
    int k = sketch.getK();
    assert oldSize < 2 * k;
    int newSize = Math.max(Math.min(2*k, 2*oldSize), 1);
    sketch.combinedBufferAllocatedCount_ = newSize;
    double[] newBuf = Arrays.copyOf(baseBuffer, newSize);
    // just while debugging
    //for (int i = oldSize; i < newSize; i++) {newBuf[i] = DUMMY_VALUE;}
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
  static void downSamplingMergeInto(HeapQuantilesSketch src, HeapQuantilesSketch tgt) {
    int targetK = tgt.getK();
    int sourceK = src.getK();
    
    if ((sourceK % targetK) != 0) {
      throw new IllegalArgumentException(
          "source.getK() must equal target.getK() * 2^(nonnegative integer).");
    }
    
    int downFactor = sourceK / targetK;
    checkIfPowerOf2(downFactor, "source.getK()/target.getK() ratio");
    int lgDownFactor = Integer.numberOfTrailingZeros(downFactor);
    
    double [] sourceLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    double [] sourceBaseBuffer = src.getCombinedBuffer(); // aliasing is a bit dangerous
  
    long nFinal = tgt.getN() + src.getN();
    
    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update (sourceBaseBuffer[i]);
    }
  
    Util.maybeGrowLevels (nFinal, tgt); 
  
    double [] scratchBuf = new double [2*targetK];
    double [] downBuf    = new double [targetK];
  
    long srcBitPattern = src.getBitPattern();
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        justZipWithStride (
            sourceLevels, ((2+srcLvl) * sourceK),
            downBuf, 0,
            targetK,
            downFactor);
        Util.inPlacePropagateCarry (
            srcLvl+lgDownFactor,
            downBuf, 0,
            scratchBuf, 0,
            false, tgt);
        // won't update target.n_ until the very end
      }
    }
    tgt.n_ = nFinal; 
    
    assert tgt.getN() / (2*targetK) == tgt.getBitPattern(); // internal consistency check
  
    double srcMax = src.getMaxValue();
    double srcMin = src.getMinValue();
    double tgtMax = tgt.getMaxValue();
    double tgtMin = tgt.getMinValue();
    
    if (srcMax > tgtMax) { tgt.maxValue_ = srcMax; }
    if (srcMin < tgtMin) { tgt.minValue_ = srcMin; }
    
  }

  static String toString(boolean sketchSummary, boolean dataDetail, HeapQuantilesSketch sketch) {
    StringBuilder sb = new StringBuilder();
    String thisSimpleName = sketch.getClass().getSimpleName();
    int bbCount = sketch.getBaseBufferCount();
    int combAllocCount = sketch.getCombinedBufferAllocatedCount();
    int k = sketch.getK();
    long bitPattern = sketch.getBitPattern();
    
    if (dataDetail) {
      sb.append(LS).append("### ").append(thisSimpleName).append(" DATA DETAIL: ").append(LS);
      double[] levelsArr  = sketch.getCombinedBuffer();
      double[] baseBuffer = sketch.getCombinedBuffer();
      
      //output the base buffer
      
      sb.append("   BaseBuffer   : ");
      if (bbCount > 0) {
        for (int i = 0; i < bbCount; i++) { 
          sb.append(String.format("%10.1f", baseBuffer[i]));
        }
      }
      sb.append(LS);
      //output all the levels
      
      int items = combAllocCount;
      if (items > 2*k) {
        sb.append("   Valid | Level");
        for (int j = 2*k; j < items; j++) { //output level data starting at 2K
          if (j % k == 0) { //start output of new level
            int levelNum = (j > 2*k)? ((j-2*k)/k): 0;
            String validLvl = (((1L << levelNum) & bitPattern) > 0L)? "    T  " : "    F  "; 
            String lvl = String.format("%5d",levelNum);
            sb.append(LS).append("   ").append(validLvl).append(" ").append(lvl).append(": ");
          }
          sb.append(String.format("%10.1f", levelsArr[j]));
        }
        sb.append(LS);
      }
      sb.append("### END DATA DETAIL").append(LS);
    }
    
    if (sketchSummary) {
      long n = sketch.getN();
      String nStr = String.format("%,d", n);
      int numLevels = computeNumLevelsNeeded(k, n);
      int bufBytes = combAllocCount * 8;
      String bufCntStr = String.format("%,d", combAllocCount);
      //includes k, n, min, max, preamble of 8.
      int preBytes = 4 + 8 + 8 + 8 + 8;
      double eps = EpsilonFromK.getAdjustedEpsilon(k);
      String epsPct = String.format("%.3f%%", eps * 100.0);
      int numSamples = sketch.getRetainedEntries();
      String numSampStr = String.format("%,d", numSamples);
      sb.append(LS).append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      sb.append("   K                            : ").append(k).append(LS);
      sb.append("   N                            : ").append(nStr).append(LS);
      sb.append("   Seed                         : ").append(sketch.getSeed()).append(LS);
      sb.append("   BaseBufferCount              : ").append(bbCount).append(LS);
      sb.append("   CombinedBufferAllocatedCount : ").append(bufCntStr).append(LS);
      sb.append("   Total Levels                 : ").append(numLevels).append(LS);
      sb.append("   Valid Levels                 : ").append(numValidLevels(bitPattern)).append(LS);
      sb.append("   Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern)).append(LS);
      sb.append("   Valid Samples                : ").append(numSampStr).append(LS);
      sb.append("   Buffer Storage Bytes         : ").append(String.format("%,d", bufBytes)).append(LS);
      sb.append("   Preamble Bytes               : ").append(preBytes).append(LS);
      sb.append("   Normalized Rank Error        : ").append(epsPct).append(LS);
      sb.append("   Min Value                    : ").append(String.format("%,.3f", sketch.getMinValue())).append(LS);
      sb.append("   Max Value                    : ").append(String.format("%,.3f", sketch.getMaxValue())).append(LS);
      sb.append("### END SKETCH SUMMARY").append(LS);
    }
    return sb.toString();
  }

  static void zipSize2KBuffer(
      double[] bufA, int startA, // input
      double[] bufC, int startC, // output
      int k) {
    //    assert bufA.length >= 2*k; // just for now    
    //    assert startA == 0; // just for now
  
    //    int randomOffset = (int) (2.0 * Math.random());
    int randomOffset = (QuantilesSketch.rand.nextBoolean())? 1 : 0;
    //    assert randomOffset == 0 || randomOffset == 1;
  
    //    int limA = startA + 2*k;
    int limC = startC + k;
  
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  static void justZipWithStride(
      double[] bufA, int startA, // input
      double[] bufC, int startC, // output
      int kC, // number of items that should be in the output
      int stride) {
    int randomOffset = (QuantilesSketch.rand.nextInt(stride));
    int limC = startC + kC;
  
    for (int a = startA + randomOffset, c = startC; c < limC; a += stride, c++ ) {
      bufC[c] = bufA[a];
    }
  }

  static void mergeTwoSizeKBuffers(
      double[] keySrc1, int arrStart1,
      double[] keySrc2, int arrStart2,
      double[] keyDst,  int arrStart3,
      int k) {
    int arrStop1 = arrStart1 + k;
    int arrStop2 = arrStart2 + k;
  
    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
  
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc2[i2] < keySrc1[i1]) { 
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

  static boolean sameStructurePredicate( HeapQuantilesSketch mq1, HeapQuantilesSketch mq2) {
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
   * Returns the current element capacity of the combined data buffer given <i>k</i> and <i>n</i>.
   * 
   * @param k sketch parameter. This determines the accuracy of the sketch and the 
   * size of the updatable data structure, which is a function of k.
   * 
   * @param n The number of elements in the input stream
   * @return the current element capacity of the combined data buffer
   */
  static int bufferElementCapacity(int k, long n) {
    int maxLevels = computeNumLevelsNeeded(k, n);
    if (maxLevels > 0) return (2+maxLevels) * k;
    assert n < 2*k;
    int m = Math.min(MIN_BASE_BUF_SIZE,2*k);
    if (n <= m) return m;
    int q = intDivideRoundUp(n, m);
    assert q >= 1;
    int q2 = ceilingPowerOf2(q);
    int x = m*q2;
    return Math.min(x, 2*k);
  }

  private static int intDivideRoundUp(long n, int m) {
    int q = (int)n/m;
    if (q*m == n) return q;
    else return q+1;
  }
  
  /**
   * Computes the number of valid levels above the base buffer
   * @param bitPattern the bit pattern for valid log levels
   * @return the number of valid levels above the base buffer
   */
  static int numValidLevels(long bitPattern) {
    return Long.bitCount(bitPattern);
  }

  /**
   * Computes the base buffer count given k, n
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch
   * @return the base buffer count
   */
  static int computeBaseBufferCount(int k, long n) {
    return (int) (n % (2L * k));
  }

  /**
   * Computes the levels bit pattern given k, n.
   * This is computed as <i>n / (2*k)</i>.
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the levels bit pattern
   */
  static long computeBitPattern(int k, long n) {
    return n / (2L * k);
  }

  //used by HeapQS
  static double lg(double x) {
    return ( Math.log(x)) / (Math.log(2.0) );
  }
  
  /**
   * Computes the number of logarithmic levels needed given k and n.
   * This is equivalent to max(floor(lg(n/k), 0).
   * @param k the configured size of the sketch
   * @param n the total values presented to the sketch.
   * @return the number of levels needed.
   */
  static int computeNumLevelsNeeded(int k, long n) {
    return 1 + hiBitPos(n / (2L * k));
  }
  
  /**
   * Zero based position of the highest one-bit of the given long
   * @param num the given long
   * @return Zero based position of the highest one-bit of the given long
   */
  static int hiBitPos(long num) {
    return 63 - Long.numberOfLeadingZeros(num);
  }

  //used by HeapQS
  static int positionOfLowestZeroBitStartingAt(long numIn, int startingPos) {
    long num = numIn >>> startingPos;
    int pos = 0;
    while ((num & 1L) != 0) {
      num = num >>> 1;
      pos++;
    }
    return (pos + startingPos);
  }

  //Used by HeapQS
  static double sumOfDoublesInSubArray(double[] arr, int subArrayStart, int subArrayLength) {
    double total = 0.0;
    int subArrayStop = subArrayStart + subArrayLength;
    for (int i = subArrayStart; i < subArrayStop; i++) {
      total += arr[i];
    }
    return total;
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
  static void bilinearTimeIncrementHistogramCounters(double[] samples, int offset, int numSamples, 
      long weight, double[] splitPoints, long[] counters) {
    assert (splitPoints.length + 1 == counters.length);
    for (int i = 0; i < numSamples; i++) { 
      double sample = samples[i+offset];
      int j = 0;

      for (j = 0; j < splitPoints.length; j++) {
        double splitpoint = splitPoints[j];
        if (sample < splitpoint) { 
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
  static void linearTimeIncrementHistogramCounters(double[] samples, int offset, int numSamples, 
      long weight, double[] splitPoints, long[] counters) {
    int numSplitPoints = splitPoints.length;

    int i = 0;
    int j = 0;

    while (i < numSamples && j < numSplitPoints) {
      if (samples[i+offset] < splitPoints[j]) {
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
  static void blockyTandemMergeSort(double[] keyArr, long[] valArr, int arrLen, int blkSize) {
    assert blkSize >= 1;
    if (arrLen <= blkSize) return;
    int numblks = arrLen / blkSize;
    if (numblks * blkSize < arrLen) numblks += 1;
    assert (numblks * blkSize >= arrLen);

    // duplicate the input is preparation for the "ping-pong" copy reduction strategy. 
    double[] keyTmp = Arrays.copyOf(keyArr, arrLen);
    long[] valTmp   = Arrays.copyOf(valArr, arrLen);

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
  private static void blockyTandemMergeSortRecursion(double[] keySrc, long[] valSrc,
      double[] keyDst, long[] valDst, int grpStart, int grpLen, /* indices of blocks */
      int blkSize, int arrLim) {
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
                           grpStart1, grpLen1, blkSize, arrLim);

    //swap roles of src and dst
    blockyTandemMergeSortRecursion(keyDst, valDst,
                           keySrc, valSrc,
                           grpStart2, grpLen2, blkSize, arrLim);

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
  private static void tandemMerge(double[] keySrc, long[] valSrc,
                                  int arrStart1, int arrLen1,
                                  int arrStart2, int arrLen2,
                                  double[] keyDst, long[] valDst,
                                  int arrStart3) {
    int arrStop1 = arrStart1 + arrLen1;
    int arrStop2 = arrStart2 + arrLen2;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;

    while (i1 < arrStop1 && i2 < arrStop2) {
      if (keySrc[i2] < keySrc[i1]) { 
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
  
  //************************************************************
  /**
   * Computes epsilon from K. The following table are examples.
   * <code>
   *           eps      eps from inverted
   *     K   empirical  adjusted formula
   *  -------------------------------------
   *    16   0.121094   0.121454102233560
   *    32   0.063477   0.063586601346532
   *    64   0.033081   0.033169048393679
   *   128   0.017120   0.017248096847308
   *   256   0.008804   0.008944835012965
   *   512   0.004509   0.004627803568920
   *  1024   0.002303   0.002389303789572
   *
   *  these could be used in a unit test
   *  2   0.821714930853465
   *  16   0.12145410223356
   *  1024   0.00238930378957284
   *  1073741824   3.42875166500824e-09
   * </code>
   */ //used by Heap QS
  static class EpsilonFromK {
    /**
     *  Used while crunching down the empirical results.  If this value is changed the adjustKForEps
     *  value will be incorrect and must also be recomputed.  Don't touch this!
     */
    private static final double deltaForEps = 0.01;  

    /**
     *  A heuristic fudge factor that causes the inverted formula to better match the empirical.
     *  The value of 4/3 is directly associated with the deltaForEps value of 0.01. Don't touch this!
     */
    private static final double adjustKForEps = 4.0 / 3.0;  // fudge factor

    /**
     *  Ridiculously fine tolerance given the fudge factor; 1e-3 would probably suffice
     */
    private static final double bracketedBinarySearchForEpsTol = 1e-15; 

    /**
     * From extensive empirical testing we recommend most users use this method for deriving 
     * epsilon. This uses a fudge factor of 4/3 times the theoretical calculation of epsilon.
     * @param k the given k that must be greater than one.
     * @return the resulting epsilon
     */ //used by HeapQS, so far
    static double getAdjustedEpsilon(int k) {
      if (k == 1) return 1.0; 
      return getTheoreticalEpsilon(k, adjustKForEps);
    }
    
    /**
     * Finds the epsilon given K and a fudge factor.
     * See Cormode's Mergeable Summaries paper, Journal version, Theorem 3.6. 
     * This has a good fit between values of k between 16 and 1024. 
     * Beyond that has not been empirically tested.
     * @param k The given value of k
     * @param ff The given fudge factor. No fudge factor = 1.0. 
     * @return the resulting epsilon
     */ //used only by getAdjustedEpsilon()
    private static double getTheoreticalEpsilon(int k, double ff) {
      if (k < 2) throw new IllegalArgumentException("K must be greater than one.");
      // don't need to check in the other direction because an int is very small
      double kf = k*ff;
      assert kf >= 2.15; // ensures that the bracketing succeeds
      assert kf < 1e12;  // ditto, but could actually be bigger
      double lo = 1e-16;
      double hi = 1.0 - 1e-16;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      return bracketedBinarySearchForEps(kf, lo, hi);
    }
    
    private static double kOfEpsFormula(double eps) {
      return (1.0 / eps) * (Math.sqrt(Math.log(1.0 / (eps * deltaForEps))));
    }

    private static boolean epsForKPredicate(double eps, double kf) {
      return kOfEpsFormula(eps) >= kf;
    }

    private static double bracketedBinarySearchForEps(double kf, double lo, double hi) {
      assert lo < hi;
      assert epsForKPredicate(lo, kf);
      assert !epsForKPredicate(hi, kf);
      if ((hi - lo) / lo < bracketedBinarySearchForEpsTol) {
        return lo;
      }
      double mid = (lo + hi) / 2.0;
      assert mid > lo;
      assert mid < hi;
      if (epsForKPredicate(mid, kf)) {
        return bracketedBinarySearchForEps(kf, mid, hi);
      }
      else {
        return bracketedBinarySearchForEps(kf, lo, mid);
      }
    }
  } //End of EpsilonFromK

//  public static void main(String[] args) {
//    long v = 1;
//    for (int i=0; i<64; i++) {
//      long w = v << i;
//      long w2 = w -1;
//      System.out.println(i+"\t"+Long.toBinaryString(w2)+"\t"+hiBitPos(w2)+"\t"+w2);
//    }
//  }
  
}