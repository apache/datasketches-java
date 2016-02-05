/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.bufferElementCapacity;
import static com.yahoo.sketches.quantiles.Util.computeNumLevelsNeeded;
import static com.yahoo.sketches.quantiles.Util.DEFAULT_K;
import static com.yahoo.sketches.quantiles.Util.DEFAULT_SEED;
import static com.yahoo.sketches.quantiles.Util.lg;
import static java.lang.Math.floor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class HeapQuantilesSketchTest {
  private static final short SEED = 32749; // > 0 means sketch is deterministic for testing
  
  @Test
    public void checkGetAdjustedEpsilon() {
    // note: there is a big fudge factor in these numbers, so they don't need to be computed exactly
    double absTol = 1e-14; // we just want to catch gross bugs
    int[] kArr = {2,16,1024,1 << 30};
    double[] epsArr = { // these were computed by an earlier ocaml version of the function
      0.821714930853465,
      0.12145410223356,
      0.00238930378957284,
      3.42875166500824e-09 };
    for (int i = 0; i < 4; i++) {
      assertEquals(epsArr[i], 
                   Util.EpsilonFromK.getAdjustedEpsilon(kArr[i]),
                   absTol,
                   "adjustedFindEpsForK() doesn't match precomputed value");
    }
    for (int i = 0; i < 3; i++) {
      QuantilesSketch qs = QuantilesSketch.builder().build(kArr[i]);
      assertEquals(epsArr[i], 
                   qs.getNormalizedRankError(),
                   absTol,
                   "getNormalizedCountError() doesn't match precomputed value");
    }
  }

  // Please note that this is a randomized test that could probabilistically fail 
  // if we didn't set the seed. (The probability of failure could be reduced by increasing k.)
  // Setting the seed has now made it deterministic.
  @Test
  public void checkEndToEnd() {
    int k = 256;
    QuantilesSketch qs = QuantilesSketch.builder().setSeed(SEED).build(k);
    QuantilesSketch qs2 = QuantilesSketch.builder().setSeed(SEED).build(k);
    int n = 1000000;
    for (int item = n; item >= 1; item--) {
      if (item % 4 == 0) {
        qs.update(item);
      }
      else {
        qs2.update(item);
      }
    }
    assertEquals(qs.getN()+qs2.getN(), n);
    qs.merge(qs2);

    int numPhiValues = 99;
    double[] phiArr = new double[numPhiValues];
    for (int q = 1; q <= 99; q++) {
      phiArr[q-1] = q / 100.0;
    }
    double[] splitPoints = qs.getQuantiles(phiArr);

//    for (int i = 0; i < 99; i++) {
//      String s = String.format("%d\t%.6f\t%.6f", i, phiArr[i], splitPoints[i]);
//      println(s);
//    }

    for (int q = 1; q <= 99; q++) {
      double nominal = 1e6 * q / 100.0;
      double reported = splitPoints[q-1];
      assert reported >= nominal - 10000.0;
      assert reported <= nominal + 10000.0;
    }

    double[] pmfResult = qs.getPMF(splitPoints);
    double subtotal = 0.0;
    for (int q = 1; q <= 99; q++) {
      double phi = q / 100.0;
      subtotal += pmfResult[q-1];
      assert subtotal >= phi - 0.01;
      assert subtotal <= phi + 0.01;
    }
    // should probably assert that the pmf sums to 1.0

    double[] cdfResult = qs.getCDF(splitPoints);
    for (int q = 1; q <= 99; q++) {
      double phi = q / 100.0;
      subtotal = cdfResult[q-1];
      assert subtotal >= phi - 0.01;
      assert subtotal <= phi + 0.01;
    }
    // should probably assert that the final cdf value is 1.0
  }

  @Test
  public void checkConstructAuxiliary() {
    for (int k = 2; k <= 32; k *= 2) {
      HeapQuantilesSketch qs = HeapQuantilesSketch.getInstance(k, SEED);
      for (int numItemsSoFar = 0; numItemsSoFar < 1000; numItemsSoFar++) {
        Auxiliary aux = qs.constructAuxiliary();
        int numSamples = qs.getRetainedEntries();
        double[] auxItems = aux.auxSamplesArr_;
        long[] auxAccum = aux.auxCumWtsArr_;

        assert qs.getN() == aux.auxN_;
        assert numItemsSoFar == aux.auxN_;

        assert auxItems.length == numSamples;
        assert auxAccum.length == numSamples + 1;

        double mqSumOfSamples = qs.sumOfSamplesInSketch();
        double auSumOfSamples = Util.sumOfDoublesInSubArray(auxItems, 0, numSamples);

        // the following test might be able to detect errors in handling the samples
        // e.g. accidentally dropping or duplicating a sample
        assert Math.floor(0.5 + mqSumOfSamples) == Math.floor(0.5 + auSumOfSamples);

        // the following test might be able to detect errors in handling the sample weights
        assert auxAccum[numSamples] == numItemsSoFar;

        for (int i = 0; i < numSamples-1; i++) {
          assert auxItems[i] <= auxItems[i+1]; // assert sorted order
          assert auxAccum[i] <  auxAccum[i+1]; // assert cumulative property
        }

        // This is a better test when the items are inserted in reverse order
        // as follows, but the negation seems kind of awkward.
        qs.update (-1.0 * (numItemsSoFar + 1) );
      } // end of loop over test stream
    } // end of loop over values of k
  }

  @Test
  public void checkBigMinMax () {
    int k = 32;
    QuantilesSketch qs1 = QuantilesSketch.builder().setSeed(SEED).build(k);
    QuantilesSketch qs2 = QuantilesSketch.builder().setSeed(SEED).build(k);
    QuantilesSketch qs3 = QuantilesSketch.builder().setSeed(SEED).build(k);

    for (int i = 999; i >= 1; i--) {
      qs1.update(i);
      qs2.update(1000+i);
      qs3.update(i);
    }
    assert (qs1.getQuantile (0.0) == 1.0);
    assert (qs1.getQuantile (1.0) == 999.0);

    assert (qs2.getQuantile (0.0) == 1001.0);
    assert (qs2.getQuantile (1.0) == 1999.0);

    assert (qs3.getQuantile (0.0) == 1.0);
    assert (qs3.getQuantile (1.0) == 999.0);

    double[] queries = {0.0, 1.0};

    double[] resultsA = qs1.getQuantiles(queries);
    assert (resultsA[0] == 1.0);
    assert (resultsA[1] == 999.0);

    qs1.merge(qs2);
    qs2.merge(qs3);

    double[] resultsB = qs1.getQuantiles(queries);
    assert (resultsB[0] == 1.0);
    assert (resultsB[1] == 1999.0);

    double[] resultsC = qs2.getQuantiles(queries);
    assert (resultsC[0] == 1.0);
    assert (resultsC[1] == 1999.0);
  }

  @Test
  public void checkSmallMinMax () {
    int k = 32;
    QuantilesSketch qs1 = QuantilesSketch.builder().setSeed(SEED).build(k);
    QuantilesSketch qs2 = QuantilesSketch.builder().setSeed(SEED).build(k);
    QuantilesSketch qs3 = QuantilesSketch.builder().setSeed(SEED).build(k);

    for (int i = 8; i >= 1; i--) {
      qs1.update(i);
      qs2.update(10+i);
      qs3.update(i);
    }
    assert (qs1.getQuantile (0.0) == 1.0);
    assert (qs1.getQuantile (0.5) == 5.0);
    assert (qs1.getQuantile (1.0) == 8.0);

    assert (qs2.getQuantile (0.0) == 11.0);
    assert (qs2.getQuantile (0.5) == 15.0);
    assert (qs2.getQuantile (1.0) == 18.0);

    assert (qs3.getQuantile (0.0) == 1.0);
    assert (qs3.getQuantile (0.5) == 5.0);
    assert (qs3.getQuantile (1.0) == 8.0);

    double[] queries = {0.0, 0.5, 1.0};

    double[] resultsA = qs1.getQuantiles(queries);
    assert (resultsA[0] == 1.0);
    assert (resultsA[1] == 5.0);
    assert (resultsA[2] == 8.0);

    qs1.merge(qs2);
    qs2.merge(qs3);

    double[] resultsB = qs1.getQuantiles(queries);
    assert (resultsB[0] == 1.0);
    assert (resultsB[1] == 11.0);
    assert (resultsB[2] == 18.0);

    double[] resultsC = qs2.getQuantiles(queries);
    assert (resultsC[0] == 1.0);
    assert (resultsC[1] == 11.0);
    assert (resultsC[2] == 18.0);
  }
  
  @Test
  public void checkMisc() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k, n, 0, (short)0);

    int n2 = (int)qs.getN();
    assertEquals(n2, n);
    qs.update(Double.NaN);
    qs.reset();
    assertEquals(qs.getN(), 0);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkToStringDetail() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k, 0, 0, (short)0);
    String s = qs.toString();
    s = qs.toString(false, true);
    qs = buildQS(k, n, 0, (short)0);
    //println(qs.toString());
    s = qs.toString(false, true);
    //println(qs.toString(false, true));
    
    int n2 = (int)qs.getN();
    assertEquals(n2, n);
    qs.update(Double.NaN); //ignore
    qs.reset();
    assertEquals(qs.getN(), 0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkConstructorException() {
    @SuppressWarnings("unused")
    QuantilesSketch qs = QuantilesSketch.builder().setSeed(SEED).build(0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetQuantiles() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k, n, 0, (short)0);
    double[] frac = {-0.5}; //an array
    qs.getQuantiles(frac);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetQuantile() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k, n, 0, (short)0);
    double frac = -0.5; //negative not allowed
    qs.getQuantile(frac);
  }
  
  //@Test  //visual only
  public void summaryCheckViaMemory() {
    QuantilesSketch qs = buildQS(256, 1000000, 0, (short)0);
    println(qs.toString());
    println("");
    
    NativeMemory srcMem = new NativeMemory(qs.toByteArray());
    
    HeapQuantilesSketch qs2 = HeapQuantilesSketch.getInstance(srcMem);
    println(qs2.toString());
  }
  
  
  @Test
  public void checkComputeNumLevelsNeeded() {
    int n = 1 << 20;
    int k = DEFAULT_K;
    int lvls1 = computeNumLevelsNeeded(k, n);
    int lvls2 = (int)Math.max(floor(lg((double)n/k)),0);
    assertEquals(lvls1, lvls2);
  }
  
  @Test
  public void checkComputeBitPattern() {
    int n = 1 << 20;
    int k = DEFAULT_K;
    long bitP = Util.computeBitPattern(k, n);
    assertEquals(bitP, n/(2L*k));
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkValidateSplitPoints() {
    double[] arr = {2, 1};
    QuantilesSketch.validateSequential(arr);
  }
  
  @Test
  public void checkGetStorageBytes() {
    int k = DEFAULT_K;
    QuantilesSketch qs = buildQS(k, 0, 0, DEFAULT_SEED); //k, n, start, seed
    int stor = qs.getStorageBytes();
    assertEquals(stor, 8);
    
    qs = buildQS(k, 2*(k), 0, DEFAULT_SEED); //forces one level
    stor = qs.getStorageBytes();
    int bbLen = qs.getCombinedBuffer().length << 3;
    println("BufLen      : "+bbLen);
    println("getStorBytes: "+stor);
    assertEquals(stor, 40 + bbLen);
    
    qs = buildQS(k, 2*k-1, 0, DEFAULT_SEED); //just Base Buffer
    stor = qs.getStorageBytes();
    bbLen = qs.getCombinedBuffer().length << 3;
    println("BufLen      : "+bbLen);
    println("getStorBytes: "+stor);
    assertEquals(stor, 40 + bbLen);
  }
  
  @Test
  public void checkGetStorageBytes2() {
    int k = DEFAULT_K;
    long v = 1;
    QuantilesSketch qs = QuantilesSketch.builder().setSeed(SEED).build(k);
    for (int i = 0; i< 1000; i++) {
      qs.update(v++);
//      for (int j = 0; j < 1000; j++) {
//        qs.update(v++);
//      }
      byte[] byteArr = qs.toByteArray();
      assertEquals(qs.getStorageBytes(), byteArr.length);
    }
  }
  
  
  @Test
  public void checkMerge() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs1 = buildQS(k,n,0, (short)0);
    QuantilesSketch qs2 = buildQS(k,0,0, (short)0);
    qs2.merge(qs1);
    double med1 = qs1.getQuantile(0.5);
    double med2 = qs2.getQuantile(0.5);
    assertEquals(med1, med2, 0.0);
    //println(med1+","+med2);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMergeException() {
    int k = DEFAULT_K;
    QuantilesSketch qs1 = buildQS(k,0,0, (short)0);
    QuantilesSketch qs2 = buildQS(2*k,0,0, (short)0);
    qs2.merge(qs1);
  }
  
  @Test
  public void checkInternalBuildHistogram() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k,n,0, (short)0);
    double eps = qs.getNormalizedRankError();
    //println("EPS:"+eps);
    double[] spts = {100000, 500000, 900000};
    double[] fracArr = qs.getPMF(spts);
//    println(fracArr[0]+", "+ (fracArr[0]-0.1));
//    println(fracArr[1]+", "+ (fracArr[1]-0.4));
//    println(fracArr[2]+", "+ (fracArr[2]-0.4));
//    println(fracArr[3]+", "+ (fracArr[3]-0.1));
    assertEquals(fracArr[0], .1, eps);
    assertEquals(fracArr[1], .4, eps);
    assertEquals(fracArr[2], .4, eps);
    assertEquals(fracArr[3], .1, eps);
  }
  
  @Test
  public void checkComputeBaseBufferCount() {
    int n = 1 << 20;
    int k = DEFAULT_K;
    long bbCnt = Util.computeBaseBufferCount(k, n);
    assertEquals(bbCnt, n % (2L*k));
  }
  
  @Test
  public void checkToFromByteArray() {
    int k = DEFAULT_K;
    int n = 1000000;
    QuantilesSketch qs = buildQS(k,n, 0, (short)0);
    
    byte[] byteArr = qs.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    QuantilesSketch qs2 = QuantilesSketch.heapify(mem);
    //HeapQuantilesSketch qs2 = HeapQuantilesSketch.getInstance(mem);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(qs.getQuantile(f), qs2.getQuantile(f), 0.0);
    }
  }
  
  @Test
  public void checkEmpty() {
    int k = DEFAULT_K;
    QuantilesSketch qs1 = buildQS(k, 0, 0, (short)0);
    byte[] byteArr = qs1.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    QuantilesSketch qs2 = QuantilesSketch.heapify(mem);
    assertTrue(qs2.isEmpty());
    assertEquals(byteArr.length, 8);
    //println(qs1.toString(true, true));
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMemTooSmall1() {
    Memory mem = new NativeMemory(new byte[7]);
    @SuppressWarnings("unused")
    HeapQuantilesSketch qs2 = HeapQuantilesSketch.getInstance(mem);
  }
  
  //Corruption tests
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSerVer() {
    QuantilesSketch.checkSerVer(0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkFamilyID() {
    QuantilesSketch.checkFamilyID(3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBufAllocAndCap() {
    int k = DEFAULT_K;
    long n = 1000;
    int computedBufAlloc = bufferElementCapacity(k, n);
    int memAlloc = computedBufAlloc -1; //corrupt
    int memCap = (computedBufAlloc + 5) << 3;
    QuantilesSketch.checkBufAllocAndCap(k, n, memAlloc, memCap);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBufAllocAndCap2() {
    int k = DEFAULT_K;
    long n = 1000;
    int computedBufAlloc = bufferElementCapacity(k, n);
    int memAlloc = computedBufAlloc; //corrupt
    int memCap = (computedBufAlloc + 5) << 3;
    QuantilesSketch.checkBufAllocAndCap(k, n, memAlloc, memCap-1);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkPreLongsFlagsCap() {
    int preLongs = 5;
    int flags = EMPTY_FLAG_MASK;
    int memCap = 8;
    QuantilesSketch.checkPreLongsFlagsCap(preLongs, flags,  memCap); //corrupt
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkPreLongsFlagsCap2() {
    int preLongs = 5;
    int flags = 0;
    int memCap = 8;
    QuantilesSketch.checkPreLongsFlagsCap(preLongs, flags,  memCap); //corrupt
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkFlags() {
    int flags = 1;
    QuantilesSketch.checkFlags(flags);
  }

  @Test
  public void checkImproperKvalues() {
    checksForK(0);
    checksForK(1<<16);
  }
  
  //Visual only tests
  static void testDownSampling(int bigK, int smallK) {  
    HeapQuantilesSketch origSketch = HeapQuantilesSketch.getInstance(bigK, Util.DEFAULT_SEED);
    HeapQuantilesSketch directSketch = HeapQuantilesSketch.getInstance(smallK, Util.DEFAULT_SEED);
    for (int i = 127; i >= 1; i--) {
      origSketch.update (i);
      directSketch.update (i);
    }
    HeapQuantilesSketch downSketch = (HeapQuantilesSketch)origSketch.downSample(smallK);
    println ("\nOrig\n");
    println(origSketch.toString(true, true));
    println ("\nDown\n");
    println(downSketch.toString(true, true));
    println("\nDirect\n");
    println(directSketch.toString(true, true));
  }
  
  @Test
  public void checkDownSampling() {
    testDownSampling(4,4);
    testDownSampling(16,4);
    testDownSampling(12,3);
  }
  
  @Test
  public void testDownSampling2 () {
    HeapQuantilesSketch origSketch = HeapQuantilesSketch.getInstance(8, Util.DEFAULT_SEED);
    HeapQuantilesSketch directSketch = HeapQuantilesSketch.getInstance(2, Util.DEFAULT_SEED);
    HeapQuantilesSketch downSketch;
    downSketch = (HeapQuantilesSketch)origSketch.downSample(2);
    assertTrue(HeapQuantilesSketch.sameStructurePredicate (directSketch, downSketch));
    for (int i = 0; i < 50; i++) {
      origSketch.update (i);
      directSketch.update (i);
      downSketch = (HeapQuantilesSketch)origSketch.downSample(2);
      assertTrue (HeapQuantilesSketch.sameStructurePredicate (directSketch, downSketch));
    }
    
  }

  @Test
  public void testDownSampling3() {
    for (int n1 = 0; n1 < 50; n1++ ) {
      HeapQuantilesSketch bigSketch = HeapQuantilesSketch.getInstance(8, Util.DEFAULT_SEED);
      for (int i1 = 1; i1 <= n1; i1++ ) {
        bigSketch.update(i1);
      }
      for (int n2 = 0; n2 < 50; n2++ ) {
        HeapQuantilesSketch directSketch = HeapQuantilesSketch.getInstance(2, Util.DEFAULT_SEED);
        for (int i1 = 1; i1 <= n1; i1++ ) {
          directSketch.update(i1);
        }
        for (int i2 = 1; i2 <= n2; i2++ ) {
          directSketch.update(i2);
        }
        HeapQuantilesSketch smlSketch = HeapQuantilesSketch.getInstance(2, Util.DEFAULT_SEED);
        for (int i2 = 1; i2 <= n2; i2++ ) {
          smlSketch.update(i2);
        }
        HeapQuantilesSketch.downSamplingMergeInto(bigSketch, smlSketch);
        assertTrue (HeapQuantilesSketch.sameStructurePredicate(directSketch, smlSketch));
      }
    }
  }

  @Test
  public void testDownSampling4() {
    for (int n1 = 0; n1 < 50; n1++ ) {
      HeapQuantilesSketch bigSketch = HeapQuantilesSketch.getInstance(8, Util.DEFAULT_SEED);
      for (int i1 = 1; i1 <= n1; i1++ ) {
        bigSketch.update(i1);
      }
      for (int n2 = 0; n2 < 50; n2++ ) {
        HeapQuantilesSketch directSketch = HeapQuantilesSketch.getInstance(2, Util.DEFAULT_SEED);
        for (int i1 = 1; i1 <= n1; i1++ ) {
          directSketch.update(i1);
        }
        for (int i2 = 1; i2 <= n2; i2++ ) {
          directSketch.update(i2);
        }
        HeapQuantilesSketch smlSketch = HeapQuantilesSketch.getInstance(2, Util.DEFAULT_SEED);
        for (int i2 = 1; i2 <= n2; i2++ ) {
          smlSketch.update(i2);
        }
        smlSketch.merge(bigSketch);
        assertTrue (HeapQuantilesSketch.sameStructurePredicate(directSketch, smlSketch));
      }
    }
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions1() {
    QuantilesSketch qs1 = QuantilesSketch.builder().build(4); // not smaller
    QuantilesSketch qs2 = QuantilesSketch.builder().build(3);
    qs1.merge(qs2);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions2() {
    QuantilesSketch qs1 = QuantilesSketch.builder().build(4);
    QuantilesSketch qs2 = QuantilesSketch.builder().build(7); // 7/4 not pwr of 2
    qs1.merge(qs2);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions3() {
    QuantilesSketch qs1 = QuantilesSketch.builder().build(4);
    QuantilesSketch qs2 = QuantilesSketch.builder().build(12); // 12/4 not pwr of 2
    qs1.merge(qs2);
  }
  
  private static void checksForK(int k) {
    String s = "Did not catch improper k: "+k;
    try {
      QuantilesSketch.builder().setK(k);
      checkForKfailed(s);
    } catch (IllegalArgumentException e) {}
    try {
      QuantilesSketch.builder().build(k);
      checkForKfailed(s);
    } catch (IllegalArgumentException e) {}
    try {
      HeapQuantilesSketch.getInstance(k, DEFAULT_SEED);
      checkForKfailed(s);
    } catch (IllegalArgumentException e) {}
  }
  
  private static void checkForKfailed(String s) {
    boolean print = false; //useful for debugging
    if (print) { System.err.println(s); }
    else { throw new IllegalStateException(s); }
  }
  
  //@Test  //visual only
  public void quantilesCheckViaMemory() {
    int k = 256;
    long n = 1000000;
    QuantilesSketch qs = buildQS(k, n, 0, (short)0);
    double[] ranks = {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    println(getRanksTable(qs, ranks));
    println("");
    
    NativeMemory srcMem = new NativeMemory(qs.toByteArray());
    
    HeapQuantilesSketch qs2 = HeapQuantilesSketch.getInstance(srcMem);
    println(getRanksTable(qs2, ranks));
  }
  
  static String getRanksTable(QuantilesSketch qs, double[] ranks) {
    double rankError = qs.getNormalizedRankError();
    double[] values = qs.getQuantiles(ranks);
    double maxV = qs.getMaxValue();
    double minV = qs.getMinValue();
    double delta = maxV - minV;
    println("Note: This prints the relative value errors for illustration.");
    println("The quantiles sketch does not and can not guarantee relative value errors");
    
    StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("N = ").append(qs.getN()).append(LS);
    sb.append("K = ").append(qs.getK()).append(LS);
    String formatStr1 = "%10s%15s%10s%15s%10s%10s";
    String formatStr2 = "%10.1f%15.5f%10.0f%15.5f%10.5f%10.5f";
    String hdr = String.format(formatStr1, "Rank", "ValueLB", "<= Value", "<= ValueUB", "RelErrLB", "RelErrUB");
    sb.append(hdr).append(LS);
    for (int i=0; i<ranks.length; i++) {
      double rank = ranks[i];
      double value = values[i];
      if (rank == 0.0) { assertEquals(value, minV, 0.0); }
      else if (rank == 1.0) { assertEquals(value, maxV, 0.0); }
      else {
        double rankUB = rank + rankError;
        double valueUB = minV + delta*rankUB;
        double rankLB = Math.max(rank - rankError, 0.0);
        double valueLB = minV + delta*rankLB;
        assertTrue(value < valueUB);
        assertTrue(value > valueLB);

        double valRelPctErrUB = valueUB/ value -1.0;
        double valRelPctErrLB = valueLB/ value -1.0;
        String row = String.format(formatStr2,rank, valueLB, value, valueUB, valRelPctErrLB, valRelPctErrUB);
        sb.append(row).append(LS);
      }
    }
    return sb.toString();
  }
  
  static QuantilesSketch buildQS(int k, long n, int startV, short seed) {
    QuantilesSketch qs = QuantilesSketch.builder().setSeed(seed).build(k);
    for (int i=0; i<n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }
  
  @Test
  public void checkGetSeed() {
    QuantilesSketch qs1 = QuantilesSketch.builder().build(4);
    assertEquals(qs1.getSeed(), 0);
  }
  
  @Test
  public void checkKisOne() {
    QuantilesSketch qs1 = QuantilesSketch.builder().build(1);
    double err = qs1.getNormalizedRankError();
    assertEquals(err, 1.0, 0.0);
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here //TODO
  }
  
}
