/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SKETCH_TYPE_BYTE;
import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.computeCombBufItemCapacity;
import static com.yahoo.sketches.quantiles.Util.computeNumLevelsNeeded;
import static com.yahoo.sketches.quantiles.Util.lg;
import static java.lang.Math.floor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class HeapDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    DoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

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
      DoublesSketch qs = DoublesSketch.builder().build(kArr[i]);
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
    DoublesSketch qs = DoublesSketch.builder().build(k);
    DoublesSketch qs2 = DoublesSketch.builder().build(k);
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
    DoublesUnion union = DoublesUnion.builder().build(qs);
    union.update(qs2);
    DoublesSketch result = union.getResult();
    
    int numPhiValues = 99;
    double[] phiArr = new double[numPhiValues];
    for (int q = 1; q <= 99; q++) {
      phiArr[q-1] = q / 100.0;
    }
    double[] splitPoints = result.getQuantiles(phiArr);

//    for (int i = 0; i < 99; i++) {
//      String s = String.format("%d\t%.6f\t%.6f", i, phiArr[i], splitPoints[i]);
//      println(s);
//    }

    for (int q = 1; q <= 99; q++) {
      double nominal = 1e6 * q / 100.0;
      double reported = splitPoints[q-1];
      assertTrue(reported >= nominal - 10000.0);
      assertTrue(reported <= nominal + 10000.0);
    }

    double[] pmfResult = result.getPMF(splitPoints);
    double subtotal = 0.0;
    for (int q = 1; q <= 99; q++) {
      double phi = q / 100.0;
      subtotal += pmfResult[q-1];
      assertTrue(subtotal >= phi - 0.01);
      assertTrue(subtotal <= phi + 0.01);
    }
    // should probably assert that the pmf sums to 1.0

    double[] cdfResult = result.getCDF(splitPoints);
    for (int q = 1; q <= 99; q++) {
      double phi = q / 100.0;
      subtotal = cdfResult[q-1];
      assertTrue(subtotal >= phi - 0.01);
      assertTrue(subtotal <= phi + 0.01);
    }
    // should probably assert that the final cdf value is 1.0
  }

  @Test
  public void checkConstructAuxiliary() {
    for (int k = 2; k <= 32; k *= 2) {
      HeapDoublesSketch qs = HeapDoublesSketch.getInstance(k);
      for (int numItemsSoFar = 0; numItemsSoFar < 1000; numItemsSoFar++) {
        DoublesAuxiliary aux = qs.constructAuxiliary();
        int numSamples = qs.getRetainedItems();
        double[] auxItems = aux.auxSamplesArr_;
        long[] auxAccum = aux.auxCumWtsArr_;

        assertTrue(qs.getN() == aux.auxN_);
        assertTrue(numItemsSoFar == aux.auxN_);

        assertTrue(auxItems.length == numSamples);
        assertTrue(auxAccum.length == numSamples + 1);

        double mqSumOfSamples = sumOfSamplesInSketch(qs);
        double auSumOfSamples = sumOfDoublesInSubArray(auxItems, 0, numSamples);

        // the following test might be able to detect errors in handling the samples
        // e.g. accidentally dropping or duplicating a sample
        assertTrue(Math.floor(0.5 + mqSumOfSamples) == Math.floor(0.5 + auSumOfSamples));

        // the following test might be able to detect errors in handling the sample weights
        assertTrue(auxAccum[numSamples] == numItemsSoFar);

        for (int i = 0; i < numSamples-1; i++) {
          assertTrue(auxItems[i] <= auxItems[i+1]); // assert sorted order
          assertTrue(auxAccum[i] <  auxAccum[i+1]); // assert cumulative property
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
    DoublesSketch qs1 = DoublesSketch.builder().build(k);
    DoublesSketch qs2 = DoublesSketch.builder().build(k);
    DoublesSketch qs3 = DoublesSketch.builder().build(k);

    for (int i = 999; i >= 1; i--) {
      qs1.update(i);
      qs2.update(1000+i);
      qs3.update(i);
    }
    assertTrue(qs1.getQuantile(0.0) == 1.0);
    assertTrue(qs1.getQuantile(1.0) == 999.0);

    assertTrue(qs2.getQuantile(0.0) == 1001.0);
    assertTrue(qs2.getQuantile(1.0) == 1999.0);

    assertTrue((qs3.getQuantile(0.0) == 1.0));
    assertTrue(qs3.getQuantile(1.0) == 999.0);

    double[] queries = {0.0, 1.0};

    double[] resultsA = qs1.getQuantiles(queries);
    assertTrue(resultsA[0] == 1.0);
    assertTrue(resultsA[1] == 999.0);
    
    DoublesUnion union1 = DoublesUnion.builder().build(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();
    
    DoublesUnion union2 = DoublesUnion.builder().build(qs2);
    union2.update(qs3);
    DoublesSketch result2 = union2.getResult();

    double[] resultsB = result1.getQuantiles(queries);
    assertTrue(resultsB[0] == 1.0);
    assertTrue(resultsB[1] == 1999.0);

    double[] resultsC = result2.getQuantiles(queries);
    assertTrue(resultsC[0] == 1.0);
    assertTrue(resultsC[1] == 1999.0);
  }

  @Test
  public void checkSmallMinMax () {
    int k = 32;
    DoublesSketch qs1 = DoublesSketch.builder().build(k);
    DoublesSketch qs2 = DoublesSketch.builder().build(k);
    DoublesSketch qs3 = DoublesSketch.builder().build(k);

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

    DoublesUnion union1 = DoublesUnion.builder().build(qs1);
    union1.update(qs2);
    DoublesSketch result1 = union1.getResult();
    
    DoublesUnion union2 = DoublesUnion.builder().build(qs2);
    union2.update(qs3);
    DoublesSketch result2 = union2.getResult();

    double[] resultsB = result1.getQuantiles(queries);
    assert (resultsB[0] == 1.0);
    assert (resultsB[1] == 11.0);
    assert (resultsB[2] == 18.0);

    double[] resultsC = result2.getQuantiles(queries);
    assert (resultsC[0] == 1.0);
    assert (resultsC[1] == 11.0);
    assert (resultsC[2] == 18.0);
  }
  
  @Test
  public void checkMisc() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = buildQS(k, n);

    int n2 = (int)qs.getN();
    assertEquals(n2, n);
    qs.update(Double.NaN);
    qs.reset();
    assertEquals(qs.getN(), 0);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkToStringDetail() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = buildQS(k, 0);
    String s = qs.toString();
    s = qs.toString(false, true);
    //println(s);
    qs = buildQS(k, n);
    s = qs.toString();
    //println(s);
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
    DoublesSketch qs = DoublesSketch.builder().build(0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetQuantiles() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = buildQS(k, n);
    double[] frac = {-0.5};
    qs.getQuantiles(frac);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetQuantile() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = buildQS(k, n);
    double frac = -0.5; //negative not allowed
    qs.getQuantile(frac);
  }
  
  //@Test  //visual only
  public void summaryCheckViaMemory() {
    DoublesSketch qs = buildQS(256, 1000000);
    String s = qs.toString();
    println(s);
    println("");
    
    NativeMemory srcMem = new NativeMemory(qs.toByteArray());
    
    HeapDoublesSketch qs2 = HeapDoublesSketch.getInstance(srcMem);
    s = qs2.toString();
    println(s);
  }
  
  @Test
  public void checkComputeNumLevelsNeeded() {
    int n = 1 << 20;
    int k = DoublesSketch.DEFAULT_K;
    int lvls1 = computeNumLevelsNeeded(k, n);
    int lvls2 = (int)Math.max(floor(lg((double)n/k)),0);
    assertEquals(lvls1, lvls2);
  }
  
  @Test
  public void checkComputeBitPattern() {
    int n = 1 << 20;
    int k = DoublesSketch.DEFAULT_K;
    long bitP = Util.computeBitPattern(k, n);
    assertEquals(bitP, n/(2L*k));
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkValidateSplitPoints() {
    double[] arr = {2, 1};
    Util.validateFractions(arr);
  }
  
  @Test
  public void checkGetStorageBytes() {
    int k = DoublesSketch.DEFAULT_K; //128
    DoublesSketch qs = buildQS(k, 0); //k, n
    int stor = qs.getStorageBytes();
    assertEquals(stor, 8);
    
    qs = buildQS(k, 2*k); //forces one level
    stor = qs.getStorageBytes();
    
    int retItems = Util.computeRetainedItems(k, 2*k);
    assertEquals(stor, 32 + (retItems << 3));
    
    qs = buildQS(k, 2*k-1); //just Base Buffer
    stor = qs.getStorageBytes();
    retItems = Util.computeRetainedItems(k, 2*k-1);
    assertEquals(stor, 32 + (retItems << 3));
  }
  
  @Test
  public void checkGetStorageBytes2() {
    int k = DoublesSketch.DEFAULT_K;
    long v = 1;
    DoublesSketch qs = DoublesSketch.builder().build(k);
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
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs1 = buildQS(k,n,0);
    DoublesSketch qs2 = buildQS(k,0,0); //empty
    DoublesUnion union = DoublesUnion.builder().build(qs2);
    union.update(qs1);
    DoublesSketch result = union.getResult();
    double med1 = qs1.getQuantile(0.5);
    double med2 = result.getQuantile(0.5);
    assertEquals(med1, med2, 0.0);
    //println(med1+","+med2);
  }
  
  @Test
  public void checkReverseMerge() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs1 = buildQS(k,  1000, 0);
    DoublesSketch qs2 = buildQS(2*k,1000, 1000);
    DoublesUnion union = DoublesUnion.builder().build(qs2);
    union.update(qs1); //attempt merge into larger k
    DoublesSketch result = union.getResult();
    assertEquals(result.getK(), k);
  }
  
  @Test
  public void checkInternalBuildHistogram() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000000;
    DoublesSketch qs = buildQS(k,n,0);
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
    int k = DoublesSketch.DEFAULT_K;
    long bbCnt = Util.computeBaseBufferItems(k, n);
    assertEquals(bbCnt, n % (2L*k));
  }
  
  @Test
  public void checkToFromByteArray() {
    int k = DoublesSketch.DEFAULT_K;
    int n = 1000;
    DoublesSketch qs = buildQS(k,n);
    
    byte[] byteArr = qs.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    DoublesSketch qs2 = DoublesSketch.heapify(mem);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(qs.getQuantile(f), qs2.getQuantile(f), 0.0);
    }
  }
  
  @Test
  public void checkEmpty() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs1 = buildQS(k, 0);
    byte[] byteArr = qs1.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    DoublesSketch qs2 = DoublesSketch.heapify(mem);
    assertTrue(qs2.isEmpty());
    assertEquals(byteArr.length, 8);
    assertEquals(qs2.getQuantile(0.0), Double.POSITIVE_INFINITY);
    assertEquals(qs2.getQuantile(1.0), Double.NEGATIVE_INFINITY);
    assertEquals(qs2.getQuantile(0.5), Double.NaN);
    
    //println(qs1.toString(true, true));
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMemTooSmall1() {
    Memory mem = new NativeMemory(new byte[7]);
    @SuppressWarnings("unused")
    HeapDoublesSketch qs2 = HeapDoublesSketch.getInstance(mem);
  }
  
  //Corruption tests
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSerVer() {
    Util.checkSerVer(0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkFamilyID() {
    Util.checkFamilyID(3);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBufAllocAndCap() {
    int k = DoublesSketch.DEFAULT_K;
    long n = 1000;
    int combBufItemCap = computeCombBufItemCapacity(k, n);
    int badCapItems = combBufItemCap - 1; //corrupt
    int memCapBytes = (combBufItemCap + 2) * Long.BYTES;
    Util.checkMemCapacity(badCapItems, memCapBytes);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkIllegalSketchType() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs1 = buildQS(k, 0);
    byte[] byteArr = qs1.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    mem.putByte(SKETCH_TYPE_BYTE, (byte) 2); //Corrupt
    HeapDoublesSketch.getInstance(mem);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBufAllocAndCap2() {
    int k = DoublesSketch.DEFAULT_K;
    long n = 1000;
    int combBufItemCap = computeCombBufItemCapacity(k, n);
    int memCap = (combBufItemCap + 2) * Long.BYTES;
    Util.checkMemCapacity(combBufItemCap, memCap - 1); //corrupt
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkPreLongsFlagsCap() {
    int preLongs = 5;
    int flags = EMPTY_FLAG_MASK;
    int memCap = 8;
    Util.checkPreLongsFlagsCap(preLongs, flags,  memCap); //corrupt
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkPreLongsFlagsCap2() {
    int preLongs = 5;
    int flags = 0;
    int memCap = 8;
    Util.checkPreLongsFlagsCap(preLongs, flags,  memCap); //corrupt
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkFlags() {
    int flags = 1;
    Util.checkFlags(flags);
  }

  @Test
  public void checkZeroPatternReturn() {
    int k = DoublesSketch.DEFAULT_K;
    DoublesSketch qs1 = buildQS(k, 64);
    byte[] byteArr = qs1.toByteArray();
    Memory mem = new NativeMemory(byteArr);
    HeapDoublesSketch.getInstance(mem);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkBadDownSamplingRatio() {
    int k1 = 64;
    DoublesSketch qs1 = buildQS(k1, k1);
    qs1.downSample(2*k1);//should be smaller
  }
  
  @Test
  public void checkImproperKvalues() {
    checksForImproperK(0);
    checksForImproperK(1<<16);
  }
  
  //Visual only tests
  static void testDownSampling(int bigK, int smallK) {  
    HeapDoublesSketch origSketch = HeapDoublesSketch.getInstance(bigK);
    HeapDoublesSketch directSketch = HeapDoublesSketch.getInstance(smallK);
    for (int i = 127; i >= 1; i--) {
      origSketch.update (i);
      directSketch.update (i);
    }
    HeapDoublesSketch downSketch = (HeapDoublesSketch)origSketch.downSample(smallK);
    println ("\nOrig\n");
    String s = origSketch.toString(true, true);
    println(s);
    println ("\nDown\n");
    s = downSketch.toString(true, true);
    println(s);
    println("\nDirect\n");
    s = directSketch.toString(true, true);
    println(s);
  }
  
  //@Test  //Visual only tests
  public void checkDownSampling() {
    testDownSampling(4,4);
    testDownSampling(16,4);
    testDownSampling(12,3);
  }
  
  @Test
  public void testDownSampling2 () {
    HeapDoublesSketch origSketch = HeapDoublesSketch.getInstance(8);
    HeapDoublesSketch directSketch = HeapDoublesSketch.getInstance(2);
    HeapDoublesSketch downSketch;
    downSketch = (HeapDoublesSketch)origSketch.downSample(2);
    assertTrue(sameStructurePredicate (directSketch, downSketch));
    for (int i = 0; i < 50; i++) {
      origSketch.update (i);
      directSketch.update (i);
      downSketch = (HeapDoublesSketch)origSketch.downSample(2);
      assertTrue (sameStructurePredicate (directSketch, downSketch));
    }
    
  }

  @Test
  public void testDownSampling3() {
    for (int n1 = 0; n1 < 50; n1++ ) {
      HeapDoublesSketch bigSketch = HeapDoublesSketch.getInstance(8);
      for (int i1 = 1; i1 <= n1; i1++ ) {
        bigSketch.update(i1);
      }
      for (int n2 = 0; n2 < 50; n2++ ) {
        HeapDoublesSketch directSketch = HeapDoublesSketch.getInstance(2);
        for (int i1 = 1; i1 <= n1; i1++ ) {
          directSketch.update(i1);
        }
        for (int i2 = 1; i2 <= n2; i2++ ) {
          directSketch.update(i2);
        }
        HeapDoublesSketch smlSketch = HeapDoublesSketch.getInstance(2);
        for (int i2 = 1; i2 <= n2; i2++ ) {
          smlSketch.update(i2);
        }
        DoublesUtil.downSamplingMergeInto(bigSketch, smlSketch);
        assertTrue (sameStructurePredicate(directSketch, smlSketch));
      }
    }
  }

  @Test
  public void testDownSampling4() {
    for (int n1 = 0; n1 < 50; n1++ ) {
      HeapDoublesSketch bigSketch = HeapDoublesSketch.getInstance(8);
      for (int i1 = 1; i1 <= n1; i1++ ) {
        bigSketch.update(i1);
      }
      for (int n2 = 0; n2 < 50; n2++ ) {
        HeapDoublesSketch directSketch = HeapDoublesSketch.getInstance(2);
        for (int i1 = 1; i1 <= n1; i1++ ) {
          directSketch.update(i1);
        }
        for (int i2 = 1; i2 <= n2; i2++ ) {
          directSketch.update(i2);
        }
        HeapDoublesSketch smlSketch = HeapDoublesSketch.getInstance(2);
        for (int i2 = 1; i2 <= n2; i2++ ) {
          smlSketch.update(i2);
        }
        DoublesUnion union = DoublesUnion.builder().build(smlSketch);
        union.update(bigSketch);
        assertTrue (sameStructurePredicate(directSketch, smlSketch));
      }
    }
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions1() {
    DoublesSketch qs1 = DoublesSketch.builder().build(4); // not smaller
    DoublesSketch qs2 = DoublesSketch.builder().build(3);
    HeapDoublesUnion.mergeInto(qs2, qs1);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions2() {
    DoublesSketch qs1 = DoublesSketch.builder().build(4);
    DoublesSketch qs2 = DoublesSketch.builder().build(7); // 7/4 not pwr of 2
    HeapDoublesUnion.mergeInto(qs2, qs1);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDownSamplingExceptions3() {
    DoublesSketch qs1 = DoublesSketch.builder().build(4);
    DoublesSketch qs2 = DoublesSketch.builder().build(12); // 12/4 not pwr of 2
    HeapDoublesUnion.mergeInto(qs2, qs1);
  }

  //@Test  //visual only
  public void quantilesCheckViaMemory() {
    int k = 256;
    long n = 1000000;
    DoublesSketch qs = buildQS(k, n);
    double[] ranks = {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    String s = getRanksTable(qs, ranks);
    println(s);
    println("");
    
    NativeMemory srcMem = new NativeMemory(qs.toByteArray());
    
    HeapDoublesSketch qs2 = HeapDoublesSketch.getInstance(srcMem);
    println(getRanksTable(qs2, ranks));
  }
  
  static String getRanksTable(DoublesSketch qs, double[] ranks) {
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
    String hdr = String.format(
        formatStr1, "Rank", "ValueLB", "<= Value", "<= ValueUB", "RelErrLB", "RelErrUB");
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
        String row = String.format(
            formatStr2,rank, valueLB, value, valueUB, valRelPctErrLB, valRelPctErrUB);
        sb.append(row).append(LS);
      }
    }
    return sb.toString();
  }

  @Test
  public void checkKisOne() {
    DoublesSketch qs1 = DoublesSketch.builder().build(1); //k = 1
    double err = qs1.getNormalizedRankError();
    assertEquals(err, 1.0, 0.0);
  }
  
  @Test
  public void checkPutMemory() {
    DoublesSketch qs1 = DoublesSketch.builder().build(); //k = 128
    for (int i=0; i<1000; i++) qs1.update(i);
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes]);
    qs1.putMemory(dstMem);
    Memory srcMem = dstMem;
    DoublesSketch qs2 = DoublesSketch.heapify(srcMem);
    assertEquals(qs1.getMinValue(), qs2.getMinValue(), 0.0);
    assertEquals(qs1.getMaxValue(), qs2.getMaxValue(), 0.0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkPutMemoryTooSmall() {
    DoublesSketch qs1 = DoublesSketch.builder().build(); //k = 128
    for (int i=0; i<1000; i++) qs1.update(i);
    int bytes = qs1.getStorageBytes();
    Memory dstMem = new NativeMemory(new byte[bytes-1]); //too small
    qs1.putMemory(dstMem);
  }
  
  @Test
  public void checkAuxPosOfPhi() throws Exception {
    long n = 10;
    Method privateMethod = DoublesAuxiliary.class.getDeclaredMethod("posOfPhi", double.class, long.class );
    privateMethod.setAccessible(true);
    long returnValue = (long) privateMethod.invoke(null, new Double(1.0), new Long(10));
    //println("" + returnValue);
    assertEquals(returnValue, n-1);
  }
  
  //Himanshu's case
  @Test
  public void testIt() {
    java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(1<<20);
    NativeMemory mem = new NativeMemory(bb);

    int k = 1024;
    DoublesSketch qsk = new DoublesSketchBuilder().build(k);
    DoublesUnion u1 = DoublesUnion.builder().build(qsk);
    u1.getResult().putMemory(mem);
    DoublesUnion u2 = DoublesUnion.builder().build(mem);
    DoublesSketch qsk2 = u2.getResult();
    assertTrue(qsk2.isEmpty());
  }
  
  @Test
  public void checkEvenlySpacedQuantiles() {
    DoublesSketch qsk = buildQS(32, 1001);
    double[] values = qsk.getQuantiles(11);
//    for (int i = 0; i<values.length; i++) {
//      println(""+values[i]);
//    }
    assertEquals(values[0], 0.0, 0.0);
    assertEquals(values[10], 1000.0, 0.0);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkEvenlySpacedQuantilesException() {
    DoublesSketch qsk = buildQS(32, 1001);
    qsk.getQuantiles(1);
    qsk.getQuantiles(0);
  }
  
  @Test
  public void checkEvenlySpaced() {
    int n = 11;
    double[] es = DoublesSketch.getEvenlySpaced(n);
    int len = es.length;
    for (int j=0; j<len; j++) {
      double f = es[j];
      assertEquals(f, j/10.0, 0.0);
      print(es[j]+", ");
    }
    println("");
  }
  
  @Test
  public void checkPMFonEmpty() {
    DoublesSketch qsk = buildQS(32, 1001);
    double[] array = new double[0];
    double[] qOut = qsk.getQuantiles(array); //check empty array
    println("qOut: "+qOut.length);
    double[] cdfOut = qsk.getCDF(array);
    println("cdfOut: "+cdfOut.length);
    assertEquals(cdfOut[0], 1.0, 0.0);
    
  }
  
  private static void checksForImproperK(int k) {
    String s = "Did not catch improper k: "+k;
    try {
      DoublesSketch.builder().setK(k);
      fail(s);
    } catch (IllegalArgumentException e) {
      //pass
    }
    try {
      DoublesSketch.builder().build(k);
      fail(s);
    } catch (IllegalArgumentException e) {
      //pass
    }
    try {
      HeapDoublesSketch.getInstance(k);
      fail(s);
    } catch (IllegalArgumentException e) {
      //pass
    }
  }
  
  /**
   * Computes a checksum of all the samples in the sketch. Used in testing the Auxiliary
   * @param sketch the given quantiles sketch
   * @return a checksum of all the samples in the sketch
   */
  private static double sumOfSamplesInSketch(HeapDoublesSketch sketch) {
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

  private static double sumOfDoublesInSubArray(double[] arr, int subArrayStart, int subArrayLength) {
    double total = 0.0;
    int subArrayStop = subArrayStart + subArrayLength;
    for (int i = subArrayStart; i < subArrayStop; i++) {
      total += arr[i];
    }
    return total;
  }

  private static boolean sameStructurePredicate(final HeapDoublesSketch mq1, final HeapDoublesSketch mq2) {
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

  static DoublesSketch buildQS(int k, long n) {
    return buildQS(k, n, 0);
  }
  
  static DoublesSketch buildQS(int k, long n, int startV) {
    DoublesSketch qs = DoublesSketch.builder().build(k);
    for (int i=0; i<n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print 
   */
  static void println(String s) {
    print(s+LS);
  }
  
  /**
   * @param s value to print 
   */
  static void print(String s) {
    //System.err.print(s); //disable here
  }

}
