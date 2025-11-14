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

import static java.lang.Math.floor;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.log2;
import static org.apache.datasketches.quantiles.ClassicUtil.computeCombinedBufferItemCapacity;
import static org.apache.datasketches.quantiles.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles.HeapUpdateDoublesSketch.checkPreLongsFlagsSerVer;
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.equallySpacedDoubles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.QuantilesUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HeapUpdateDoublesSketchTest {

  @BeforeMethod
  public void setUp() {
    QuantilesDoublesSketch.rand.setSeed(32749); // make sketches deterministic for testing
  }

  // Please note that this is a randomized test that could probabilistically fail
  // if we didn't set the seed. (The probability of failure could be reduced by increasing k.)
  // Setting the seed has now made it deterministic.
  @Test
  public void checkEndToEnd() {
    final int k = 256;
    final UpdatableQuantilesDoublesSketch qs = QuantilesDoublesSketch.builder().setK(k).build();
    final UpdatableQuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().setK(k).build();
    final int n = 1000000;
    for (int item = n; item >= 1; item--) {
      if ((item % 4) == 0) {
        qs.update(item);
      }
      else {
        qs2.update(item);
      }
    }
    assertEquals(qs.getN() + qs2.getN(), n);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.heapify(qs);
    union.union(qs2);
    final QuantilesDoublesSketch result = union.getResult();

    final int numPhiValues = 99;
    final double[] phiArr = new double[numPhiValues];
    for (int q = 1; q <= 99; q++) {
      phiArr[q-1] = q / 100.0;
    }
    final double[] splitPoints = result.getQuantiles(phiArr);

//    for (int i = 0; i < 99; i++) {
//      String s = String.format("%d\t%.6f\t%.6f", i, phiArr[i], splitPoints[i]);
//      println(s);
//    }

    for (int q = 1; q <= 99; q++) {
      final double nominal = (1e6 * q) / 100.0;
      final double reported = splitPoints[q-1];
      assertTrue(reported >= (nominal - 10000.0));
      assertTrue(reported <= (nominal + 10000.0));
    }

    final double[] pmfResult = result.getPMF(splitPoints);
    double subtotal = 0.0;
    for (int q = 1; q <= 100; q++) {
      final double phi = q / 100.0;
      subtotal += pmfResult[q-1];
      assertTrue(subtotal >= (phi - 0.01));
      assertTrue(subtotal <= (phi + 0.01));
    }

    final double[] cdfResult = result.getCDF(splitPoints);
    for (int q = 1; q <= 100; q++) {
      final double phi = q / 100.0;
      subtotal = cdfResult[q-1];
      assertTrue(subtotal >= (phi - 0.01));
      assertTrue(subtotal <= (phi + 0.01));
    }

    assertEquals(result.getRank(500000), 0.5, 0.01);
  }

  @Test
  public void checkSmallMinMax () {
    final int k = 32;
    final int n = 8;
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(k).build();
    final UpdatableQuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().setK(k).build();
    final UpdatableQuantilesDoublesSketch qs3 = QuantilesDoublesSketch.builder().setK(k).build();

    for (int i = n; i >= 1; i--) {
      qs1.update(i);
      qs2.update(10+i);
      qs3.update(i);
    }
    assertEquals(qs1.getQuantile (0.0, EXCLUSIVE), 1.0);
    assertEquals(qs1.getQuantile (0.5, EXCLUSIVE), 5.0);
    assertEquals(qs1.getQuantile (1.0, EXCLUSIVE), 8.0);

    assertEquals(qs2.getQuantile (0.0, EXCLUSIVE), 11.0);
    assertEquals(qs2.getQuantile (0.5, EXCLUSIVE), 15.0);
    assertEquals(qs2.getQuantile (1.0, EXCLUSIVE), 18.0);

    assertEquals(qs3.getQuantile (0.0, EXCLUSIVE), 1.0);
    assertEquals(qs3.getQuantile (0.5, EXCLUSIVE), 5.0);
    assertEquals(qs3.getQuantile (1.0, EXCLUSIVE), 8.0);

    final double[] queries = {0.0, 0.5, 1.0};

    final double[] resultsA = qs1.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsA[0], 1.0);
    assertEquals(resultsA[1], 5.0);
    assertEquals(resultsA[2], 8.0);

    final QuantilesDoublesUnion union1 = QuantilesDoublesUnion.heapify(qs1);
    union1.union(qs2);
    final QuantilesDoublesSketch result1 = union1.getResult();

    final QuantilesDoublesUnion union2 = QuantilesDoublesUnion.heapify(qs2);
    union2.union(qs3);
    final QuantilesDoublesSketch result2 = union2.getResult();

    final double[] resultsB = result1.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsB[0], 1.0);
    assertEquals(resultsB[1], 11.0);
    assertEquals(resultsB[2], 18.0);

    final double[] resultsC = result2.getQuantiles(queries, EXCLUSIVE);
    assertEquals(resultsC[0], 1.0);
    assertEquals(resultsC[1], 11.0);
    assertEquals(resultsC[2], 18.0);
  }

  @Test
  public void checkMisc() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 10000;
    final UpdatableQuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    qs.update(Double.NaN); //ignore
    final int n2 = (int)qs.getN();
    assertEquals(n2, n);

    qs.reset();
    assertEquals(qs.getN(), 0);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkToStringDetail() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    UpdatableQuantilesDoublesSketch qs = buildAndLoadQS(k, 0);
    String s = qs.toString();
    s = qs.toString(false, true);
    //println(s);
    qs = buildAndLoadQS(k, n);
    s = qs.toString();
    //println(s);
    s = qs.toString(false, true);
    //println(qs.toString(false, true));

    final int n2 = (int)qs.getN();
    assertEquals(n2, n);
    qs.update(Double.NaN); //ignore
    qs.reset();
    assertEquals(qs.getN(), 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorException() {
    QuantilesDoublesSketch.builder().setK(0).build();
  }

  @Test
  public void checkPreLongsFlagsAndSize() {
    byte[] byteArr;
    final UpdatableQuantilesDoublesSketch ds = QuantilesDoublesSketch.builder().build(); //k = 128
    //empty
    byteArr = ds.toByteArray(true); // compact
    assertEquals(byteArr.length, 8);

    byteArr = ds.toByteArray(false); // not compact
    assertEquals(byteArr.length, 8);
    assertEquals(byteArr[3], EMPTY_FLAG_MASK);

    //not empty
    ds.update(1);
    byteArr = ds.toByteArray(true); // compact
    assertEquals(byteArr.length, 40); //compact, 1 value

    byteArr = ds.toByteArray(false); // not compact
    assertEquals(byteArr.length, 64); // 32 + MIN_K(=2) * 2 * 8 = 64
  }

  @Test
  public void checkPreLongsFlagsSerVerB() {
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK, 1, 1); //38
    checkPreLongsFlagsSerVer(0, 1, 5);               //164
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK, 2, 1); //42
    checkPreLongsFlagsSerVer(0, 2, 2);               //72
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK | COMPACT_FLAG_MASK, 3, 1); //47
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK | COMPACT_FLAG_MASK, 3, 2); //79
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK, 3, 2);  //78
    checkPreLongsFlagsSerVer(COMPACT_FLAG_MASK, 3, 2);//77
    checkPreLongsFlagsSerVer(0, 3, 2);                //76
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreLongsFlagsSerVer3() {
    checkPreLongsFlagsSerVer(EMPTY_FLAG_MASK, 1, 2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetQuantiles() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    final QuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    final double[] frac = {-0.5};
    qs.getQuantiles(frac);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetQuantile() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    final QuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    final double frac = -0.5; //negative not allowed
    qs.getQuantile(frac);
  }

  //@Test  //visual only
  public void summaryCheckViaMemory() {
    final QuantilesDoublesSketch qs = buildAndLoadQS(256, 1000000);
    String s = qs.toString();
    println(s);
    println("");

    final MemorySegment srcSeg = MemorySegment.ofArray(qs.toByteArray());

    final HeapUpdateDoublesSketch qs2 = HeapUpdateDoublesSketch.heapifyInstance(srcSeg);
    s = qs2.toString();
    println(s);
  }

  @Test
  public void checkComputeNumLevelsNeeded() {
    final int n = 1 << 20;
    final int k = PreambleUtil.DEFAULT_K;
    final int lvls1 = computeNumLevelsNeeded(k, n);
    final int lvls2 = (int)Math.max(floor(log2((double)n/k)),0);
    assertEquals(lvls1, lvls2);
  }

  @Test
  public void checkComputeBitPattern() {
    final int n = 1 << 20;
    final int k = PreambleUtil.DEFAULT_K;
    final long bitP = ClassicUtil.computeBitPattern(k, n);
    assertEquals(bitP, n/(2L*k));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkValidateSplitPointsOrder() {
    final double[] arr = {2, 1};
    QuantilesUtil.checkDoublesSplitPointsOrder(arr);
  }

  @Test
  public void checkGetStorageBytes() {
    final int k = PreambleUtil.DEFAULT_K; //128
    QuantilesDoublesSketch qs = buildAndLoadQS(k, 0); //k, n
    int stor = qs.getCurrentCompactSerializedSizeBytes();
    assertEquals(stor, 8);

    qs = buildAndLoadQS(k, 2*k); //forces one level
    stor = qs.getCurrentCompactSerializedSizeBytes();

    int retItems = ClassicUtil.computeRetainedItems(k, 2*k);
    assertEquals(stor, 32 + (retItems << 3));

    qs = buildAndLoadQS(k, (2*k)-1); //just Base Buffer
    stor = qs.getCurrentCompactSerializedSizeBytes();
    retItems = ClassicUtil.computeRetainedItems(k, (2*k)-1);
    assertEquals(stor, 32 + (retItems << 3));
  }

  @Test
  public void checkGetStorageBytes2() {
    final int k = PreambleUtil.DEFAULT_K;
    long v = 1;
    final UpdatableQuantilesDoublesSketch qs = QuantilesDoublesSketch.builder().setK(k).build();
    for (int i = 0; i< 1000; i++) {
      qs.update(v++);
//      for (int j = 0; j < 1000; j++) {
//        qs.update(v++);
//      }
      final byte[] byteArr = qs.toByteArray(false);
      assertEquals(byteArr.length, qs.getCurrentUpdatableSerializedSizeBytes());
    }
  }

  @Test
  public void checkMerge() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(k,n,0);
    final QuantilesDoublesSketch qs2 = buildAndLoadQS(k,0,0); //empty
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.heapify(qs2);
    union.union(qs1);
    final QuantilesDoublesSketch result = union.getResult();
    final double med1 = qs1.getQuantile(0.5);
    final double med2 = result.getQuantile(0.5);
    assertEquals(med1, med2, 0.0);
    //println(med1+","+med2);
  }

  @Test
  public void checkReverseMerge() {
    final int k = PreambleUtil.DEFAULT_K;
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(k,  1000, 0);
    final QuantilesDoublesSketch qs2 = buildAndLoadQS(2*k,1000, 1000);
    final QuantilesDoublesUnion union = QuantilesDoublesUnion.heapify(qs2);
    union.union(qs1); //attempt merge into larger k
    final QuantilesDoublesSketch result = union.getResult();
    assertEquals(result.getK(), k);
  }

  @Test
  public void checkInternalBuildHistogram() {
    final int k = PreambleUtil.DEFAULT_K;
    final int n = 1000000;
    final QuantilesDoublesSketch qs = buildAndLoadQS(k,n,0);
    final double eps = qs.getNormalizedRankError(true);
    //println("EPS:"+eps);
    final double[] spts = {100000, 500000, 900000};
    final double[] fracArr = qs.getPMF(spts);
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
    final int n = 1 << 20;
    final int k = PreambleUtil.DEFAULT_K;
    final long bbCnt = ClassicUtil.computeBaseBufferItems(k, n);
    assertEquals(bbCnt, n % (2L*k));
  }

  @Test
  public void checkToFromByteArray() {
    checkToFromByteArray2(128, 1300); //generates a pattern of 5 -> 101
    checkToFromByteArray2(4, 7);
    checkToFromByteArray2(4, 8);
    checkToFromByteArray2(4, 9);
  }

  private static void checkToFromByteArray2(final int k, final int n) {
    final QuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    byte[] byteArr;
    MemorySegment seg;
    QuantilesDoublesSketch qs2;

    // from compact
    byteArr = qs.toByteArray(true);
    seg = MemorySegment.ofArray(byteArr);
    qs2 = UpdatableQuantilesDoublesSketch.heapify(seg);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(qs.getQuantile(f), qs2.getQuantile(f), 0.0);
    }

    // ordered, non-compact
    byteArr = qs.toByteArray(false);
    seg = MemorySegment.ofArray(byteArr);
    qs2 = QuantilesDoublesSketch.heapify(seg);
    final DoublesSketchAccessor dsa = DoublesSketchAccessor.wrap(qs2, false);
    dsa.sort();
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(qs.getQuantile(f), qs2.getQuantile(f), 0.0);
    }

    // not ordered, not compact
    byteArr = qs.toByteArray(false);
    seg = MemorySegment.ofArray(byteArr);
    qs2 = QuantilesDoublesSketch.heapify(seg);
    for (double f = 0.1; f < 0.95; f += 0.1) {
      assertEquals(qs.getQuantile(f), qs2.getQuantile(f), 0.0);
    }
  }

  @Test
  public void checkEmpty() {
    final int k = PreambleUtil.DEFAULT_K;
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(k, 0);
    final byte[] byteArr = qs1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.heapify(seg);
    assertTrue(qs2.isEmpty());
    final int expectedSizeBytes = 8; //COMBINED_BUFFER + ((2 * MIN_K) << 3);
    assertEquals(byteArr.length, expectedSizeBytes);
    try { qs2.getQuantile(0.5); fail(); } catch (final IllegalArgumentException e) { }
    try { qs2.getQuantiles(new double[] {0.0, 0.5, 1.0}); fail(); } catch (final IllegalArgumentException e) { }
    try { qs2.getRank(0); fail(); } catch (final IllegalArgumentException e) { }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegTooSmall1() {
    final MemorySegment seg = MemorySegment.ofArray(new byte[7]);
    HeapUpdateDoublesSketch.heapifyInstance(seg);
    fail();
    //qs2.getQuantile(0.5);
  }

  //Corruption tests
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSerVer() {
    DoublesUtil.checkDoublesSerVer(0, HeapUpdateDoublesSketch.MIN_HEAP_DOUBLES_SER_VER);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkFamilyID() {
    ClassicUtil.checkFamilyID(3);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSegCapacityException() {
    final int k = PreambleUtil.DEFAULT_K;
    final long n = 1000;
    final int serVer = 3;
    final int combBufItemCap = computeCombinedBufferItemCapacity(k, n);
    final int segCapBytes = (combBufItemCap + 4) << 3;
    final int badCapBytes = segCapBytes - 1; //corrupt
    HeapUpdateDoublesSketch.checkHeapSegCapacity(k, n, false, serVer, badCapBytes);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBufAllocAndCap() {
    final int k = PreambleUtil.DEFAULT_K;
    final long n = 1000;
    final int serVer = 3;
    final int combBufItemCap = computeCombinedBufferItemCapacity(k, n); //non-compact cap
    final int segCapBytes = (combBufItemCap + 4) << 3;
    final int segCapBytesV1 = (combBufItemCap + 5) << 3;
    HeapUpdateDoublesSketch.checkHeapSegCapacity(k, n, false, 1, segCapBytesV1);
    HeapUpdateDoublesSketch.checkHeapSegCapacity(k, n, false, serVer, segCapBytes - 1); //corrupt
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreLongsFlagsCap() {
    final int preLongs = 5;
    final int flags = EMPTY_FLAG_MASK;
    final int segCap = 8;
    ClassicUtil.checkPreLongsFlagsCap(preLongs, flags,  segCap); //corrupt
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPreLongsFlagsCap2() {
    final int preLongs = 5;
    final int flags = 0;
    final int segCap = 8;
    ClassicUtil.checkPreLongsFlagsCap(preLongs, flags,  segCap); //corrupt
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkFlags() {
    final int flags = 1;
    ClassicUtil.checkHeapFlags(flags);
  }

  @Test
  public void checkZeroPatternReturn() {
    final int k = PreambleUtil.DEFAULT_K;
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(k, 64);
    final byte[] byteArr = qs1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    HeapUpdateDoublesSketch.heapifyInstance(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadDownSamplingRatio() {
    final int k1 = 64;
    final QuantilesDoublesSketch qs1 = buildAndLoadQS(k1, k1);
    qs1.downSample(qs1, 2*k1, null, null);//should be smaller
  }

  @Test
  public void checkImproperKvalues() {
    checksForImproperK(0);
    checksForImproperK(1<<16);
  }

  //Primarily visual only tests
  static void testDownSampling(final int bigK, final int smallK) {
    final HeapUpdateDoublesSketch sketch1 = HeapUpdateDoublesSketch.newInstance(bigK);
    final HeapUpdateDoublesSketch sketch2 = HeapUpdateDoublesSketch.newInstance(smallK);
    for (int i = 127; i >= 1; i--) {
      sketch1.update (i);
      sketch2.update (i);
    }
    final HeapUpdateDoublesSketch downSketch =
        (HeapUpdateDoublesSketch)sketch1.downSample(sketch1, smallK, null, null);
    println (LS+"Sk1"+LS);
    String s1, s2, down;
    s1 = sketch1.toString(true, true);
    println(s1);
    println (LS+"Down"+LS);
    down = downSketch.toString(true, true);
    println(down);
    println(LS+"Sk2"+LS);
    s2 = sketch2.toString(true, true);
    println(s2);
    assertEquals(downSketch.getNumRetained(), sketch2.getNumRetained());
  }

  @Test
  public void checkDownSampling() {
    testDownSampling(4,4); //no down sampling
    testDownSampling(16,4);
    //testDownSampling(12,3);
  }

  @Test
  public void testDownSampling2() {
    final HeapUpdateDoublesSketch sketch1 = HeapUpdateDoublesSketch.newInstance(8);
    final HeapUpdateDoublesSketch sketch2 = HeapUpdateDoublesSketch.newInstance(2);
    QuantilesDoublesSketch downSketch;
    downSketch = sketch1.downSample(sketch1, 2, null, null);
    assertTrue(sameStructurePredicate(sketch2, downSketch));
    for (int i = 0; i < 50; i++) {
      sketch1.update (i);
      sketch2.update (i);
      downSketch = sketch1.downSample(sketch1, 2, null, null);
      assertTrue (sameStructurePredicate(sketch2, downSketch));
    }
  }

  @Test
  public void testDownSampling3() {
    final int k1 = 8;
    final int k2 = 2;
    final int n = 50;
    final UpdatableQuantilesDoublesSketch sketch1 = QuantilesDoublesSketch.builder().setK(k1).build();
    final UpdatableQuantilesDoublesSketch sketch2 = QuantilesDoublesSketch.builder().setK(k2).build();
    QuantilesDoublesSketch downSketch;
    for (int i = 0; i < n; i++) {
      sketch1.update (i);
      sketch2.update (i);
      downSketch = sketch1.downSample(sketch1, k2, null, null);
      assertTrue (sameStructurePredicate(sketch2, downSketch));
    }
  }

  @Test //
  public void testDownSampling3withSeg() {
    final int k1 = 8;
    final int k2 = 2;
    final int n = 50;
    final UpdatableQuantilesDoublesSketch sketch1 = QuantilesDoublesSketch.builder().setK(k1).build();
    final UpdatableQuantilesDoublesSketch sketch2 = QuantilesDoublesSketch.builder().setK(k2).build();
    QuantilesDoublesSketch downSketch;
    final int bytes = QuantilesDoublesSketch.getUpdatableStorageBytes(k2, n);
    final MemorySegment seg = MemorySegment.ofArray(new byte[bytes]);
    for (int i = 0; i < n; i++) {
      sketch1.update (i);
      sketch2.update (i);

      downSketch = sketch1.downSample(sketch1, k2, seg, null);
      assertTrue (sameStructurePredicate(sketch2, downSketch));
    }

  }

  @Test
  public void testDownSampling4() {
    for (int n1 = 0; n1 < 50; n1++ ) {
      final HeapUpdateDoublesSketch bigSketch = HeapUpdateDoublesSketch.newInstance(8);
      for (int i1 = 1; i1 <= n1; i1++ ) {
        bigSketch.update(i1);
      }
      for (int n2 = 0; n2 < 50; n2++ ) {
        final HeapUpdateDoublesSketch directSketch = HeapUpdateDoublesSketch.newInstance(2);
        for (int i1 = 1; i1 <= n1; i1++ ) {
          directSketch.update(i1);
        }
        for (int i2 = 1; i2 <= n2; i2++ ) {
          directSketch.update(i2);
        }
        final HeapUpdateDoublesSketch smlSketch = HeapUpdateDoublesSketch.newInstance(2);
        for (int i2 = 1; i2 <= n2; i2++ ) {
          smlSketch.update(i2);
        }
        DoublesMergeImpl.downSamplingMergeInto(bigSketch, smlSketch);
        assertTrue (sameStructurePredicate(directSketch, smlSketch));
      }
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testDownSamplingExceptions1() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(4).build(); // not smaller
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().setK(3).build();
    DoublesMergeImpl.mergeInto(qs2, qs1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testDownSamplingExceptions2() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(4).build();
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().setK(7).build(); // 7/4 not pwr of 2
    DoublesMergeImpl.mergeInto(qs2, qs1);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void testDownSamplingExceptions3() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(4).build();
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.builder().setK(12).build(); // 12/4 not pwr of 2
    DoublesMergeImpl.mergeInto(qs2, qs1);
  }

  //@Test  //visual only
  public void quantilesCheckViaMemory() {
    final int k = 256;
    final int n = 1000000;
    final QuantilesDoublesSketch qs = buildAndLoadQS(k, n);
    final double[] ranks = {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    final String s = getRanksTable(qs, ranks);
    println(s);
    println("");

    final MemorySegment srcSeg = MemorySegment.ofArray(qs.toByteArray());

    final HeapUpdateDoublesSketch qs2 = HeapUpdateDoublesSketch.heapifyInstance(srcSeg);
    println(getRanksTable(qs2, ranks));
  }

  static String getRanksTable(final QuantilesDoublesSketch qs, final double[] ranks) {
    final double rankError = qs.getNormalizedRankError(false);
    final double[] values = qs.getQuantiles(ranks);
    final double maxV = qs.getMaxItem();
    final double minV = qs.getMinItem();
    final double delta = maxV - minV;
    println("Note: This prints the relative value errors for illustration.");
    println("The quantiles sketch does not and can not guarantee relative value errors");

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("N = ").append(qs.getN()).append(LS);
    sb.append("K = ").append(qs.getK()).append(LS);
    final String formatStr1 = "%10s%15s%10s%15s%10s%10s";
    final String formatStr2 = "%10.1f%15.5f%10.0f%15.5f%10.5f%10.5f";
    final String hdr = String.format(
        formatStr1, "Rank", "ValueLB", "<= Value", "<= ValueUB", "RelErrLB", "RelErrUB");
    sb.append(hdr).append(LS);
    for (int i=0; i<ranks.length; i++) {
      final double rank = ranks[i];
      final double value = values[i];
      if (rank == 0.0) { assertEquals(value, minV, 0.0); }
      else if (rank == 1.0) { assertEquals(value, maxV, 0.0); }
      else {
        final double rankUB = rank + rankError;
        final double valueUB = minV + (delta*rankUB);
        final double rankLB = Math.max(rank - rankError, 0.0);
        final double valueLB = minV + (delta*rankLB);
        assertTrue(value < valueUB);
        assertTrue(value > valueLB);

        final double valRelPctErrUB = (valueUB/ value) -1.0;
        final double valRelPctErrLB = (valueLB/ value) -1.0;
        final String row = String.format(
            formatStr2,rank, valueLB, value, valueUB, valRelPctErrLB, valRelPctErrUB);
        sb.append(row).append(LS);
      }
    }
    return sb.toString();
  }

  @Test
  public void checkKisTwo() {
    final int k = 2;
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(k).build();
    final double err = qs1.getNormalizedRankError(false);
    assertTrue(err < 1.0);
    byte[] arr = qs1.toByteArray(true); //8
    assertEquals(arr.length, QuantilesDoublesSketch.getCompactSerialiedSizeBytes(k, 0));
    qs1.update(1.0);
    arr = qs1.toByteArray(true); //40
    assertEquals(arr.length, QuantilesDoublesSketch.getCompactSerialiedSizeBytes(k, 1));
  }

  @Test
  public void checkKisTwoDeprecated() {
    final int k = 2;
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().setK(k).build();
    final double err = qs1.getNormalizedRankError(false);
    assertTrue(err < 1.0);
    byte[] arr = qs1.toByteArray(true); //8
    assertEquals(arr.length, QuantilesDoublesSketch.getCompactSerialiedSizeBytes(k, 0));
    assertEquals(arr.length, qs1.getCurrentCompactSerializedSizeBytes());
    qs1.update(1.0);
    arr = qs1.toByteArray(true); //40
    assertEquals(arr.length, QuantilesDoublesSketch.getCompactSerialiedSizeBytes(k, 1));
    assertEquals(arr.length, qs1.getCurrentCompactSerializedSizeBytes());
  }

  @Test
  public void checkPutMemory() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build(); //k = 128
    for (int i=0; i<1000; i++) {
      qs1.update(i);
    }
    final int bytes = qs1.getCurrentUpdatableSerializedSizeBytes();
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes]);
    qs1.putIntoMemorySegment(dstSeg, false);
    final MemorySegment srcSeg = dstSeg;
    final QuantilesDoublesSketch qs2 = QuantilesDoublesSketch.heapify(srcSeg);
    assertEquals(qs1.getMinItem(), qs2.getMinItem(), 0.0);
    assertEquals(qs1.getMaxItem(), qs2.getMaxItem(), 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkPutMemoryTooSmall() {
    final UpdatableQuantilesDoublesSketch qs1 = QuantilesDoublesSketch.builder().build(); //k = 128
    for (int i=0; i<1000; i++) {
      qs1.update(i);
    }
    final int bytes = qs1.getCurrentCompactSerializedSizeBytes();
    final MemorySegment dstSeg = MemorySegment.ofArray(new byte[bytes-1]); //too small
    qs1.putIntoMemorySegment(dstSeg);
  }

  //Himanshu's case
  @Test
  public void testIt() {
    final java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(1<<20).order(ByteOrder.nativeOrder());
    final MemorySegment seg = MemorySegment.ofBuffer(bb);

    final int k = 1024;
    final QuantilesDoublesSketch qsk = new QuantilesDoublesSketchBuilder().setK(k).build();
    final QuantilesDoublesUnion u1 = QuantilesDoublesUnion.heapify(qsk);
    u1.getResult().putIntoMemorySegment(seg);
    final QuantilesDoublesUnion u2 = QuantilesDoublesUnion.heapify(seg);
    final QuantilesDoublesSketch qsk2 = u2.getResult();
    assertTrue(qsk2.isEmpty());
  }

  @Test
  public void checkEquallySpacedRanks() {
    final int n = 10;
    final double[] es = equallySpacedDoubles(n);
    final int len = es.length;
    for (int j=0; j<len; j++) {
      final double f = es[j];
      assertEquals(f, j/10.0, (j/10.0) * 0.001);
      print(es[j]+", ");
    }
    println("");
  }

  @Test
  public void checkPMFonEmpty() {
    final QuantilesDoublesSketch qsk = buildAndLoadQS(32, 1001);
    final double[] array = {};
    final double[] qOut = qsk.getQuantiles(array);
    assertEquals(qOut.length, 0);
    println("qOut: "+qOut.length);
    final double[] cdfOut = qsk.getCDF(array);
    println("cdfOut: "+cdfOut.length);
    assertEquals(cdfOut[0], 1.0, 0.0);
  }

  @Test
  public void checkPuts() {
    final long n1 = 1001;
    final UpdatableQuantilesDoublesSketch qsk = buildAndLoadQS(32, (int)n1);
    final long n2 = qsk.getN();
    assertEquals(n2, n1);

    final int bbCnt1 = qsk.getBaseBufferCount();
    final long pat1 = qsk.getBitPattern();

    qsk.putBitPattern(pat1 + 1); //corrupt the pattern
    final long pat2 = qsk.getBitPattern();
    assertEquals(pat1 + 1, pat2);

    qsk.putBaseBufferCount(bbCnt1 + 1); //corrupt the bbCount
    final int bbCnt2 = qsk.getBaseBufferCount();
    assertEquals(bbCnt1 + 1, bbCnt2);

    qsk.putN(n1 + 1); //corrupt N
    final long n3 = qsk.getN();
    assertEquals(n1 + 1, n3);

    assertNull(qsk.getMemorySegment());
  }

  @Test
  public void serializeDeserializeCompact() {
    final UpdatableQuantilesDoublesSketch sketch1 = QuantilesDoublesSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(i);
    }
    UpdatableQuantilesDoublesSketch sketch2;
    sketch2 = (UpdatableQuantilesDoublesSketch) QuantilesDoublesSketch.heapify(MemorySegment.ofArray(sketch1.toByteArray()));
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i + 1000);
    }
    assertEquals(sketch2.getMinItem(), 0.0);
    assertEquals(sketch2.getMaxItem(), 1999.0);
    assertEquals(sketch2.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void serializeDeserializeEmptyNonCompact() {
    final UpdatableQuantilesDoublesSketch sketch1 = QuantilesDoublesSketch.builder().build();
    final byte[] byteArr = sketch1.toByteArray(false); //Ordered, Not Compact, Empty
    assertEquals(byteArr.length, sketch1.getSerializedSizeBytes());
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final UpdatableQuantilesDoublesSketch sketch2 = (UpdatableQuantilesDoublesSketch) QuantilesDoublesSketch.heapify(seg);
    for (int i = 0; i < 1000; i++) {
      sketch2.update(i);
    }
    assertEquals(sketch2.getMinItem(), 0.0);
    assertEquals(sketch2.getMaxItem(), 999.0);
    assertEquals(sketch2.getQuantile(0.5), 500.0, 4.0);
  }

  @Test
  public void getRankAndGetCdfConsistency() {
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().build();
    final int n = 1_000_000;
    final double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }
    { // inclusive = false (default)
      final double[] ranks = sketch.getCDF(values);
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i]), 0.00001, "CDF vs rank for value " + i);
      }
    }
    { // inclusive = true
      final double[] ranks = sketch.getCDF(values, INCLUSIVE);
      for (int i = 0; i < n; i++) {
        assertEquals(ranks[i], sketch.getRank(values[i], INCLUSIVE), 0.00001, "CDF vs rank for value " + i);
      }
    }
  }

  @Test
  public void maxK() {
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().setK(32768).build();
    Assert.assertEquals(sketch.getK(), 32768);
  }

  @Test
  public void checkBounds() {
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      sketch.update(i);
    }
    final double eps = sketch.getNormalizedRankError(false);
    final double est = sketch.getQuantile(0.5);
    final double ub = sketch.getQuantileUpperBound(0.5);
    final double lb = sketch.getQuantileLowerBound(0.5);
    assertEquals(ub, sketch.getQuantile(.5 + eps));
    assertEquals(lb, sketch.getQuantile(0.5 - eps));
    println("Ext     : " + est);
    println("UB      : " + ub);
    println("LB      : " + lb);
  }

  @Test
  public void checkGetKFromEqs() {
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().build();
    final int k = sketch.getK();
    final double eps = QuantilesDoublesSketch.getNormalizedRankError(k, false);
    final double epsPmf = QuantilesDoublesSketch.getNormalizedRankError(k, true);
    final int kEps = QuantilesDoublesSketch.getKFromEpsilon(eps, false);
    final int kEpsPmf = QuantilesDoublesSketch.getKFromEpsilon(epsPmf, true);
    assertEquals(kEps, k);
    assertEquals(kEpsPmf, k);
  }

  @Test
  public void tenItems() {
    final UpdatableQuantilesDoublesSketch sketch = QuantilesDoublesSketch.builder().build();
    for (int i = 1; i <= 10; i++) { sketch.update(i); }
    assertFalse(sketch.isEmpty());
    assertEquals(sketch.getN(), 10);
    assertEquals(sketch.getNumRetained(), 10);
    for (int i = 1; i <= 10; i++) {
      assertEquals(sketch.getRank(i, EXCLUSIVE), (i - 1) / 10.0);
      assertEquals(sketch.getRank(i, INCLUSIVE), i / 10.0);
    }
    // inclusive = false
    assertEquals(sketch.getQuantile(0, EXCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, EXCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.2, EXCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.3, EXCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.4, EXCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.5, EXCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.6, EXCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.7, EXCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.8, EXCLUSIVE), 9);
    assertEquals(sketch.getQuantile(0.9, EXCLUSIVE), 10);
    assertEquals(sketch.getQuantile(1, EXCLUSIVE), 10);
    // inclusive = true
    assertEquals(sketch.getQuantile(0, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.1, INCLUSIVE), 1);
    assertEquals(sketch.getQuantile(0.2, INCLUSIVE), 2);
    assertEquals(sketch.getQuantile(0.3, INCLUSIVE), 3);
    assertEquals(sketch.getQuantile(0.4, INCLUSIVE), 4);
    assertEquals(sketch.getQuantile(0.5, INCLUSIVE), 5);
    assertEquals(sketch.getQuantile(0.6, INCLUSIVE), 6);
    assertEquals(sketch.getQuantile(0.7, INCLUSIVE), 7);
    assertEquals(sketch.getQuantile(0.8, INCLUSIVE), 8);
    assertEquals(sketch.getQuantile(0.9, INCLUSIVE), 9);
    assertEquals(sketch.getQuantile(1, INCLUSIVE), 10);

    // getQuantile() and getQuantiles() equivalence
    {
      // inclusive = false (default)
      final double[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1});
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0), quantiles[i]);
      }
    }
    {
      // inclusive = true
      final double[] quantiles =
          sketch.getQuantiles(new double[] {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1}, INCLUSIVE);
      for (int i = 0; i <= 10; i++) {
        assertEquals(sketch.getQuantile(i / 10.0, INCLUSIVE), quantiles[i]);
      }
    }
  }

  //private methods

  private static void checksForImproperK(final int k) {
    final String s = "Did not catch improper k: " + k;
    try {
      QuantilesDoublesSketch.builder().setK(k);
      fail(s);
    } catch (final SketchesArgumentException e) {
      //pass
    }
    try {
      QuantilesDoublesSketch.builder().setK(k).build();
      fail(s);
    } catch (final SketchesArgumentException e) {
      //pass
    }
    try {
      HeapUpdateDoublesSketch.newInstance(k);
      fail(s);
    } catch (final SketchesArgumentException e) {
      //pass
    }
  }

  private static boolean sameStructurePredicate(final QuantilesDoublesSketch mq1, final QuantilesDoublesSketch mq2) {
    final boolean b1 =
      ( (mq1.getK() == mq2.getK())
        && (mq1.getN() == mq2.getN())
        && (mq1.getCombinedBufferItemCapacity()
            >= ClassicUtil.computeCombinedBufferItemCapacity(mq1.getK(), mq1.getN()))
        && (mq2.getCombinedBufferItemCapacity()
            >= ClassicUtil.computeCombinedBufferItemCapacity(mq2.getK(), mq2.getN()))
        && (mq1.getBaseBufferCount() == mq2.getBaseBufferCount())
        && (mq1.getBitPattern() == mq2.getBitPattern()) );

    final boolean b2;
    if (mq1.isEmpty()) {
      b2 = mq2.isEmpty();
    } else {
      b2 =  (mq1.getMinItem() == mq2.getMinItem()) && (mq1.getMaxItem() == mq2.getMaxItem());
    }
    return b1 && b2;
  }

  static UpdatableQuantilesDoublesSketch buildAndLoadQS(final int k, final int n) {
    return buildAndLoadQS(k, n, 0);
  }

  static UpdatableQuantilesDoublesSketch buildAndLoadQS(final int k, final int n, final int startV) {
    final UpdatableQuantilesDoublesSketch qs = QuantilesDoublesSketch.builder().setK(k).build();
    for (int i=1; i<=n; i++) {
      qs.update(startV + i);
    }
    return qs;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
    print("PRINTING: "+this.getClass().getName() + LS);
  }

  /**
   * @param s value to print
   */
  static void println(final Object o) {
    print(o.toString() + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final Object o) {
    //System.out.print(o.toString()); //disable here
  }

}
