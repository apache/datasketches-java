/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.hll.HLLSketch.right;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.hll.HLLCache;
import com.yahoo.sketches.hll.HLLSketch;

/**
 * Tests the following classes:
 * <ul>
 * <li>HLLSketch</li>
 * <li>HLLCache</li>
 * </ul>
 * 
 * @author Lee Rhodes
 */
public class HLLSketchTest {
  private static final double errorBound = .05;

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSketchTooSmall() {
    @SuppressWarnings("unused")
    HLLSketch sk1 = new HLLSketch(512);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSketchTooBig() {
    @SuppressWarnings("unused")
    HLLSketch sk1 = new HLLSketch(1 << 21);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSketchCacheTooBig() {
    @SuppressWarnings("unused")
    HLLSketch sk1 = new HLLSketch(1024, 256);
  }

  @Test
  public void checkHLLCacheConstructor() {
    HLLCache cache = new HLLCache(1024, null, 0);
    Assert.assertEquals(cache.getCurrentCount(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkCurCountGTArrSize() {
    @SuppressWarnings("unused")
    HLLCache cache = new HLLCache(4, new long[1], 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkCurCountGTK() {
    @SuppressWarnings("unused")
    HLLCache cache = new HLLCache(4, new long[8], 8);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkCurCountNeg() {
    @SuppressWarnings("unused")
    HLLCache cache = new HLLCache(8, new long[8], -8);
  }

  @Test
  public void checkConstructorOK() {
    HLLCache cache = new HLLCache(8, new long[8], 0);
    Assert.assertEquals(cache.getCurrentCount(), 0);
  }

  /**
   * Utility methods.
   * <p>At empty:<br>
   * Both constructors, isEmpty(), getBins(), getCacheSize(), getEmpties(),
   * getNewSketch(), equalTo(...), getDataSize()</p>
   * 
   * <p>Fill the cache:<br>
   * isEmpty(), getEmpties(), getMemorySize(), getDataSize()</p>
   * 
   * <p>Transfer to bin array:<br>
   * clone(), equalTo(...), getDataSize(), getMemorySize()</p>
   * 
   * <p>Check merge compatible: <br>
   * isMergeCompatible(...)</p>
   * 
   * <p>Check choose cache:<br>
   * getCacheSize()</p>
   */
  @Test
  public void utilityMethodsTests() {
    int bins = 1024;
    int cacheSize = 128;
    HLLSketch sk1, sk2, sk3, sk4;

    sk1 = new HLLSketch(bins);
    //At Empty:
    Assert.assertTrue(sk1.isEmpty());
    Assert.assertEquals(sk1.getBins(), bins);
    Assert.assertEquals(sk1.getCacheSize(), cacheSize);
    Assert.assertEquals(sk1.getEmpties(), bins);

    sk2 = sk1.getNewSketch();
    sk3 = new HLLSketch(bins, cacheSize);
    Assert.assertTrue(sk1.equalTo(sk2));
    Assert.assertTrue(sk1.equalTo(sk3));
    Assert.assertEquals(sk1.getDataSize(), 0);

    //Fill Cache: 
    for (int i = cacheSize; i-- > 0;) {
      sk1.update(i);
    }
    Assert.assertFalse(sk1.isEmpty());
    Assert.assertEquals(sk1.getEmpties(), bins);
    Assert.assertTrue(sk1.getMemorySize() > (cacheSize * 8));
    Assert.assertEquals(sk1.getDataSize(), cacheSize * 8);

    //test transfer to binArr
    for (int i = 1000; i-- > 0;) {
      sk1.update(i);
    }
    sk4 = sk1.clone();
    Assert.assertTrue(sk1.equalTo(sk4));
    Assert.assertEquals(sk1.getDataSize(), bins);
    Assert.assertTrue(sk1.getMemorySize() > bins);

    //test merge compatible
    Assert.assertTrue(sk1.isMergeCompatible(sk2));
    Assert.assertTrue(sk2.isMergeCompatible(sk3));

    //test choose cache
    sk1 = new HLLSketch(8192);
    Assert.assertEquals(sk1.getCacheSize(), 256);
    sk1 = new HLLSketch(8192 * 2);
    Assert.assertEquals(sk1.getCacheSize(), 512);
  }

  /**
   * More Utility Methods:
   * <p>right(), powerOf2(...)
   */
  @Test
  public void checkUtilityMethods2() {
    //right()
    String s = "abc";
    Assert.assertEquals(right(s, 0, ' '), ""); //colwidth < 1
    Assert.assertEquals(right(s, 2, ' '), "bc"); //in > out
    Assert.assertEquals(right(null, 2, 'X'), "XX"); //arg == null
  }

  /**
   * Check estimation methods in initial cache:
   * update{5}*, getVariance*, getEstimate*, getFlajoletEstimate*,
   * getHllEstimate*, getPoissonEstimate*, getCacheCount*, getEmpties*,
   * getBinArr*, merge
   */
  @Test
  public void checkEstimationMethodsInCache() {
    int bins = 1024;
    int cacheSize = 128;
    HLLSketch sk1 = new HLLSketch(bins, cacheSize);
    //5 different updates:
    sk1.update(1L); // => long array of size 1
    sk1.update(1.0);// => long array of size 1
    sk1.update("1");// => byte array of size 1
    sk1.update(new byte[] { 1 }); // => byte array of size 1
    sk1.update(new long[] { 1L, 0L });// => long array of size 2
    sk1.update(new long[] { 0L, 1L });// => long array of size 2

    //Estimate variants
    double expected1 = 6.0;
    Assert.assertEquals(sk1.getEstimate(), expected1); //from cache
    Assert.assertEquals(sk1.getCacheCount(), expected1);
    Assert.assertEquals(sk1.getPoissonEstimate(), 0.0);
    Assert.assertTrue(sk1.getRawHllEstimate() < (0.73 * bins));

    //Variance, etc.
    Assert.assertEquals(sk1.getVariance(), 0.0);
    Assert.assertEquals(sk1.getEmpties(), bins);
    Assert.assertNull(sk1.getBinArr(), "binArr should be null");

    //test cache clone
    HLLSketch sk2 = sk1.clone();
    Assert.assertTrue(sk1.equalTo(sk2));

    //test cache merge
    sk2 = new HLLSketch(bins, cacheSize);
    //merge sk2 with 5 and no overlap
    for (int i = 0; i < 5; i++ ) {
      sk2.update(i + 10);
    }
    sk1.merge(sk2);
    double expected2 = expected1 + 5.0;
    Assert.assertEquals(sk1.getEstimate(), expected2);

    //merge with wrong sketch
    Assert.assertFalse(sk1.isMergeCompatible(new HLLSketch(1024, 64)));
  }

  /**
   * Check estimation variants
   */
  @Test
  public void checkEstimationVariants() {
    int bins = 4096;
    int cacheSize = 1;
    HLLSketch sk1 = new HLLSketch(bins, cacheSize);
    sk1.update(1);
    sk1.update(2); //force data into binArr
    double expected = 2.0;
    Assert.assertTrue(sk1.getFlajoletEstimate() > expected);
    Assert.assertTrue(sk1.getRawHllEstimate() > expected);
    Assert.assertTrue(testBounds(2.0, sk1.getPoissonEstimate()));
  }

  /**
   * Check Estimates for mid-range counts
   * 
   */
  @Test
  public void checkEstimationMethods_MidRangeCounts() {
    int bins = 4096;
    int count = bins;
    HLLSketch sk1 = new HLLSketch(bins);
    for (int i = 0; i < count; i++ ) {
      sk1.update(i);
    }
    //Estimate variants
    double val = sk1.getEstimate();
    Assert.assertTrue(testBounds(count, val), "" + val);
    Assert.assertEquals(sk1.getCacheCount(), 0.0);
    Assert.assertNotNull(sk1.getBinArr(), "binArr should not be null");
  }

  /**
   * Check estimation methods High Counts:<br>
   * getEstimate, getFlajoletEstimate, getHllEstimate, getPoissionEstimate
   * 
   * getVariance, getCacheCount*, getEmpties*,
   * getBinArr*
   * merge
   */
  @Test
  public void checkEstimationMethods_HiCounts() {
    int bins = 1024;
    int cacheSize = 128;
    int count = 100000;
    HLLSketch sk1 = new HLLSketch(bins, cacheSize);
    for (int i = 0; i < count; i++ ) {
      sk1.update(i);
    }
    //Estimate variants
    double val = sk1.getEstimate();
    Assert.assertTrue(testBounds(count, val), "" + val);
    val = sk1.getPoissonEstimate();
    Assert.assertTrue(Double.isInfinite(val), "" + val);
    val = sk1.getFlajoletEstimate();
    Assert.assertTrue(testBounds(count, val), "" + val);
    val = sk1.getRawHllEstimate();
    Assert.assertTrue(testBounds(count, val), "" + val);
    //Variance, etc.
    val = sk1.getVariance();
    Assert.assertTrue(testBounds(1.0861063168474205E7, val), "" + val);
    Assert.assertEquals(sk1.getCacheCount(), 0.0);
    Assert.assertEquals(sk1.getEmpties(), 0);
    Assert.assertNotNull(sk1.getBinArr(), "binArr should not be null");
    //test hi count merge
    HLLSketch sk2 = new HLLSketch(bins, cacheSize);
    for (int i = 0; i < 100000; i++ ) {
      sk2.update(i + 50000);
    }
    sk1.merge(sk2);
    val = sk1.getEstimate();
    Assert.assertTrue(testBounds(150000, val), "" + val);
  }

  /**
   * Test special equalTo cases
   */
  @Test
  public void checkEqualTo() {
    HLLSketch sk1, sk2, sk3, sk4;
    sk1 = new HLLSketch(8192);
    sk2 = new HLLSketch(1024);
    sk3 = new HLLSketch(8192, 128);
    sk4 = sk2.clone();

    //equalTo with wrong sketch
    Assert.assertFalse(sk1.equalTo(sk2));
    Assert.assertTrue(sk1.equalTo(sk1)); //this == that
    Assert.assertFalse(sk1.equalTo(sk2)); //binArrSize not equal
    Assert.assertFalse(sk1.equalTo(sk3));//cacheSize not equal
    for (int i = 0; i < 64; i++ ) {
      sk2.update(i);
    }
    Assert.assertFalse(sk2.equalTo(sk4)); //cache count not equal

    sk2 = new HLLSketch(1024);
    sk4 = new HLLSketch(1024);
    for (int i = 0; i < 250; i++ ) {
      sk2.update(i); //partial fill binsArr
    }
    for (int i = 0; i < 750; i++ ) {
      sk4.update(i); //partial fill binsArr
    }
    Assert.assertFalse(sk2.equalTo(sk4)); //empties are different

    sk2 = new HLLSketch(1024);
    sk4 = new HLLSketch(1024);
    for (int i = 0; i < 10000; i++ ) {
      sk2.update(i); //fill binsArr
    }
    for (int i = 0; i < 12000; i++ ) {
      sk4.update(i); //fill binsArr
    }
    Assert.assertFalse(sk2.equalTo(sk4)); //binArr contents are different

    long[] cacheArr1 = { 1, 2, 3, 4 };
    long[] cacheArr2 = { 2, 3, 4, 5, 0, 0, 0, 0 };

    HLLCache c1 = new HLLCache(4, cacheArr1, 4);
    HLLCache c2 = new HLLCache(8, cacheArr2, 5);
    Assert.assertFalse(c1.equalTo(c2)); //rejects on k

    c1 = new HLLCache(8, cacheArr1, 4);
    Assert.assertFalse(c1.equalTo(c2)); //rejects on count

    c2 = new HLLCache(8, cacheArr2, 4);
    Assert.assertFalse(c1.equalTo(c2)); //rejects on array length

    long[] cacheArr1b = Arrays.copyOf(cacheArr1, 8);
    c1 = new HLLCache(8, cacheArr1b, 4);
    Assert.assertFalse(c1.equalTo(c2)); //rejects on content
  }

  /**
   * Test special update cases
   */
  @Test
  public void checkEstimationMethods_SpecialCases() {
    int bins = 1024;
    int cacheSize = 128;
    HLLSketch sk1 = new HLLSketch(bins, cacheSize);
    sk1.update( -0.0);
    String s = null;
    sk1.update(s); //no update
    sk1.update(""); //no update
    byte[] b = null;
    sk1.update(b); //no update
    b = new byte[0];
    sk1.update(b); //no update
    long[] c = null;
    sk1.update(c); //no update
    c = new long[0];
    sk1.update(c); //no update
    Assert.assertEquals(sk1.getEstimate(), 1.0);

    HLLCache cache = new HLLCache(8);
    cache.updateCache(0L);
    Assert.assertEquals(cache.getCurrentCount(), 0);

  }

  /**
   * Test special merge cases, Empty state, warmup state and null
   */
  @Test
  public void checkSpecialMergeCases_EmptyStates() {
    HLLSketch skNull = null;
    HLLSketch skResult;
    double v;
    //check for that == null
    try {
      HLLSketch skEmpty = new HLLSketch(1024, 128);
      skResult = skEmpty.merge(skNull);
      Assert.fail("Should have thrown IllegalArgumentException");
    } 
    catch (IllegalArgumentException e) {
      //pass
    }

    //check for invalid isMergeCompatible via merge method
    try {
      HLLSketch skEmpty1 = new HLLSketch(1024, 128);
      HLLSketch skEmpty2 = new HLLSketch(1024, 64);
      skResult = skEmpty1.merge(skEmpty2);
      Assert.fail("Should have thrown IllegalArgumentException");
    } 
    catch (IllegalArgumentException e) {
      //pass
    }

    //check empty combinations
    //case: this is empty, that is empty
    {
      HLLSketch skEmpty1 = new HLLSketch(1024, 128);
      HLLSketch skEmpty2 = new HLLSketch(1024, 128);
      skResult = skEmpty1.merge(skEmpty2);
      Assert.assertTrue(skEmpty1.equalTo(skEmpty2));
      Assert.assertTrue(skResult.isEmpty());
    }

    //case: this is empty, that is valid and cache is null
    {
      HLLSketch skEmpty = new HLLSketch(1024, 128);
      HLLSketch skValidWuNull = new HLLSketch(1024, 128);
      for (int i = 0; i < 256; i++ ) {
        skValidWuNull.update(i);
      }
      skResult = skEmpty.merge(skValidWuNull);
      v = skResult.getEstimate();
      Assert.assertTrue(v > 200);
    }

    //case: this is empty, that is valid and cache is valid
    {
      HLLSketch skEmpty = new HLLSketch(1024, 128);
      HLLSketch skValidWuValid = new HLLSketch(1024, 128);
      for (int i = 0; i < 64; i++ ) {
        skValidWuValid.update(i);
      }
      skResult = skEmpty.merge(skValidWuValid);
      v = skResult.getEstimate();
      Assert.assertEquals(v, 64.0, 0.0);
    }

    //case: this is valid/null, that is empty
    {
      HLLSketch skEmpty = new HLLSketch(1024, 128);
      HLLSketch skValidWuValid = new HLLSketch(1024, 128);
      for (int i = 0; i < 256; i++ ) {
        skValidWuValid.update(i);
      }
      skResult = skValidWuValid.merge(skEmpty);
      Assert.assertTrue(skResult.getEstimate() > 200);
    }

    //case: this is valid/valid that is empty
    {
      HLLSketch skEmpty = new HLLSketch(1024, 128);
      HLLSketch skValidWuValid = new HLLSketch(1024, 128);
      for (int i = 0; i < 64; i++ ) {
        skValidWuValid.update(i);
      }
      skResult = skValidWuValid.merge(skEmpty);
      Assert.assertEquals(skResult.getEstimate(), 64.0, 0.0);
    }
  }

  /**
   * Test special merge cases, Cache States
   */
  @Test
  public void checkSpecialMergeCases_CacheState() {
    HLLSketch sk1, sk2, sk3;
    int bins = 1024; //cache = 128
    int count = 32;
    double expected;

    //Case 0.1 within cache limits
    {
      sk1 = new HLLSketch(bins);
      sk2 = new HLLSketch(bins);
      for (int i = 0; i < (count * 2); i++ ) {
        sk1.update(i);
      }
      for (int i = 0; i < (count * 3); i++ ) {
        sk2.update(i);
      }
      sk3 = sk1.merge(sk2);
      expected = count * 3; //overlap & within cache
      Assert.assertEquals(sk3.getEstimate(), expected);
    }

    //Case 0.2 rolls over to binArr
    {
      sk1 = new HLLSketch(bins);
      sk2 = new HLLSketch(bins);
      for (int i = 0; i < (count * 2); i++ ) {
        sk1.update(i);
      }
      for (int i = 0; i < (count * 3); i++ ) {
        sk2.update(i + (count * 2));
      }
      sk3 = sk1.merge(sk2);
      expected = count * 5; //no overlap
      Assert.assertTrue(testBounds(expected, sk3.getEstimate()));
    }

    //Case 1 this binArr valid, that cache valid
    {
      sk1 = new HLLSketch(bins);
      sk2 = new HLLSketch(bins);
      for (int i = 0; i < (count * 5); i++ ) {
        sk1.update(i); //~160
      }
      for (int i = 0; i < (count * 3); i++ ) {
        sk2.update(i); //96, but overlapping
      }
      sk3 = sk1.merge(sk2);
      expected = count * 5;
      Assert.assertTrue(testBounds(expected, sk3.getEstimate()));
    }

    //Case 2 this cache valid, that cache is null (binArr valid)
    {
      sk1 = new HLLSketch(bins);
      sk2 = new HLLSketch(bins);
      for (int i = 0; i < (count * 3); i++ ) {
        sk1.update(i); //96
      }
      for (int i = 0; i < (count * 5); i++ ) {
        sk2.update(i); //~160, but overlapping
      }
      sk3 = sk1.merge(sk2);
      expected = count * 5;
      Assert.assertTrue(testBounds(expected, sk3.getEstimate()), "" + sk3.getEstimate());
    }
  }

  /**
   * Checks the diagnostic "pretty" strings.
   */
  @Test
  public void checkPrettyStrings() {
    int bins = 1024;
    int cacheSize = 128;
    HLLSketch sk1 = new HLLSketch(bins, cacheSize);
    StringBuilder sb = new StringBuilder();
    sb.append(sk1.toString() + LS); //empty
    //tests on sk1: fill the cache only
    for (int i = cacheSize; i-- > 0;) {
      sk1.update(i);
    }
    sb.append(sk1.getSketchDetail() + LS); //pretty string of cache 
    //test transfer to binArr
    for (int i = 1000; i-- > 0;) {
      sk1.update(i);
    }
    sb.append(sk1.getSketchDetail() + LS); //pretty string of binArr

    //HLLCache pretty strings
    HLLCache cache = new HLLCache(4);
    for (int i = 4; i-- > 0;) {
      cache.updateCache(i + 1);
    }
    sb.append(cache.toString(false) + LS); //decimal mode
    //println(sb.toString());
  }

  @Test
  public void checkCompareUnsignedZeroHigh() {
    int result = HLLCache.compareUnsignedZeroHigh(1L, 1L);
    Assert.assertEquals(result, 0);
    result = HLLCache.compareUnsignedZeroHigh(0L, 1L);
    Assert.assertEquals(result, 1);
    result = HLLCache.compareUnsignedZeroHigh(1L, 0L);
    Assert.assertEquals(result, -1);

    long[] arr = { 1L, 2L };
    int idx = HLLCache.getInsertIndex(2L, arr);
    Assert.assertEquals(idx, -1);
  }

  /**
   * Test update with empty/null long[], int[]
   */
  @Test
  public void checkNullEmptyLongIntArr() {
    HLLSketch sk1 = new HLLSketch(1024);
    long[] arr = null;
    sk1.update(arr);
    Assert.assertTrue(sk1.isEmpty());
    arr = new long[0];
    Assert.assertTrue(sk1.isEmpty());
    int[] arr2 = null;
    sk1.update(arr2);
    Assert.assertTrue(sk1.isEmpty());
    arr2 = new int[0];
    Assert.assertTrue(sk1.isEmpty());
  }

  @Test
  public void checkValidLongIntArr() {
    HLLSketch sk1 = new HLLSketch(1024);
    long[] larr = new long[2];
    larr[0] = (2L << 32) | 1L;
    larr[1] = (4L << 32) | 3L;
    sk1.update(larr);
    double est1 = sk1.getEstimate();
    HLLSketch sk2 = new HLLSketch(1024);
    int[] iarr = new int[4];
    iarr[0] = 1;
    iarr[1] = 2;
    iarr[2] = 3;
    iarr[3] = 4;
    sk2.update(iarr);
    double est2 = sk2.getEstimate();
    Assert.assertEquals(est1, est2);
  }

  /**
   * Test that the result value is within 5% of the expected value.
   * 
   * @param expected
   * @param result
   */
  private static boolean testBounds(double expected, double result) {
    return Math.abs((result / expected) - 1.0) <= errorBound;
  }
}
