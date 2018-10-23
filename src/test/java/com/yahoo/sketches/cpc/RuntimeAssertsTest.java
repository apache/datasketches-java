/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssert;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertFalse;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class RuntimeAssertsTest {

  @Test
  public void checkPositives() {
    rtAssertFalse(false);

    short[] shortArr1 = new short[] { 1, 2, 3 };
    short[] shortArr2 = shortArr1.clone();
    rtAssertEquals(shortArr1, shortArr2);
    shortArr1 = null;
    shortArr2 = null;
    rtAssertEquals(shortArr1, shortArr2);

    float[] floatArr1 = new float[] { 1, 2, 3 };
    float[] floatArr2 = floatArr1.clone();
    rtAssertEquals(floatArr1, floatArr2, 0);
    floatArr1 = null;
    floatArr2 = null;
    rtAssertEquals(floatArr1, floatArr2, 0);

    double[] doubleArr1 = new double[] { 1, 2, 3 };
    double[] doubleArr2 = doubleArr1.clone();
    rtAssertEquals(doubleArr1, doubleArr2, 0);
    doubleArr1 = null;
    doubleArr2 = null;
    rtAssertEquals(doubleArr1, doubleArr2, 0);
  }

  @Test
  public void checkSimpleExceptions() {
    try { rtAssert(false); fail(); } catch (AssertionError e) { }
    try { rtAssertFalse(true); fail(); } catch (AssertionError e) { }
    try { rtAssertEquals(1L, 2L); fail(); } catch (AssertionError e) { }
    try { rtAssertEquals(1.0, 2.0, 0); fail(); } catch (AssertionError e) { }
    try { rtAssertEquals(true, false); fail(); } catch (AssertionError e) { }
  }

  @Test
  public void checkByteArr() {
    byte[] arr1 = {1, 2};
    byte[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = new byte[] {1, 3};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2);
  }

  @Test
  public void checkShortArr() {
    short[] arr1 = {1, 2};
    short[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = new short[] {1, 3};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2);
  }

  @Test
  public void checkIntArr() {
    int[] arr1 = {1, 2};
    int[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = new int[] {1, 3};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2);
  }

  @Test
  public void checkLongArr() {
    long[] arr1 = {1, 2};
    long[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = new long[] {1, 3};
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2);
  }

  @Test
  public void checkFloatArr() {
    float[] arr1 = {1, 2};
    float[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = new float[] {1, 3};
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2, 0);
  }

  @Test
  public void checkDoubleArr() {
    double[] arr1 = {1, 2};
    double[] arr2 = {1};
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = new double[] {1, 3};
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = null;
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = arr1;
    arr1 = null;
    try { rtAssertEquals(arr1, arr2, 0); fail(); } catch (AssertionError e) { }
    arr2 = null;
    rtAssertEquals(arr1, arr2, 0);
  }

}
