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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssertEquals;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssertFalse;
import static org.testng.Assert.assertThrows;

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
    assertThrows(AssertionError.class, () -> rtAssert(false));
    assertThrows(AssertionError.class, () -> rtAssertFalse(true));
    assertThrows(AssertionError.class, () -> rtAssertEquals(1L, 2L));
    assertThrows(AssertionError.class, () -> rtAssertEquals(1.0, 2.0, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals(true, false));
  }

  @Test
  public void checkByteArr() {
    final byte[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new byte[] {1}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new byte[] {1, 3}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (byte[]) null));
    assertThrows(AssertionError.class, () -> rtAssertEquals((byte[]) null, arr));
    rtAssertEquals((byte[]) null, (byte[]) null);
  }

  @Test
  public void checkShortArr() {
    final short[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new short[] {1}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new short[] {1, 3}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (short[]) null));
    assertThrows(AssertionError.class, () -> rtAssertEquals((short[]) null, arr));
    rtAssertEquals((short[]) null, (short[]) null);
  }

  @Test
  public void checkIntArr() {
    final int[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new int[] {1}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new int[] {1, 3}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (int[]) null));
    assertThrows(AssertionError.class, () -> rtAssertEquals((int[]) null, arr));
    rtAssertEquals((int[]) null, (int[]) null);
  }

  @Test
  public void checkLongArr() {
    final long[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new long[] {1}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new long[] {1, 3}));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (long[]) null));
    assertThrows(AssertionError.class, () -> rtAssertEquals((long[]) null, arr));
    rtAssertEquals((long[]) null, (long[]) null);
  }

  @Test
  public void checkFloatArr() {
    final float[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new float[] {1}, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new float[] {1, 3}, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (float[]) null, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals((float[]) null, arr, 0));
    rtAssertEquals((float[]) null, (float[]) null, 0);
  }

  @Test
  public void checkDoubleArr() {
    final double[] arr = {1, 2};
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new double[] {1}, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, new double[] {1, 3}, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals(arr, (double[]) null, 0));
    assertThrows(AssertionError.class, () -> rtAssertEquals((double[]) null, arr, 0));
    rtAssertEquals((double[]) null, (double[]) null, 0);
  }

}
