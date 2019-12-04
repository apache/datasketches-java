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

package org.apache.datasketches;

import static java.lang.String.format;
import static org.apache.datasketches.QuickSelect.select;
import static org.apache.datasketches.QuickSelect.selectExcludingZeros;
import static org.apache.datasketches.QuickSelect.selectIncludingZeros;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class QuickSelectTest {
  private static final String LS = System.getProperty("line.separator");
  private static final Random random = new Random(); // pseudo-random number generator

  //long[] arrays

  @Test
  public void checkQuickSelect0Based() {
    final int len = 64;
    final long[] arr = new long[len];
    for (int i = 0; i < len; i++ ) {
      arr[i] = i;
    }
    for (int pivot = 0; pivot < 64; pivot++ ) {
      final long trueVal = pivot;
      for (int i = 0; i < 1000; i++ ) {
        shuffle(arr);
        final long retVal = select(arr, 0, len - 1, pivot);
        Assert.assertEquals(retVal, trueVal);
      }
    }
  }

  @Test
  public void checkQuickSelect1BasedExcludingZeros() {
    final int len = 64;
    final int nonZeros = (7 * len) / 8;
    final long[] arr = new long[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    final int pivot = len / 2;
    final long trueVal = arr[pivot - 1];
    shuffle(arr);
    final long retVal = selectExcludingZeros(arr, nonZeros, pivot);
    Assert.assertEquals(retVal, trueVal);
  }

  @Test
  public void checkQuickSelect1BasedExcludingZeros2() {
    final int len = 64;
    final int nonZeros = 16;
    final long[] arr = new long[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    shuffle(arr);
    final int pivot = len / 2;
    final long retVal = selectExcludingZeros(arr, nonZeros, pivot);
    Assert.assertEquals(retVal, 0);
  }

  @Test
  public void checkQuickSelect1BasedIncludingZeros() {
    final int len = 64;
    final int zeros = len / 8;
    final long[] arr = new long[len];
    for (int i = zeros; i < len; i++ ) {
      arr[i] = i + 1;
    }
    final int pivot = len / 2;
    final long trueVal = arr[pivot - 1];
    shuffle(arr);
    final long retVal = selectIncludingZeros(arr, pivot);
    Assert.assertEquals(retVal, trueVal);
  }

  //double[] arrays

  @Test
  public void checkQuickSelectDbl0Based() {
    final int len = 64;
    final double[] arr = new double[len];
    for (int i = 0; i < len; i++ ) {
      arr[i] = i;
    }
    for (int pivot = 0; pivot < 64; pivot++ ) {
      final double trueVal = pivot;
      for (int i = 0; i < 1000; i++ ) {
        shuffle(arr);
        final double retVal = select(arr, 0, len - 1, pivot);
        Assert.assertEquals(retVal, trueVal, 0.0);
      }
    }
  }

  @Test
  public void checkQuickSelectDbl1BasedExcludingZeros() {
    final int len = 64;
    final int nonZeros = (7 * len) / 8;
    final double[] arr = new double[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    final int pivot = len / 2;
    final double trueVal = arr[pivot - 1];
    shuffle(arr);
    final double retVal = selectExcludingZeros(arr, nonZeros, pivot);
    Assert.assertEquals(retVal, trueVal, 0.0);
  }

  @Test
  public void checkQuickSelectDbl1BasedExcludingZeros2() {
    final int len = 64;
    final int nonZeros = 16;
    final double[] arr = new double[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    shuffle(arr);
    final int pivot = len / 2;
    final double retVal = selectExcludingZeros(arr, nonZeros, pivot);
    Assert.assertEquals(retVal, 0, 0.0);
  }

  @Test
  public void checkQuickSelectDbl1BasedIncludingZeros() {
    final int len = 64;
    final int zeros = len / 8;
    final double[] arr = new double[len];
    for (int i = zeros; i < len; i++ ) {
      arr[i] = i + 1;
    }
    final int pivot = len / 2;
    final double trueVal = arr[pivot - 1];
    shuffle(arr);
    final double retVal = selectIncludingZeros(arr, pivot);
    Assert.assertEquals(retVal, trueVal, 0.0);
  }


  /**
   * Rearrange the elements of an array in random order.
   * @param a long array
   */
  public static void shuffle(final long[] a) {
    final int N = a.length;
    for (int i = 0; i < N; i++ ) {
      final int r = i + uniform(N - i); // between i and N-1
      final long temp = a[i];
      a[i] = a[r];
      a[r] = temp;
    }
  }

  /**
   * Rearrange the elements of an array in random order.
   * @param a double array
   */
  public static void shuffle(final double[] a) {
    final int N = a.length;
    for (int i = 0; i < N; i++ ) {
      final int r = i + uniform(N - i); // between i and N-1
      final double temp = a[i];
      a[i] = a[r];
      a[r] = temp;
    }
  }


  /**
   * Returns an integer uniformly between 0 (inclusive) and n (exclusive) where {@code n > 0}
   *
   * @param n the upper exclusive bound
   * @return random integer
   */
  public static int uniform(final int n) {
    if (n <= 0) {
      throw new SketchesArgumentException("n must be positive");
    }
    return random.nextInt(n);
  }

  private static String printArr(final long[] arr) {
    final StringBuilder sb = new StringBuilder();
    final int len = arr.length;
    sb.append(" Base0").append(" Base1").append(" Value").append(LS);
    for (int i = 0; i < len; i++ ) {
      sb
          .append(format("%6d", i)).append(format("%6d", i + 1)).append(format("%6d", arr[i]))
          .append(LS);
    }
    return sb.toString();
  }

  private static String printArr(final double[] arr) {
    final StringBuilder sb = new StringBuilder();
    final int len = arr.length;
    sb.append(" Base0").append(" Base1").append("    Value").append(LS);
    for (int i = 0; i < len; i++ ) {
      sb
          .append(format("%6d", i)).append(format("%6d", i + 1)).append(format("%9.3f", arr[i]))
          .append(LS);
    }
    return sb.toString();
  }

  //For console testing
  static void test1() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final long[] arr = new long[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2;
    println("select(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot: " + pivot);
    final long ret = select(arr, 0, len - 1, pivot);
    println("Return value of 0-based pivot including zeros:");
    println("select(arr, 0, " + (len - 1) + ", " + pivot + ") => " + ret);
    println("0-based index of pivot = pivot = " + (pivot));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  //For console testing
  static void test2() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final long[] arr = new long[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2; //= 8
    println("selectDiscountingZeros(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot= " + pivot);
    final long ret = selectExcludingZeros(arr, nonZeros, pivot);
    println("Return value of 1-based pivot discounting zeros:");
    println("selectDiscountingZeros(arr, " + nonZeros + ", " + pivot + ") => " + ret);
    println("0-based index of pivot= pivot+zeros-1 = " + ((pivot + zeros) - 1));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  //For console testing
  static void test3() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final long[] arr = new long[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2; //= 8
    println("selectIncludingZeros(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot= " + pivot);
    final long ret = selectIncludingZeros(arr, pivot);
    println("Return value of 1-based pivot including zeros:");
    println("selectIncludingZeros(arr, " + pivot + ") => " + ret);
    println("0-based index of pivot= pivot-1 = " + (pivot - 1));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  static void testDbl1() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final double[] arr = new double[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2;
    println("select(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot: " + pivot);
    final double ret = select(arr, 0, len - 1, pivot);
    println("Return value of 0-based pivot including zeros:");
    println("select(arr, 0, " + (len - 1) + ", " + pivot + ") => " + ret);
    println("0-based index of pivot = pivot = " + (pivot));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  //For console testing
  static void testDbl2() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final double[] arr = new double[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2; //= 8
    println("selectDiscountingZeros(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot= " + pivot);
    final double ret = selectExcludingZeros(arr, nonZeros, pivot);
    println("Return value of 1-based pivot discounting zeros:");
    println("selectDiscountingZeros(arr, " + nonZeros + ", " + pivot + ") => " + ret);
    println("0-based index of pivot= pivot+zeros-1 = " + ((pivot + zeros) - 1));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  //For console testing
  static void testDbl3() {
    final int len = 16;
    final int nonZeros = (3 * len) / 4;
    final int zeros = len - nonZeros;
    final double[] arr = new double[len];
    for (int i = 0; i < nonZeros; i++ ) {
      arr[i] = i + 1;
    }
    println("Generated Numbers:");
    println(printArr(arr));
    shuffle(arr);
    println("Randomized Ordering:");
    println(printArr(arr));
    final int pivot = len / 2; //= 8
    println("selectIncludingZeros(...):");
    println("ArrSize : " + len);
    println("NonZeros: " + nonZeros);
    println("Zeros   : " + zeros);
    println("Choose pivot at 1/2 array size, pivot= " + pivot);
    final double ret = selectIncludingZeros(arr, pivot);
    println("Return value of 1-based pivot including zeros:");
    println("selectIncludingZeros(arr, " + pivot + ") => " + ret);
    println("0-based index of pivot= pivot-1 = " + (pivot - 1));
    println("Result Array:" + LS);
    println(printArr(arr));
  }

  //  public static void main(String[] args) {
  //    println(LS+"==LONGS 1=========="+LS);
  //    test1();
  //    println(LS+"==LONGS 2=========="+LS);
  //    test2();
  //    println(LS+"==LONGS 3=========="+LS);
  //    test3();
  //    println(LS+"==DOUBLES 1========"+LS);
  //    testDbl1();
  //    println(LS+"==DOUBLES 2========"+LS);
  //    testDbl2();
  //    println(LS+"==DOUBLES 3========"+LS);
  //    testDbl3();
  //
  //
  //    QuickSelectTest qst = new QuickSelectTest();
  //    qst.checkQuickSelect0Based();
  //    qst.checkQuickSelect1BasedExcludingZeros();
  //    qst.checkQuickSelect1BasedExcludingZeros2();
  //    qst.checkQuickSelect1BasedIncludingZeros();
  //    qst.checkQuickSelectDbl0Based();
  //    qst.checkQuickSelectDbl1BasedExcludingZeros();
  //    qst.checkQuickSelectDbl1BasedExcludingZeros2();
  //    qst.checkQuickSelectDbl1BasedIncludingZeros();
  //
  //  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

  /**
   * @param d value to print
   */
  static void println(final double d) {
    //System.out.println(d); //disable here
  }

}
