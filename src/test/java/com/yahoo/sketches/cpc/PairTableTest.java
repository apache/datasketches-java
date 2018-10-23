/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.PairTable.introspectiveInsertionSort;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.Random;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class PairTableTest {
  Random rand = new Random();

  @Test
  public void checkSort() {
    int len = 10;
    int[] arr1 = new int[len];
    for (int i = 0; i < len; i++) {
      int r1 = rand.nextInt(10000);
      arr1[i] = r1;
    }
    introspectiveInsertionSort(arr1, 0, len-1);
    println("");
    for (int i : arr1) { println(""+i); }
  }

  @Test
  public void checkSort2() {
    int len = 10;
    int[] arr2 = new int[len];
    for (int i = 0; i < len; i++) {
      int r1 = rand.nextInt(10000);
      long r2 = 3_000_000_000L;
      arr2[i] = (int) (r2 + r1);
    }
    println("");
    introspectiveInsertionSort(arr2, 0, len-1);
    for (int i : arr2) { println("" + (i & 0XFFFF_FFFFL)); }
  }

  @Test
  public void checkSort3() {
    int len = 20;
    int[] arr3 = new int[len];
    for (int i = 0; i < len; i++) {
      arr3[i] = (len - i) + 1;
    }
    println("");
    introspectiveInsertionSort(arr3, 0, len-1);
    for (int i : arr3) { println(""+i); }
  }

  @Test
  public void checkMerge() {
    int[] arrA = new int[] { 1, 3, 5 };
    int[] arrB = new int[] { 2, 4, 6 };
    int[] arrC = new int[6];
    PairTable.merge(arrA, 0, 3, arrB, 0, 3, arrC, 0);
    for (int i : arrC) { println(""+i); }
  }

  @Test
  public void checkMerge2() {
    int[] arrA = new int[] { 1, 3, 5 };
    int[] arrB = new int[] { 2, 4, 6 };
    int[] arrC = new int[6];
    for (int i = 0; i < 3; i++) {
      arrA[i] = (int) (arrA[i] + 3_000_000_000L);
      arrB[i] = (int) (arrB[i] + 3_000_000_000L);
    }
    PairTable.merge(arrA, 0, 3, arrB, 0, 3, arrC, 0);
    for (int i : arrC) { println("" + (i & 0XFFFF_FFFFL)); }
  }

  @SuppressWarnings("unused")
  @Test
  public void checkException() {
    int lgK = 10;
    PairTable a = new PairTable(2, lgK + 6);
    assertEquals(a.getValidBits(), lgK + 6);
    println(a.toString());
    PairTable b = null;
    try {
      PairTable.equals(a, b);
      fail();
    } catch (SketchesArgumentException e) { }
    try {
      PairTable.equals(b, a);
      fail();
    } catch (SketchesArgumentException e) { }
    try {
      PairTable c = new PairTable(1, 16);
      fail();
    } catch (SketchesArgumentException e) { }
  }



  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
