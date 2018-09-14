/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CpcMerging.getResult;
import static com.yahoo.sketches.cpc.CpcMerging.mergeInto;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class QuickTestMerge {
  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio as a long
  long counter0 = 35538947;  // some arbitrary random number

  /*
  This test of merging is less exhaustive than testAll.c,
  but is more practical for large values of K.
  */

  // Quick Test of Merging (equal K case)

  @SuppressWarnings("unused")
  void quickTest(final int lgK, final long cA, final long cB) {
    CpcSketch skA = new CpcSketch(lgK);
    CpcSketch skB = new CpcSketch(lgK);
    CpcSketch skD = new CpcSketch(lgK); // direct sketch
    long nA = 0;
    long nB = 0;

    long t0, t1, t2, t3, t4, t5;

    t0 = System.nanoTime();
    while (skA.numCoupons < cA) {
      nA++;
      final long in = counter0 += golden64;
      skA.update(in);
      skD.update(in);
    }
    t1 = System.nanoTime();
    while (skB.numCoupons < cB) {
      nB++;
      final long in = counter0 += golden64;
      skB.update(in);
      skD.update(in);
    }
    t2 = System.nanoTime();

    CpcMerging ugM = new CpcMerging(lgK);
    mergeInto(ugM, skA);
    t3 = System.nanoTime();

    mergeInto(ugM, skB);
    t4 = System.nanoTime();

    CpcSketch skR = getResult(ugM);
    t5 = System.nanoTime();

    //  printf ("(lgK %d)\t", (int) lgK);
    //  printf ("(N %lld = %lld + %lld)\t", nA + nB, nA, nB);
    //  printf ("(C %lld vs %lld (%lld %lld))\t", skR.numCoupons, skD.numCoupons, skA.numCoupons, skB.numCoupons);
    //  printf ("(flavorDAB %d %d %d)\n", (int) determineSketchFlavor (skD), (int) determineSketchFlavor (skA), (int) determineSketchFlavor (skB));

    //assert skR.numCoupons == skD.numCoupons;
    //assertSketchesEqual(skD, skR, true);
    CpcSketch.equals(skD, skR, true);
    Flavor fA = CpcSketch.determineSketchFlavor(skA);
    Flavor fB = CpcSketch.determineSketchFlavor(skB);

    printf(" (fA,fB) (updAD,BD) (mrgA,B) R (%7s %7s) (%.1f %.1f) (%.1f %.1f) %.1f\n",
      //    500.0 * ((double) (t1 - t0)) / ((double) nA), // 500 is 1000 / 2
      //    500.0 * ((double) (t2 - t1)) / ((double) nB),
      fA, fB,
      (t1 - t0) / 2E6,  //update A,D to cA
      (t2 - t1) / 2E6,  //update B,D to cB

      (t3 - t2) / 1E6,  //merge A
      (t4 - t3) / 1E6,  //merge B

      (t5 - t4) / 1E6); //get Result
  }

  /*
    Note: In terms of normalized nanoseconds, the two stream processing
    costs are fairly reasonable, although 50 nsec seems slightly high.
   */

  void multiQuickTest(int lgK) {
    int k = 1 << lgK;
    int[] targetC = new int[] { 0, 1, ((3 * k) / 32) - 1, k / 3, k, (7 * k) / 2 };
    //int[] targetC = new int[] { 0, k/16, k/4, k, (7*k)/2 }; //only 5
    //int[] targetC = new int[] {0LL, k/16, k/4, 2*k, 3*k, 4*k};
    int len = targetC.length;
    for (int i = 0; i < len; i++) {
      printf("\n");
      for (int j = 0; j < len; j++) {
        printf("%10d %10d", targetC[i], targetC[j]);
        quickTest(lgK, targetC[i], targetC[j]);
      }
    }
  }

  @Test //Move to characterization
  void quickTestMain() {
    for (int lgK = 12; lgK < 13; lgK++) {
      println("\nLgK = " + lgK);
      multiQuickTest(lgK);
    }
  }

  //@Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * Print with format
   * @param format the given format
   * @param args the args
   */
  static void printf(String format, Object ... args) {
    String out = String.format(format, args);
    print(out);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + "\n");
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }
}
