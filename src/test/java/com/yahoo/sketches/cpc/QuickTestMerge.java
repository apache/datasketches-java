/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.cpc.Fm85Merging.getResult;
import static com.yahoo.sketches.cpc.Fm85Merging.mergeInto;
import static com.yahoo.sketches.cpc.Fm85TestingUtil.assertSketchesEqual;
import static com.yahoo.sketches.cpc.Fm85TestingUtil.dualUpdate;
import static com.yahoo.sketches.cpc.Fm85Util.printf;
import static com.yahoo.sketches.cpc.Fm85Util.println;
import static com.yahoo.sketches.hash.MurmurHash3.hash;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class QuickTestMerge {
  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio as a long
  long counter0 = 35538947;  // some arbitrary random number

  long[] getTwoRandomHashes() {
    long[] in = new long[] { counter0 += golden64 };
    return hash(in, DEFAULT_UPDATE_SEED);
  }
  /*
  This test of merging is less exhaustive than testAll.c,
  but is more practical for large values of K.

  gcc -O3 -Wall -pedantic -o quickTestMerge u32Table.c fm85Util.c fm85.c iconEstimator.c fm85Compression.c fm85Merging.c fm85Testing.c quickTestMerge.c

*/

  // Quick Test of Merging (equal K case)

  @SuppressWarnings("unused")
  void quickTest(final int lgK, final long cA, final long cB) {
    long[] twoHashes = new long[2];
    Fm85 skA = new Fm85(lgK);
    Fm85 skB = new Fm85(lgK);
    Fm85 skD = new Fm85(lgK); // direct sketch
    long nA = 0;
    long nB = 0;

    long t0, t1, t2, t3, t4, t5;

    t0 = System.nanoTime();
    while (skA.numCoupons < cA) {
      nA++;
      twoHashes = getTwoRandomHashes();
      dualUpdate(skA, skD, twoHashes[0], twoHashes[1]);
    }
    //printf(" A");
    t1 = System.nanoTime();
    while (skB.numCoupons < cB) {
      nB++;
      twoHashes = getTwoRandomHashes();
      dualUpdate(skB, skD, twoHashes[0], twoHashes[1]);
    }
    //printf("B");
    t2 = System.nanoTime();

    Fm85Merging ugM = new Fm85Merging(lgK);
    mergeInto(ugM, skA);
    //printf(" A");
    t3 = System.nanoTime();

    mergeInto(ugM, skB);
    //printf("B");
    t4 = System.nanoTime();

    Fm85 skR = getResult(ugM);
    //printf(" R ");
    t5 = System.nanoTime();

    //  printf ("(lgK %d)\t", (int) lgK);
    //  printf ("(N %lld = %lld + %lld)\t", nA + nB, nA, nB);
    //  printf ("(C %lld vs %lld (%lld %lld))\t", skR.numCoupons, skD.numCoupons, skA.numCoupons, skB.numCoupons);
    //  printf ("(flavorDAB %d %d %d)\n", (int) determineSketchFlavor (skD), (int) determineSketchFlavor (skA), (int) determineSketchFlavor (skB));

    assert skR.numCoupons == skD.numCoupons;
    assertSketchesEqual(skD, skR, true);
    Flavor fA = Fm85.determineSketchFlavor(skA);
    Flavor fB = Fm85.determineSketchFlavor(skB);

    printf (" (fA,fB) (updAD,BD) (mrgA,B) R (%7s %7s) (%.1f %.1f) (%.1f %.1f) %.1f\n",
      //    500.0 * ((double) (t1 - t0)) / ((double) nA), // 500 is 1000 / 2
      //    500.0 * ((double) (t2 - t1)) / ((double) nB),
      fA, fB,
      (t1 - t0) / 2000.0,  //update A,D to cA
      (t2 - t1) / 2000.0,  //update B,D to cB

      (t3 - t2) / 1000.0,  //merge A
      (t4 - t3) / 1000.0,  //merge B

      (t5 - t4) / 1000.0); //get Result
  }

  /***************************************************************/
  /*
    Note: In terms of normalized nanoseconds, the two stream processing
    costs are fairly reasonable, although 50 nsec seems slightly high.
   */
  /***************************************************************/

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

  /***************************************************************/
  @Test
  void quickTestMain() {
    for (int lgK = 20; lgK < 21; lgK++) {
      println("\nLgK = " + lgK);
      multiQuickTest(lgK);
    }
  }

}
