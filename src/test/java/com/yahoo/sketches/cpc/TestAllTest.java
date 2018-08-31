/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.pwr2LawNext;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {

  //Streaming
  int streamingNumTrials = 10;

  public void streamingDoAStreamLength(int lgK, long n) {
    //int k = 1 << lgK;
    int trialNo = 0;
    printf ("%d %d", lgK, n);
    Flavor flavor = null;
    long c = 0;
    int offset = 0;
    double avgC = 0.0;
    double avgIconEst = 0.0;
    double avgHIPEst = 0.0;
    long[] twoHashes = new long[2];
    Fm85Testing testing = new Fm85Testing();
    Fm85 sketch = new Fm85(lgK);
    Simple85 simple = new Simple85(lgK);

    for (trialNo = 0; trialNo < streamingNumTrials; trialNo++) {
      for (long i = 0; i < n; i++) {
        testing.getTwoRandomHashes(twoHashes);
        Fm85.hashUpdate(sketch, twoHashes[0], twoHashes[1]);
        Simple85.hashUpdate(simple, twoHashes[0], twoHashes[1]);
      }

      flavor = Fm85.determineSketchFlavor(sketch);
      offset = sketch.windowOffset;
      avgC   += sketch.numCoupons;
      avgIconEst += IconEstimator.getIconEstimate(lgK, sketch.numCoupons);
      avgHIPEst  += sketch.hipEstAccum;

      c = sketch.numCoupons;
      assertEquals(sketch.numCoupons, simple.numCoupons);

      long[] matrix = Fm85.bitMatrixOfSketch (sketch);

      assertEquals(matrix, simple.bitMatrix);

    }
    printf(" (%d %s %d %.3f %.3f %.3f) okay\n",
      c, flavor, offset, // these are from the final trial
      avgC / (streamingNumTrials),
      avgIconEst / (streamingNumTrials),
      avgHIPEst / (streamingNumTrials) );
  }

  /***************************************************************/
  @Test
  void streamingMain() {
    int lgK = 5;
    long numItems = 0;
    int k = 1 << lgK;

    while (numItems < (1200L * k)) {
      streamingDoAStreamLength(lgK, numItems);
      long prev = numItems;
      numItems = (17 * numItems) / 16;
      if (numItems == prev) {
        numItems += 1;
      }
    }
  }

  @Test
  public void testPwrLaw() {
    int numItems = 1 << 20;
    while (numItems < (1 << 22)) {
      println(""+ numItems);
      numItems = pwr2LawNext(16, numItems);
    }
  }

  @Test
  public void testStreaming() {
    long numItems = 1L << 20;
    while (numItems < (1L << 22)) {
      println(""+ numItems);
      long prev = numItems;
      numItems = (17 * numItems) / 16;
      if (numItems == prev) {
        numItems += 1;
        println("INC");
      }
    }
  }


  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  static void printf(String format, Object ... args) {
    System.out.printf(format, args);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.println(s); //disable here
  }

}
