/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {
  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio
  long counter0 = 35538947;  // some arbitrary random number

  long[] getTwoRandomHashes(long[] twoHashes) {
    twoHashes[0] = counter0 += golden64;
    twoHashes[1] = 0;
    return hash(twoHashes, DEFAULT_UPDATE_SEED);
  }

  //Streaming
  int streamingNumTrials = 1;

  public void streamingDoAStreamLength(int lgK, long n) {
    int trialNo = 0;
    printf ("LgK=%d, n=%d", lgK, n);

    double avgC = 0.0;
    double avgIconEst = 0.0;
    double avgHIPEst = 0.0;
    long[] twoHashes = new long[2];

    Fm85 sketch = new Fm85(lgK);
    Simple85 simple = new Simple85(lgK);

    for (trialNo = 0; trialNo < streamingNumTrials; trialNo++) {
      sketch.reset();
      simple.reset();
      for (long i = 0; i < n; i++) {
        twoHashes = getTwoRandomHashes(twoHashes);
        Fm85.hashUpdate(sketch, twoHashes[0], twoHashes[1]);
        Simple85.hashUpdate(simple, twoHashes[0], twoHashes[1]);
      }

      avgC   += sketch.numCoupons;
      avgIconEst += IconEstimator.getIconEstimate(lgK, sketch.numCoupons);
      avgHIPEst  += sketch.hipEstAccum;

      assertEquals(sketch.numCoupons, simple.numCoupons);
      long[] matrix = Fm85.bitMatrixOfSketch (sketch);
      assertEquals(matrix, simple.bitMatrix);
    }
    long c = sketch.numCoupons;
    Flavor flavor = Fm85.determineSketchFlavor(sketch);
    int offset = sketch.windowOffset;
    printf(" final(C=%d, %s, Off=%d) avgC=%.3f, avgICON=%.3f, avgHIP=%.3f) okay\n",
      c, flavor, offset, // these are from the final trial
      avgC / (streamingNumTrials),
      avgIconEst / (streamingNumTrials),
      avgHIPEst / (streamingNumTrials) );
  }

  /***************************************************************/
  @Test
  void streamingMain() {
    int lgK = 4;
    long numItems = 1; //start
    int k = 1 << lgK;

    while (numItems < (10)) { //1200L * k
      streamingDoAStreamLength(lgK, numItems);
      numItems = Math.round(pwr2LawNextDouble(16, numItems));
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
