/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.milliSecToString;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.cpc.Fm85.bitMatrixOfSketch;
import static com.yahoo.sketches.cpc.Fm85.determineSketchFlavor;
import static com.yahoo.sketches.cpc.Fm85Compression.fm85Compress;
import static com.yahoo.sketches.cpc.Fm85Compression.fm85Uncompress;
import static com.yahoo.sketches.cpc.Fm85Merging.mergeInto;
import static com.yahoo.sketches.cpc.Fm85TestingUtil.assertSketchesEqual;
import static com.yahoo.sketches.cpc.Fm85TestingUtil.dualUpdate;
import static com.yahoo.sketches.cpc.Fm85Util.countBitsSetInMatrix;
import static com.yahoo.sketches.cpc.IconEstimator.getIconEstimate;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {
  String streamFmt1 = "%3d\t%d";
  String streamFmt2 = "\t%3d\t%9s\t%6d\t%12.3f\t%12.3f\t%12.3f";
  String compressFmt1 = "%d\t%d\t%d\t%.9f\t%.3f\t%s";
  long[] twoHashes = new long[2];

  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio as a long
  long counter0 = 35538947;  // some arbitrary random number

  long[] getTwoRandomHashes() {
    long[] in = new long[] { counter0 += golden64 };
    return hash(in, DEFAULT_UPDATE_SEED);
  }

  //STREAMING

  public void streamingDoAStreamLength(int lgK, long n, int streamingNumTrials) {
    int trialNo = 0;
    String lineStart = String.format(streamFmt1, lgK, n);
    print(lineStart);

    double avgC = 0.0;
    double avgIconEst = 0.0;
    double avgHIPEst = 0.0;


    Fm85 sketch = new Fm85(lgK);
    Simple85 simple = new Simple85(lgK);

    for (trialNo = 0; trialNo < streamingNumTrials; trialNo++) {
      sketch.reset();
      simple.reset();
      for (long i = 0; i < n; i++) {
        twoHashes = getTwoRandomHashes();
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
    String out = String.format(streamFmt2,
      c, flavor, offset, // these are from the final trial
      avgC / (streamingNumTrials),
      avgIconEst / (streamingNumTrials),
      avgHIPEst / (streamingNumTrials) );
    println(out);
  }

  //@Test  //move to characterization
  void streamingMain() {
    println("\nStreaming Test");
    println("LgK\tn\tFinC\tFinFlavor\tFinOff\tAvgC\tAvgICON\tAvgHIP");
    long n = 1; //start
    int trials = 1;

    for (int lgK = 10; lgK < 11; lgK++) {
      int k = 1 << lgK;

      while (n < (64L * k)) { //1200
        streamingDoAStreamLength(lgK, n, trials);
        n = Math.round(pwr2LawNextDouble(1, n));
      }
    }
  }

  //COMPRESSION

  void compressionDoAStreamLength(int lgK, long n, int numSketches) {
    int k = 1 << lgK;
    int minKN = (int) ((k < n) ? k : n);

    numSketches = Math.max(1, numSketches);

    Fm85[] streamSketches = new Fm85[numSketches];
    Fm85[] compressedSketches = new Fm85[numSketches];
    Fm85[] unCompressedSketches = new Fm85[numSketches];
    long start, time, avgTime;

    //update: fill stream sketches array
    start = System.currentTimeMillis();
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      Fm85 sketch = new Fm85(lgK);
      streamSketches[sketchIndex] = sketch;
      for (long i = 0; i < n; i++) {
        twoHashes = getTwoRandomHashes();
        Fm85.hashUpdate(sketch, twoHashes[0], twoHashes[1]);
      }
    }
    time = System.currentTimeMillis() - start;
    avgTime = Math.round((double) time / numSketches);
    println("  Update     " + numSketches + ", " + n + ", Avg: " + milliSecToString(avgTime));

    //compress
    start = System.currentTimeMillis();
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      compressedSketches[sketchIndex] = fm85Compress (streamSketches[sketchIndex]);
    }
    time = System.currentTimeMillis() - start;
    avgTime = Math.round((double) time / numSketches);
    println("  Compress   " + numSketches + ", " + n + ", Avg: " + milliSecToString(avgTime));

    //uncompress
    start = System.currentTimeMillis();
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      unCompressedSketches[sketchIndex] = fm85Uncompress (compressedSketches[sketchIndex]);
    }
    time = System.currentTimeMillis() - start;
    avgTime = Math.round((double) time / numSketches);
    println("  Uncompress " + numSketches + ", " + n + ", Avg: " + milliSecToString(avgTime));

    double totalC = 0.0;
    double totalW = 0.0;
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      totalC += streamSketches[sketchIndex].numCoupons;
      totalW += compressedSketches[sketchIndex].cwLength + compressedSketches[sketchIndex].csvLength;
    }

    double avgC     = totalC / numSketches;
    double avgBytes = (4.0 * totalW) / numSketches;
    Flavor flavor = Fm85.determineSketchFlavor(unCompressedSketches[numSketches - 1]);
    String out = String.format(compressFmt1, k, n, minKN, avgC / k, avgBytes, flavor);
    println(out);
  }

  //@Test //move to characterization
  void compressionMain() {
    println("\nCompression Test");
    println("K\tN\tminKN\tavgCoK\tavgBytes\tFinal Flavor");
    for (int lgK = 10; lgK < 11; lgK++) {
      int k = 1 << lgK;
      long n = k; //start
      int numSketches = 1;
      while (n < ( 64L * k)) { // 120 * k

        compressionDoAStreamLength(lgK, n, numSketches);
        n = Math.round(pwr2LawNextDouble(1, n));
      }
    }
  }

  //MERGING

  void testMerging(final int lgKm, final int lgKa, final int lgKb, final long nA, final long nB) {
    Fm85Merging ugM = new Fm85Merging(lgKm);

//    int lgKd = ((nA != 0) && (lgKa < lgKm)) ? lgKa : lgKm;
//    lgKd =     ((nB != 0) && (lgKb < lgKd)) ? lgKb : lgKd;
    int lgKd = lgKm;
    if ((lgKa < lgKd) && (nA != 0)) { lgKd = lgKa; } //d = min(a,m) : m
    if ((lgKb < lgKd) && (nB != 0)) { lgKd = lgKb; } //d = min(b,d) : d

    Fm85 skD = new Fm85(lgKd); // direct sketch, updated with both streams

    Fm85 skA = new Fm85(lgKa);
    Fm85 skB = new Fm85(lgKb);

    for (long i = 0; i < nA; i++) {
      twoHashes = getTwoRandomHashes();
      dualUpdate(skA, skD, twoHashes[0], twoHashes[1]);
    }
    for (long i = 0; i < nB; i++) {
      twoHashes = getTwoRandomHashes();
      dualUpdate(skB, skD, twoHashes[0], twoHashes[1]);
    }

    mergeInto(ugM, skA);
    mergeInto(ugM, skB);

    final int finalLgKm = ugM.lgK;
    final long[] matrixM = Fm85Merging.getBitMatrix(ugM);

    final long cM = countBitsSetInMatrix(matrixM);
    final long cD = skD.numCoupons;
    Flavor flavorD = determineSketchFlavor(skD);
    Flavor flavorA = determineSketchFlavor(skA);
    Flavor flavorB = determineSketchFlavor(skB);
    double iconEstD = getIconEstimate(lgKd, cD);

    assert (finalLgKm <= lgKm);
    assert (cM <= (skA.numCoupons + skB.numCoupons));
    assert (cM == cD);

    assert (finalLgKm == lgKd);
    final long[] matrixD = bitMatrixOfSketch(skD);
    assertEquals(matrixM, matrixD);

    Fm85 skR = Fm85Merging.getResult(ugM);
    double iconEstR = getIconEstimate(skR.lgK, skR.numCoupons);
    assertSketchesEqual(skD, skR, true); //was merged
    printf("(lgK(MFD)AB (%d %d %d) %d %d)", lgKm, ugM.lgK, lgKd, lgKa, lgKb);
    printf("\t(N %d = %d + %d)", nA + nB, nA, nB);
    printf("\t(flavorDAB %s %s %s)", flavorD, flavorA, flavorB);
    printf("\t(cMDAB %d %d %d %d)", cM, cD, skA.numCoupons, skB.numCoupons);
    printf("\t estDR %.3f %.3f\n", iconEstD, iconEstR);
  }

  /***************************************************************/

  void multiTestMerging(final int lgKm, final int lgKa, final int lgKb) {
    printf ("\nTesting lgKm = %d, lgKa = %d, lgKb = %d\n", lgKm, lgKa, lgKb);
    final int kA = 1 << lgKa;
    final int kB = 1 << lgKb;
    final long limA = kA * 30; //2^26 * 30 is less than 2^31-1
    final long limB = kB * 30;
    long nA = 0;
    while (nA < limA) {
      long nB = 0;
      while (nB < limB) {
        testMerging(lgKm, lgKa, lgKb, nA, nB);
        //long nxtB = (11 * nB) / 10;
        long nxtB = (4 * nB) / 3;
        nB = (nxtB > (nB + 1)) ? nxtB : (nB + 1);
      }
      //long nxtA = (11 * nA) / 10;
      long nxtA = (4 * nA) / 3;
      nA = (nxtA > (nA + 1)) ? nxtA : (nA + 1);
    }
  }

  //@Test //move to characterization
  void mergingMain() {
    println("\nMerging Test");

//    int lgK = 12;
//    multiTestMerging(lgK, lgK, lgK);

    for (int lgK = 10; lgK < 11; lgK++) {  //lgKm, lgKa, lgKb
      multiTestMerging(lgK, lgK - 1, lgK - 1);
      multiTestMerging(lgK, lgK - 1, lgK + 0);
      multiTestMerging(lgK, lgK - 1, lgK + 1);

      multiTestMerging(lgK, lgK + 0, lgK - 1);
      multiTestMerging(lgK, lgK + 0, lgK + 0);
      multiTestMerging(lgK, lgK + 0, lgK + 1);

      multiTestMerging(lgK, lgK + 1, lgK - 1);
      multiTestMerging(lgK, lgK + 1, lgK + 0);
      multiTestMerging(lgK, lgK + 1, lgK + 1);
    }
  }

  //@Test
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
    print(s + "\n");
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }

}
