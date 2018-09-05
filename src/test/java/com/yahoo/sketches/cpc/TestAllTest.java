/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.cpc.Fm85Compression.fm85Compress;
import static com.yahoo.sketches.cpc.Fm85Compression.fm85Uncompress;
import static com.yahoo.sketches.cpc.Fm85TestingUtil.assertSketchesEqual;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {
  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio
  long counter0 = 35538947;  // some arbitrary random number
  String streamFmt1 = "%d\t%d";
  String streamFmt2 = "\t%d\t%s\t%d\t%.3f\t%.3f\t%.3f";
  String compressFmt1 = "%d\t%d\t%d\t%.9f\t%.3f\t%s";
  String compressFmt2 = "%.6ft%.6f\t%.6f";

  long[] getTwoRandomHashes(long[] twoHashes) {
    twoHashes[0] = counter0 += golden64;
    twoHashes[1] = 0;
    return hash(twoHashes, DEFAULT_UPDATE_SEED);
  }

  //Streaming

  public void streamingDoAStreamLength(int lgK, long n, int streamingNumTrials) {
    int trialNo = 0;
    String lineStart = String.format(streamFmt1, lgK, n);
    print(lineStart);

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
    String out = String.format(streamFmt2,
      c, flavor, offset, // these are from the final trial
      avgC / (streamingNumTrials),
      avgIconEst / (streamingNumTrials),
      avgHIPEst / (streamingNumTrials) );
    println(out);
  }

  /***************************************************************/
  @Test
  void streamingMain() {
    println("LgK\tn\tFinC\tFinFlavor\tFinOff\tAvgC\tAvgICON\tAvgHIP");
    long n = 1; //start
    int trials = 10;

    for (int lgK = 4; lgK < 16; lgK++) {
      int k = 1 << lgK;

      while (n < (1200L * k)) {
        streamingDoAStreamLength(lgK, n, trials);
        n = Math.round(pwr2LawNextDouble(16, n));
      }
    }
  }

/***************************************************************/
/***************************************************************/

  void compressionDoAStreamLength(int lgK, long n, int numSketches, boolean timing) {
    int k = 1 << lgK;
    int minKN = (int) ((k < n) ? k : n);
    long[] twoHashes = new long[2];

    numSketches = (timing) ? numSketches / minKN : numSketches; // was 20M
    numSketches = Math.max(1, numSketches);

    Fm85[] streamSketches = new Fm85[numSketches];
    Fm85[] compressedSketches = new Fm85[numSketches];
    Fm85[] unCompressedSketches = new Fm85[numSketches];

    long before1, after1, before2, after2, before3, after3;

    before1 = System.nanoTime();
    //fill stream sketches array with empty sketches
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      Fm85 sketch = new Fm85(lgK);
      streamSketches[sketchIndex] = sketch;
      for (int i = 0; i < n; i++) {
        twoHashes = getTwoRandomHashes(twoHashes);
        Fm85.hashUpdate(sketch, twoHashes[0], twoHashes[1]);
      }
    }
    after1 = System.nanoTime();

    before2 = System.nanoTime();
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      compressedSketches[sketchIndex] = fm85Compress (streamSketches[sketchIndex]);
    }
    after2 = System.nanoTime();

    before3 = System.nanoTime();
    for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
      unCompressedSketches[sketchIndex] = fm85Uncompress (compressedSketches[sketchIndex]);
    }
    after3 = System.nanoTime();

    if (timing) {
      double fill = (1e3 * (after1 - before1)) / (1.0 * n * numSketches);
      double comp = (1e3 * (after2 - before2)) / (1.0 * minKN * numSketches);
      double ucomp = (1e3 * (after3 - before3)) / (1.0 * minKN * numSketches);
      String out = String.format(compressFmt2, fill, comp, ucomp);
      println(out);
    } else {
      double totalC = 0.0;
      double totalW = 0.0;
      for (int sketchIndex = 0; sketchIndex < numSketches; sketchIndex++) {
        totalC += streamSketches[sketchIndex].numCoupons;
        totalW += compressedSketches[sketchIndex].cwLength + compressedSketches[sketchIndex].csvLength;
        if (timing) {
          assertSketchesEqual(streamSketches[sketchIndex], unCompressedSketches[sketchIndex], false);
        }
      }

      double avgC     = totalC / numSketches;
      double avgBytes = (4.0 * totalW) / numSketches;
      Flavor flavor = Fm85.determineSketchFlavor(unCompressedSketches[0]); //TODO
      String out = String.format(compressFmt1, k, n, minKN, avgC / k, avgBytes, flavor);
      println(out);
    }
  }

  /***************************************************************/
  /***************************************************************/
  @Test
  void compressionMain() {
    int lgK = 4;
    int k = 1 << lgK;

    long n = 1; //start

    boolean timing = false;
    int timingSketches = 20_000_000;
    int nonTimingSketches = 1000;

    int numSketches;
    if (timing) {
      numSketches = timingSketches;
      println("Fill\tCompress\tUncompress");
    } else {
      numSketches = nonTimingSketches;
      println("K\tN\tminKN\tavgCoK\tavgBytes\tFlavor");
    }

    while (n < ( 120 * k)) { // 120 * k
      compressionDoAStreamLength(lgK, n, numSketches, timing);
      n = Math.round(pwr2LawNextDouble(16, n));
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
    print(s + "\n");
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }

}
