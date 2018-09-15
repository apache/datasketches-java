/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CpcMerging.mergeInto;
import static com.yahoo.sketches.cpc.CpcSketch.bitMatrixOfSketch;
import static com.yahoo.sketches.cpc.CpcSketch.determineSketchFlavor;
import static com.yahoo.sketches.cpc.CpcUtil.countBitsSetInMatrix;
import static com.yahoo.sketches.cpc.IconEstimator.getIconEstimate;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {
  static final long golden64 = 0x9e3779b97f4a7c13L;  // the golden ratio as a long
  long counter0 = 35538947;  // some arbitrary random number

  //STREAMING

  @Test
  public void streamingCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int trials = 10;
    int ppoN = 1;

    StreamingValidation sVal = new StreamingValidation(
        lgMinK, lgMaxK, trials, ppoN, System.out, null);
    sVal.start();
  }

  //COMPRESSION

  @Test
  public void compressionCharacterizationCheck() {
    int lgMinK = 16;
    int lgMaxK = 16;
    int lgMaxT = 20;//Trials at start
    int lgMinT = 4; //Trials at end
    int lgMulK = 7;
    int uPPO = 16;
    int incLgK = 1;

    CompressionCharacterization cc = new CompressionCharacterization(
        lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK, System.out, null);
    cc.start();
  }

  //MERGING

  void testMerging(final int lgKm, final int lgKa, final int lgKb, final long nA, final long nB) {
    CpcMerging ugM = new CpcMerging(lgKm);

//    int lgKd = ((nA != 0) && (lgKa < lgKm)) ? lgKa : lgKm;
//    lgKd =     ((nB != 0) && (lgKb < lgKd)) ? lgKb : lgKd;
    int lgKd = lgKm;
    if ((lgKa < lgKd) && (nA != 0)) { lgKd = lgKa; } //d = min(a,m) : m
    if ((lgKb < lgKd) && (nB != 0)) { lgKd = lgKb; } //d = min(b,d) : d

    CpcSketch skD = new CpcSketch(lgKd); // direct sketch, updated with both streams

    CpcSketch skA = new CpcSketch(lgKa);
    CpcSketch skB = new CpcSketch(lgKb);

    for (long i = 0; i < nA; i++) {
      final long in = (counter0 += golden64);
      skA.update(in);
      skD.update(in);
    }
    for (long i = 0; i < nB; i++) {
      final long in = (counter0 += golden64);
      skB.update(in);
      skD.update(in);
    }

    mergeInto(ugM, skA);
    mergeInto(ugM, skB);

    final int finalLgKm = ugM.lgK;
    final long[] matrixM = CpcMerging.getBitMatrix(ugM);

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

    CpcSketch skR = CpcMerging.getResult(ugM);
    double iconEstR = getIconEstimate(skR.lgK, skR.numCoupons);
    //assertSketchesEqual(skD, skR, true); //was merged
    CpcSketch.equals(skD, skR, true); //was merged
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

  @Test //move to characterization
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
