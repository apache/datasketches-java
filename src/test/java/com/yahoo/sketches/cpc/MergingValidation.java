/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.cpc.CpcMerging.mergeInto;
import static com.yahoo.sketches.cpc.CpcSketch.bitMatrixOfSketch;
import static com.yahoo.sketches.cpc.CpcSketch.determineSketchFlavor;
import static com.yahoo.sketches.cpc.CpcUtil.countBitsSetInMatrix;
import static com.yahoo.sketches.cpc.IconEstimator.getIconEstimate;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertTrue;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class MergingValidation {
  private String hfmt;
  private String dfmt;
  private String[] hStrArr;
  private long vIn = 0;

  //inputs
  private int lgMinK;
  private int lgMaxK; //inclusive
  private int lgMulK; //multiplier of K to produce maxNa, maxNb
  private int uPPO;
  private int incLgK; //increment of lgK
  private PrintStream printStream;
  private PrintWriter printWriter;

  public MergingValidation(int lgMinK, int lgMaxK, int lgMulK, int uPPO, int incLgK,
      PrintStream pS, PrintWriter pW) {
    this.lgMinK = lgMinK;
    this.lgMaxK = lgMaxK;
    this.lgMulK = lgMulK;
    this.uPPO = Math.max(uPPO, 1);
    this.incLgK = Math.max(incLgK, 1);
    printStream = pS;
    printWriter = pW;
    assembleFormats();
  }

  public void start() {
    printf(hfmt, (Object[]) hStrArr); //print header
    doRangeOfLgK();
  }

  private void doRangeOfLgK() {
    for (int lgK = lgMinK; lgK <= lgMaxK; lgK += incLgK) {
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

  private void multiTestMerging(final int lgKm, final int lgKa, final int lgKb) {
    final long limA = 1L << (lgKa + lgMulK);
    final long limB = 1L << (lgKa + lgMulK);
    long nA = 0;
    while (nA <= limA) {
      long nB = 0;
      while (nB <= limB) {
        testMerging(lgKm, lgKa, lgKb, nA, nB);
        nB = Math.round(pwr2LawNextDouble(uPPO, nB));
      }
      nA = Math.round(pwr2LawNextDouble(uPPO, nA));
    }
  }

  private void testMerging(final int lgKm, final int lgKa, final int lgKb, final long nA,
      final long nB) {
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
      final long in = (vIn += iGoldenU64);
      skA.update(in);
      skD.update(in);
    }
    for (long i = 0; i < nB; i++) {
      final long in = (vIn += iGoldenU64);
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
    String dOff = Integer.toString(skD.windowOffset);
    String aOff = Integer.toString(skA.windowOffset);
    String bOff = Integer.toString(skB.windowOffset);
    String flavorDoff = flavorD + String.format("%2s",dOff);
    String flavorAoff = flavorA + String.format("%2s",aOff);
    String flavorBoff = flavorB + String.format("%2s",bOff);
    double iconEstD = getIconEstimate(lgKd, cD);

    rtAssertTrue(finalLgKm <= lgKm);
    rtAssertTrue(cM <= (skA.numCoupons + skB.numCoupons));
    rtAssertEquals(cM, cD);

    rtAssertEquals(finalLgKm, lgKd);
    final long[] matrixD = bitMatrixOfSketch(skD);
    rtAssertEquals(matrixM, matrixD);

    CpcSketch skR = CpcMerging.getResult(ugM);
    double iconEstR = getIconEstimate(skR.lgK, skR.numCoupons);
    rtAssertEquals(iconEstD, iconEstR, 0.0);
    rtAssertTrue(CpcSketch.equals(skD, skR, true));

    printf(dfmt, lgKm, lgKa, lgKb, lgKd, nA, nB, (nA + nB),
        flavorAoff, flavorBoff, flavorDoff,
        skA.numCoupons, skB.numCoupons, cD, iconEstR);
  }

  private void printf(String format, Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleFormats() {
    String[][] assy = {
        {"lgKm",        "%4s",  "%4d"},
        {"lgKa",        "%4s",  "%4d"},
        {"lgKb",        "%4s",  "%4d"},
        {"lgKfd",       "%6s",  "%6d"},
        {"nA",          "%12s", "%12d"},
        {"nB",          "%12s", "%12d"},
        {"nA+nB",       "%12s", "%12d"},
        {"Flavor_a",    "%11s", "%11s"},
        {"Flavor_b",    "%11s", "%11s"},
        {"Flavor_fd",   "%11s", "%11s"},
        {"Coupons_a",   "%9s",  "%9d"},
        {"Coupons_b",   "%9s",  "%9d"},
        {"Coupons_fd",  "%9s",  "%9d"},
        {"IconEst_dr",  "%12s", "%,12.0f"}
    };
    int cols = assy.length;
    hStrArr = new String[cols];
    StringBuilder headerFmt = new StringBuilder();
    StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nMerging Validation\n");
    for (int i = 0; i < cols; i++) {
      hStrArr[i] =assy[i][0];
      headerFmt.append(assy[i][1]);
      headerFmt.append((i < (cols - 1)) ? "\t" : "\n");
      dataFmt.append(assy[i][2]);
      dataFmt.append((i < (cols - 1)) ? "\t" : "\n");
    }
    hfmt = headerFmt.toString();
    dfmt = dataFmt.toString();
  }
}
