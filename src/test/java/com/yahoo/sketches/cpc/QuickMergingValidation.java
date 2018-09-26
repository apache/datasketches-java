/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssert;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * This test of merging is the equal K case and is less exhaustive than TestAlltest
 * but is more practical for large values of K.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class QuickMergingValidation {
  private String hfmt;
  private String dfmt;
  private String[] hStrArr;
  private long vIn = 0;

  //inputs
  private int lgMinK;
  private int lgMaxK; //inclusive
  private int incLgK; //increment of lgK
  private PrintStream printStream;
  private PrintWriter printWriter;

  public QuickMergingValidation(final int lgMinK, final int lgMaxK, final int incLgK,
      final PrintStream ps, final PrintWriter pw) {
    this.lgMinK = lgMinK;
    this.lgMaxK = lgMaxK;
    this.incLgK = incLgK;
    printStream = ps;
    printWriter = pw;
    assembleFormats();
  }

  public void start() {
    printf(hfmt, (Object[]) hStrArr); //print header
    doRangeOfLgK();
  }

  private void doRangeOfLgK() {
    for (int lgK = lgMinK; lgK <= lgMaxK; lgK += incLgK) {
      multiQuickTest(lgK);
    }
  }

  private void multiQuickTest(int lgK) {
    int k = 1 << lgK;
    int[] targetC = new int[] { 0, 1, ((3 * k) / 32) - 1, k / 3, k, (7 * k) / 2 };
    int len = targetC.length;
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        quickTest(lgK, targetC[i], targetC[j]);
      }
    }
  }

  void quickTest(final int lgK, final long cA, final long cB) {
    CpcSketch skA = new CpcSketch(lgK);
    CpcSketch skB = new CpcSketch(lgK);
    CpcSketch skD = new CpcSketch(lgK); // direct sketch

    long t0, t1, t2, t3, t4, t5;

    t0 = System.nanoTime();
    while (skA.numCoupons < cA) {
      final long in = vIn += iGoldenU64;
      skA.update(in);
      skD.update(in);
    }
    t1 = System.nanoTime();
    while (skB.numCoupons < cB) {
      final long in = vIn += iGoldenU64;
      skB.update(in);
      skD.update(in);
    }
    t2 = System.nanoTime();

    CpcUnion ugM = new CpcUnion(lgK);
    ugM.update(skA);
    t3 = System.nanoTime();

    ugM.update(skB);
    t4 = System.nanoTime();

    CpcSketch skR = ugM.getResult();
    t5 = System.nanoTime();

    rtAssert(TestUtil.specialEquals(skD, skR, false, true));
    Flavor fA = skA.getFlavor();
    Flavor fB = skB.getFlavor();
    Flavor fR = skR.getFlavor();
    String aOff = Integer.toString(skA.windowOffset);
    String bOff = Integer.toString(skB.windowOffset);
    String rOff = Integer.toString(skR.windowOffset);
    String fAoff = fA + String.format("%2s",aOff);
    String fBoff = fB + String.format("%2s",bOff);
    String fRoff = fR + String.format("%2s",rOff);
    double updA_mS = (t1 - t0) / 2E6;  //update A,D to cA
    double updB_mS = (t2 - t1) / 2E6;  //update B,D to cB
    double mrgA_mS = (t3 - t2) / 1E6;  //merge A
    double mrgB_mS = (t4 - t3) / 1E6;  //merge B
    double rslt_mS = (t5 - t4) / 1E6;  //get Result

    printf(dfmt, lgK, cA, cB, fAoff, fBoff, fRoff,
        updA_mS, updB_mS, mrgA_mS, mrgB_mS, rslt_mS);
  }


  private void printf(String format, Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleFormats() {
    String[][] assy = {
        {"lgK",         "%3s",  "%3d"},
        {"Ca",          "%10s", "%10d"},
        {"Cb",          "%10s", "%10d"},
        {"Flavor_a",    "%10s", "%10s"},
        {"Flavor_b",    "%10s", "%10s"},
        {"Flavor_m",    "%10s", "%10s"},
        {"updA_mS",     "%9s",  "%9.3f"},
        {"updB_mS",     "%9s",  "%9.3f"},
        {"mrgA_mS",     "%9s",  "%9.3f"},
        {"mrgB_mS",     "%9s",  "%9.3f"},
        {"rslt_mS",     "%9s",  "%9.3f"}
    };
    int cols = assy.length;
    hStrArr = new String[cols];
    StringBuilder headerFmt = new StringBuilder();
    StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nQuick Merging Validation\n");
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
