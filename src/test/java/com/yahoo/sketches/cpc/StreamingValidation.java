/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static org.testng.Assert.assertEquals;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class StreamingValidation {
  private String hfmt;
  private String dfmt;
  private String[] hStrArr;
  private long vIn = 0;

  //inputs
  private int lgMinK;
  private int lgMaxK; //inclusive
  private int trials;
  private int ppoN;
  private PrintStream printStream;
  private PrintWriter printWriter;


  //sketches
  private CpcSketch sketch = null;
  private CpcMatrixSketch matrixSk = null;

  public StreamingValidation(int lgMinK, int lgMaxK, int trials, int ppoN, PrintStream pS,
      PrintWriter pW) {
    this.lgMinK = lgMinK;
    this.lgMaxK = lgMaxK;
    this.trials = trials;
    this.ppoN = ppoN;
    printStream = pS;
    printWriter = pW;
    assembleStrings();
  }

  public void start() {
    printf(hfmt, (Object[]) hStrArr);
    doRangeOfLgK();
  }

  private void doRangeOfLgK() {
    for (int lgK = lgMinK; lgK <= lgMaxK; lgK++) {
      doRangeOfNAtLgK(lgK);
    }
  }

  private void doRangeOfNAtLgK(int lgK) {
    long n = 1;
    long maxN = 64L * (1L << lgK); //1200
    while (n < maxN) {
      doTrialsAtLgKAtN(lgK, n);
      n = Math.round(pwr2LawNextDouble(ppoN, n));
    }
  }

  /**
   * Performs the given number of trials at a lgK and at an N.
   */
  private void doTrialsAtLgKAtN(int lgK, long n) {
    double sumC = 0.0;
    double sumIconEst = 0.0;
    double sumHipEst = 0.0;
    sketch = new CpcSketch(lgK);
    matrixSk = new CpcMatrixSketch(lgK);

    for (int t = 0; t < trials; t++) {
      sketch.reset();
      matrixSk.reset();
      for (long i = 0; i < n; i++) {
        final long in = (vIn += iGoldenU64);
        sketch.update(in);
        matrixSk.update(in);
      }
      sumC   += sketch.numCoupons;
      sumIconEst += IconEstimator.getIconEstimate(lgK, sketch.numCoupons);
      sumHipEst  += sketch.hipEstAccum;
      assertEquals(sketch.numCoupons, matrixSk.numCoupons);
      long[] bitMatrix = CpcSketch.bitMatrixOfSketch (sketch);
      assertEquals(bitMatrix, matrixSk.bitMatrix);
    }
    long finC = sketch.numCoupons;
    Flavor finFlavor = CpcSketch.determineSketchFlavor(sketch);
    int finOff = sketch.windowOffset;
    double avgC = sumC / trials;
    double avgIconEst = sumIconEst / trials;
    double avgHipEst = sumHipEst / trials;
    printf(dfmt, lgK, trials, n, finC, finFlavor, finOff, avgC, avgIconEst, avgHipEst);
  }

  private void printf(String format, Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleStrings() {
    String[][] assy = {
        {"lgK",       "%3s",  "%3d"},
        {"Trials",    "%7s",  "%7d"},
        {"n",         "%8s",  "%8d"},
        {"FinC",      "%8s",  "%8d"},
        {"FinFlavor", "%10s", "%10s"},
        {"FinOff",    "%7s",  "%7d"},
        {"AvgC",      "%12s", "%12.3f"},
        {"AvgICON",   "%12s", "%12.3f"},
        {"AvgHIP",    "%12s", "%12.3f"}
    };
    int cols = assy.length;
    hStrArr = new String[cols];
    StringBuilder headerFmt = new StringBuilder();
    StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nStreaming Validation\n");
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
