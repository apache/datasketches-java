/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.cpc.CpcCompression.cpcCompress;
import static com.yahoo.sketches.cpc.CpcCompression.cpcUncompress;
//import static com.yahoo.sketches.cpc.CpcUtil.*;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class CompressionCharacterization {
  private String hfmt;
  private String dfmt;
  private String[] hStrArr;
  private long vIn = 0;

  //inputs
  private int lgMinK;
  private int lgMaxK; //inclusive
  private int maxTrials;
  private int ppoN;
  private int incLgK;
  private boolean timing;
  private PrintStream printStream;
  private PrintWriter printWriter;

  //intermediates
  private CpcSketch[] streamSketches;
  private CpcSketch[] compressedSketches;
  private CpcSketch[] unCompressedSketches;

  public CompressionCharacterization(int lgMinK, int lgMaxK, int maxTrials, int ppoN, int incLgK,
      boolean timing, PrintStream pS, PrintWriter pW) {
    this.lgMinK = lgMinK;
    this.lgMaxK = lgMaxK;
    this.maxTrials = Math.max(1, maxTrials);
    this.ppoN = Math.max(ppoN, 1);
    this.incLgK = Math.max(incLgK, 1);
    this.timing = timing;
    printStream = pS;
    printWriter = pW;
    assembleStrings();
  }

  public void start() {
    printf(hfmt, (Object[]) hStrArr); //print header
    doRangeOfLgK();
  }

  private void doRangeOfLgK() {
    for (int lgK = lgMinK; lgK <= lgMaxK; lgK += incLgK) {
      doRangeOfNAtLgK(lgK);
    }
  }

  private void doRangeOfNAtLgK(int lgK) {
    long n = 1;
    long maxN = 128L * (1L << lgK); //120
    while (n <= maxN) {
      doTrialsAtLgKAtN(lgK, n);
      n = Math.round(pwr2LawNextDouble(ppoN, n));
    }
  }

  private void doTrialsAtLgKAtN(int lgK, long n) {
    int k = 1 << lgK;
    int minNK = (int) ((k < n) ? k : n);
    double nOverK = (double) n / k;
    int trials = timing ? (int) Math.round((double) maxTrials / minNK) : 10;
    streamSketches = new CpcSketch[trials];
    compressedSketches = new CpcSketch[trials];
    unCompressedSketches = new CpcSketch[trials];
    long start, time;
    double avgUpd_nS, avgCom_nS, avgUnc_nS;
    double avgUpd_nSperN, avgCom_nSperMinNK, avgUnc_nSperMinNK;
    //compute

    //update: fill stream sketches array
    start = System.nanoTime();
    for (int sketchIndex = 0; sketchIndex < trials; sketchIndex++) {
      CpcSketch sketch = new CpcSketch(lgK);
      streamSketches[sketchIndex] = sketch;
      for (long i = 0; i < n; i++) {
        final long in = (vIn += iGoldenU64);
        sketch.update(in);
      }
    }
    time = System.nanoTime() - start;
    avgUpd_nS = Math.round((double) time / trials);
    avgUpd_nSperN = avgUpd_nS / n;

    //compress
    start = System.nanoTime();
    for (int sketchIndex = 0; sketchIndex < trials; sketchIndex++) {
      compressedSketches[sketchIndex] = cpcCompress(streamSketches[sketchIndex]);
    }
    time = System.nanoTime() - start;
    avgCom_nS = Math.round((double) time / trials);
    avgCom_nSperMinNK = avgCom_nS / minNK;

    //uncompress
    start = System.nanoTime();
    for (int sketchIndex = 0; sketchIndex < trials; sketchIndex++) {
      unCompressedSketches[sketchIndex] = cpcUncompress(compressedSketches[sketchIndex]);
    }
    time = System.nanoTime() - start;
    avgUnc_nS = Math.round((double) time / trials);
    avgUnc_nSperMinNK = avgUnc_nS / minNK;

    double totalC = 0.0;
    double totalW = 0.0; //words
    for (int sketchIndex = 0; sketchIndex < trials; sketchIndex++) {
      totalC += streamSketches[sketchIndex].numCoupons;
      totalW += compressedSketches[sketchIndex].cwLength + compressedSketches[sketchIndex].csvLength;

    }

    double avgC = totalC / trials;
    double avgCoK = avgC / k;
    double avgBytes = (4.0 * totalW) / trials;
    Flavor finFlavor = CpcSketch.determineSketchFlavor(unCompressedSketches[trials - 1]);
    printf(dfmt, lgK, trials, n, minNK, avgCoK, finFlavor, nOverK, avgBytes,
        avgUpd_nS, avgCom_nS, avgUnc_nS,
        avgUpd_nSperN, avgCom_nSperMinNK, avgUnc_nSperMinNK);
  }

  private void printf(String format, Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleStrings() {
    String[][] assy = {
        {"lgK",        "%3s",  "%3d"},
        {"Trials",     "%7s",  "%7d"},
        {"n",          "%12s", "%12d"},
        {"MinKN",      "%9s",  "%9d"},
        {"AvgC/K",     "%9s",  "%9.4g"},
        {"FinFlavor",  "%9s",  "%9s"},
        {"N/K",        "%9s",  "%9.4g"},
        {"AvgBytes",   "%9s",  "%9.0f"},
        {"AvgUpd_nS",  "%12s", "%,12.1f"},
        {"AvgCom_nS",  "%12s", "%,12.1f"},
        {"AvgUnc_nS",  "%12s", "%,12.1f"},
        {"AvgUpd_nSperN",      "%14s", "%,14.1f"},
        {"AvgCom_nSperMinNK",  "%18s", "%,18.1f"},
        {"AvgUnc_nSperMinNK",  "%18s", "%,18.1f"}
    };
    int cols = assy.length;
    hStrArr = new String[cols];
    StringBuilder headerFmt = new StringBuilder();
    StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nCompression Characterization\n");
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
