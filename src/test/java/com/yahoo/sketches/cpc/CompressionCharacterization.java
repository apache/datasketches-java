/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.Util.log2;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static com.yahoo.sketches.cpc.CpcCompression.cpcCompress;
import static com.yahoo.sketches.cpc.CpcCompression.cpcUncompress;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertTrue;

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
  private int lgMinT; //Trials at end
  private int lgMaxT; //Trials at start
  private int lgMulK; //multiplier of K to produce maxU
  private int uPPO;
  private int incLgK; //increment of lgK
  private PrintStream printStream;
  private PrintWriter printWriter;

  //intermediates
  private CpcSketch[] streamSketches;
  private CpcSketch[] compressedSketches;
  private CpcSketch[] unCompressedSketches;

  public CompressionCharacterization(int lgMinK, int lgMaxK, int lgMinT, int lgMaxT, int lgMulK,
      int uPPO, int incLgK, PrintStream pS, PrintWriter pW) {
    this.lgMinK = lgMinK;
    this.lgMaxK = lgMaxK;
    this.lgMinT = lgMinT;
    this.lgMaxT = lgMaxT;
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
      doRangeOfNAtLgK(lgK);
    }
  }

  private void doRangeOfNAtLgK(int lgK) {
    long n = 1;
    int lgMaxN = lgK + lgMulK;
    long maxN = 1L << lgMaxN;
    double slope = -(double)(lgMaxT - lgMinT) / lgMaxN;

    while (n <= maxN) {
      double lgT = (slope * log2(n)) + lgMaxT;
      int totTrials = Math.max(ceilingPowerOf2((int) Math.pow(2.0, lgT)), (1 << lgMinT));
      doTrialsAtLgKAtN(lgK, n, totTrials);
      n = Math.round(pwr2LawNextDouble(uPPO, n));
    }
  }

  private void doTrialsAtLgKAtN(int lgK, long n, int totalTrials) {
    int k = 1 << lgK;
    int minNK = (int) ((k < n) ? k : n);
    double nOverK = (double) n / k;
    int lgTotTrials = Integer.numberOfTrailingZeros(totalTrials);
    int lgWaves = Math.max(lgTotTrials - 10, 0);
    int trialsPerWave = 1 << (lgTotTrials - lgWaves);

    streamSketches = new CpcSketch[trialsPerWave];
    compressedSketches = new CpcSketch[trialsPerWave];
    unCompressedSketches = new CpcSketch[trialsPerWave];

    //update: fill, compress, uncompress sketches arrays in waves
    long totalC = 0;
    long totalW = 0;
    long sumCtor_nS = 0;
    long sumUpd_nS = 0;
    long sumCom_nS = 0;
    long sumUnc_nS = 0;
    long t1, t2, t3, t4, t5;
    long start = System.currentTimeMillis();
    //Wave Loop
    for (int w = 0; w < (1 << lgWaves); w++) {

      //Construct array with sketches loop
      t1 = System.nanoTime();
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = new CpcSketch(lgK);
        streamSketches[trial] = sketch;
      }
      t2 = System.nanoTime();
      sumCtor_nS += t2 - t1;

      //Sketch Update loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = streamSketches[trial];
        for (long i = 0; i < n; i++) { //increment loop
          sketch.update(vIn += iGoldenU64);
        }
      }
      t3 = System.nanoTime();
      sumUpd_nS += t3 - t2;
        //totalC += sketch.numCoupons;

      //Compress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = streamSketches[trial];
        CpcSketch comSk = cpcCompress(sketch);
        compressedSketches[trial] = comSk;
        totalC += sketch.numCoupons;
        totalW += comSk.csvLength + comSk.cwLength;
      }
      t4 = System.nanoTime();
      sumCom_nS += t4 - t3;

      //Uncompress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = compressedSketches[trial];
        CpcSketch uncSk = cpcUncompress(sketch);
        unCompressedSketches[trial] = uncSk;
      }
      t5 = System.nanoTime();
      sumUnc_nS += t5 - t4;

      //optional
      for (int trial = 0; trial < trialsPerWave; trial++) {
        rtAssertTrue(CpcSketch.equals(streamSketches[trial], unCompressedSketches[trial], false));
      }
    } // end wave loop
    double total_S = (System.currentTimeMillis() - start) / 1E3;
    final double avgCtor_nS = Math.round((double) sumCtor_nS / totalTrials);
    final double avgUpd_nS = Math.round((double) sumUpd_nS / totalTrials);
    final double avgUpd_nSperN = avgUpd_nS / n;
    final double avgCom_nS = Math.round((double) sumCom_nS / totalTrials);
    final double avgCom_nSperMinNK = avgCom_nS / minNK;
    final double avgUnc_nS = Math.round((double) sumUnc_nS / totalTrials);
    final double avgUnc_nSperMinNK = avgUnc_nS / minNK;

    double avgC = totalC / totalTrials;
    double avgCoK = avgC / k;
    double avgBytes = (4.0 * totalW) / totalTrials;
    int len = unCompressedSketches.length;
    Flavor finFlavor = CpcSketch.determineSketchFlavor(unCompressedSketches[len - 1]);
    String offStr = Integer.toString(unCompressedSketches[len - 1].windowOffset);
    String flavorOff = finFlavor.toString() + String.format("%2s", offStr);
    printf(dfmt, lgK, totalTrials, n, minNK, avgCoK, flavorOff, nOverK, avgBytes,
        avgCtor_nS, avgUpd_nS, avgCom_nS, avgUnc_nS,
        avgUpd_nSperN, avgCom_nSperMinNK, avgUnc_nSperMinNK, total_S);
  }

  private void printf(String format, Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleFormats() {
    String[][] assy = {
        {"lgK",        "%3s",  "%3d"},
        {"Trials",     "%9s",  "%9d"},
        {"n",          "%12s", "%12d"},
        {"MinKN",      "%9s",  "%9d"},
        {"AvgC/K",     "%9s",  "%9.4g"},
        {"FinFlavor",  "%11s",  "%11s"},
        {"N/K",        "%9s",  "%9.4g"},
        {"AvgBytes",   "%9s",  "%9.0f"},
        {"AvgCtor_nS", "%12s", "%,12.1f"},
        {"AvgUpd_nS",  "%12s", "%,12.1f"},
        {"AvgCom_nS",  "%12s", "%,12.1f"},
        {"AvgUnc_nS",  "%12s", "%,12.1f"},
        {"AvgUpd_nSperN",      "%14s", "%,14.1f"},
        {"AvgCom_nSperMinNK",  "%18s", "%,18.1f"},
        {"AvgUnc_nSperMinNK",  "%18s", "%,18.1f"},
        {"Total_S",            "%8s", "%,8.3f"}
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
