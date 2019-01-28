/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.iGoldenU64;
import static com.yahoo.sketches.Util.log2;
import static com.yahoo.sketches.Util.pwrLawNextDouble;
import static com.yahoo.sketches.cpc.CompressedState.importFromMemory;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssert;

import java.io.PrintStream;
import java.io.PrintWriter;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

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
  private PrintStream ps;
  private PrintWriter pw;

  //intermediates
  private CpcSketch[] streamSketches;
  private CompressedState[] compressedStates1;
  private WritableMemory[] memoryArr;
  private CompressedState[] compressedStates2;
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
    ps = pS;
    pw = pW;
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
      n = Math.round(pwrLawNextDouble(uPPO, n, true, 2.0));
    }
  }

  private void doTrialsAtLgKAtN(int lgK, long n, int totalTrials) {
    int k = 1 << lgK;
    int minNK = (int) ((k < n) ? k : n);
    double nOverK = (double) n / k;
    int lgTotTrials = Integer.numberOfTrailingZeros(totalTrials);
    int lgWaves = Math.max(lgTotTrials - 10, 0);
    int trialsPerWave = 1 << (lgTotTrials - lgWaves);
    //printf("%d %d %d %d\n", totalTrials, lgTotTrials, 1 << lgWaves, trialsPerWave);
    streamSketches = new CpcSketch[trialsPerWave];
    compressedStates1 = new CompressedState[trialsPerWave];
    memoryArr = new WritableMemory[trialsPerWave];
    compressedStates2 = new CompressedState[trialsPerWave];
    unCompressedSketches = new CpcSketch[trialsPerWave];


    //update: fill, compress, uncompress sketches arrays in waves
    long totalC = 0;
    long totalW = 0;
    long sumCtor_nS = 0;
    long sumUpd_nS = 0;
    long sumCom_nS = 0;
    long sumSer_nS = 0;
    long sumDes_nS = 0;
    long sumUnc_nS = 0;
    long sumEqu_nS = 0;
    long nanoStart, nanoEnd;
    long start = System.currentTimeMillis();
    //Wave Loop
    for (int w = 0; w < (1 << lgWaves); w++) {

      //Construct array with sketches loop
      nanoStart = System.nanoTime();
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = new CpcSketch(lgK);
        streamSketches[trial] = sketch;
      }
      nanoEnd = System.nanoTime();
      sumCtor_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Sketch Update loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = streamSketches[trial];
        for (long i = 0; i < n; i++) { //increment loop
          sketch.update(vIn += iGoldenU64);
        }
      }
      nanoEnd = System.nanoTime();
      sumUpd_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Compress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CpcSketch sketch = streamSketches[trial];
        CompressedState state = CompressedState.compress(sketch);
        compressedStates1[trial] = state;
        totalC += sketch.numCoupons;
        totalW += state.csvLengthInts + state.cwLengthInts;
      }
      nanoEnd = System.nanoTime();
      sumCom_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //State to Memory loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CompressedState state = compressedStates1[trial];
        long cap = state.getRequiredSerializedBytes();
        WritableMemory wmem = WritableMemory.allocate((int) cap);
        state.exportToMemory(wmem);
        memoryArr[trial] = wmem;
      }

      nanoEnd = System.nanoTime();
      sumSer_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Memory to State loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        Memory mem = memoryArr[trial];
        CompressedState state = importFromMemory(mem);
        compressedStates2[trial] = state;
      }

      nanoEnd = System.nanoTime();
      sumDes_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Uncompress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        CompressedState state = compressedStates2[trial];
        CpcSketch uncSk = null;
        uncSk = CpcSketch.uncompress(state, DEFAULT_UPDATE_SEED);
        unCompressedSketches[trial] = uncSk;
      }

      nanoEnd = System.nanoTime();
      sumUnc_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Equals check
      for (int trial = 0; trial < trialsPerWave; trial++) {
        rtAssert(TestUtil.specialEquals(streamSketches[trial], unCompressedSketches[trial], false, false));
      }

      nanoEnd = System.nanoTime();
      sumEqu_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

    } // end wave loop

    double total_S = (System.currentTimeMillis() - start) / 1E3;
    double avgC = (1.0 * totalC) / totalTrials;
    double avgCoK = avgC / k;
    double avgWords = (1.0 * totalW) / totalTrials;
    double avgBytes = (4.0 * totalW) / totalTrials;

    final double avgCtor_nS = Math.round((double) sumCtor_nS / totalTrials);

    final double avgUpd_nS = Math.round((double) sumUpd_nS / totalTrials);
    final double avgUpd_nSperN = avgUpd_nS / n;

    final double avgCom_nS = Math.round((double) sumCom_nS / totalTrials);
    final double avgCom_nSper2C = avgCom_nS / (2.0 * avgC);
    final double avgCom_nSperK = avgCom_nS / k;

    final double avgSer_nS = Math.round((double) sumSer_nS / totalTrials);
    final double avgSer_nSperW = avgSer_nS / avgWords;

    final double avgDes_nS = Math.round((double) sumDes_nS / totalTrials);
    final double avgDes_nSperW = avgDes_nS / avgWords;


    final double avgUnc_nS = Math.round((double) sumUnc_nS / totalTrials);
    final double avgUnc_nSper2C = avgUnc_nS / (2.0 * avgC);
    final double avgUnc_nSperK = avgUnc_nS / k;

    final double avgEqu_nS = Math.round((double) sumEqu_nS / totalTrials);
    final double avgEqu_nSperMinNK = avgEqu_nS / minNK;


    int len = unCompressedSketches.length;
    Flavor finFlavor = unCompressedSketches[len - 1].getFlavor();
    String offStr = Integer.toString(unCompressedSketches[len - 1].windowOffset);
    String flavorOff = finFlavor.toString() + String.format("%2s", offStr);
    printf(dfmt,
        lgK,
        totalTrials,
        n,
        minNK,
        avgCoK,
        flavorOff,
        nOverK,
        avgBytes,
        avgCtor_nS,
        avgUpd_nS,
        avgCom_nS,
        avgSer_nS,
        avgDes_nS,
        avgUnc_nS,
        avgEqu_nS,
        avgUpd_nSperN,
        avgCom_nSper2C,
        avgCom_nSperK,
        avgSer_nSperW,
        avgDes_nSperW,
        avgUnc_nSper2C,
        avgUnc_nSperK,
        avgEqu_nSperMinNK,
        total_S);
  }

  private void printf(String format, Object ... args) {
    if (ps != null) { ps.printf(format, args); }
    if (pw != null) { pw.printf(format, args); }
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
        {"AvgCtor_nS", "%11s", "%11.0f"},
        {"AvgUpd_nS",  "%10s", "%10.4e"},
        {"AvgCom_nS",  "%10s", "%10.0f"},
        {"AvgSer_nS",  "%10s", "%10.2f"},
        {"AvgDes_nS",  "%10s", "%10.2f"},
        {"AvgUnc_nS",  "%10s", "%10.0f"},
        {"AvgEqu_nS",  "%10s", "%10.0f"},
        {"AvgUpd_nSperN",     "%14s", "%14.2f"},
        {"AvgCom_nSper2C",    "%15s", "%15.4g"},
        {"AvgCom_nSperK",     "%14s", "%14.4g"},
        {"AvgSer_nSperW",     "%14s", "%14.2f"},
        {"AvgDes_nSperW",     "%14s", "%14.2f"},
        {"AvgUnc_nSper2C",    "%15s", "%15.4g"},
        {"AvgUnc_nSperK",     "%14s", "%14.4g"},
        {"AvgEqu_nSperMinNK", "%18s", "%18.4g"},
        {"Total_S",           "%8s",  "%8.3f"}
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
