/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.cpc;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.Util.iGoldenU64;
import static org.apache.datasketches.Util.log2;
import static org.apache.datasketches.Util.pwrLawNextDouble;
import static org.apache.datasketches.cpc.CompressedState.importFromMemory;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This code is used both by unit tests, for short running tests,
 * and by the characterization repository for longer running, more exhaustive testing. To be
 * accessible for both, this code is part of the main hierarchy. It is not used during normal
 * production runtime.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
@SuppressWarnings("javadoc")
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


  public CompressionCharacterization(
      final int lgMinK,
      final int lgMaxK,
      final int lgMinT,
      final int lgMaxT,
      final int lgMulK,
      final int uPPO,
      final int incLgK,
      final PrintStream pS,
      final PrintWriter pW) {
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

  private void doRangeOfNAtLgK(final int lgK) {
    long n = 1;
    final int lgMaxN = lgK + lgMulK;
    final long maxN = 1L << lgMaxN;
    final double slope = -(double)(lgMaxT - lgMinT) / lgMaxN;

    while (n <= maxN) {
      final double lgT = (slope * log2(n)) + lgMaxT;
      final int totTrials = Math.max(ceilingPowerOf2((int) Math.pow(2.0, lgT)), (1 << lgMinT));
      doTrialsAtLgKAtN(lgK, n, totTrials);
      n = Math.round(pwrLawNextDouble(uPPO, n, true, 2.0));
    }
  }

  private void doTrialsAtLgKAtN(final int lgK, final long n, final int totalTrials) {
    final int k = 1 << lgK;
    final int minNK = (int) ((k < n) ? k : n);
    final double nOverK = (double) n / k;
    final int lgTotTrials = Integer.numberOfTrailingZeros(totalTrials);
    final int lgWaves = Math.max(lgTotTrials - 10, 0);
    final int trialsPerWave = 1 << (lgTotTrials - lgWaves);
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
    final long start = System.currentTimeMillis();
    //Wave Loop
    for (int w = 0; w < (1 << lgWaves); w++) {

      //Construct array with sketches loop
      nanoStart = System.nanoTime();
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final CpcSketch sketch = new CpcSketch(lgK);
        streamSketches[trial] = sketch;
      }
      nanoEnd = System.nanoTime();
      sumCtor_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Sketch Update loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final CpcSketch sketch = streamSketches[trial];
        for (long i = 0; i < n; i++) { //increment loop
          sketch.update(vIn += iGoldenU64);
        }
      }
      nanoEnd = System.nanoTime();
      sumUpd_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Compress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final CpcSketch sketch = streamSketches[trial];
        final CompressedState state = CompressedState.compress(sketch);
        compressedStates1[trial] = state;
        totalC += sketch.numCoupons;
        totalW += state.csvLengthInts + state.cwLengthInts;
      }
      nanoEnd = System.nanoTime();
      sumCom_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //State to Memory loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final CompressedState state = compressedStates1[trial];
        final long cap = state.getRequiredSerializedBytes();
        final WritableMemory wmem = WritableMemory.allocate((int) cap);
        state.exportToMemory(wmem);
        memoryArr[trial] = wmem;
      }

      nanoEnd = System.nanoTime();
      sumSer_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Memory to State loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final Memory mem = memoryArr[trial];
        final CompressedState state = importFromMemory(mem);
        compressedStates2[trial] = state;
      }

      nanoEnd = System.nanoTime();
      sumDes_nS += nanoEnd - nanoStart;
      nanoStart = nanoEnd;

      //Uncompress loop
      for (int trial = 0; trial < trialsPerWave; trial++) {
        final CompressedState state = compressedStates2[trial];
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

    final double total_S = (System.currentTimeMillis() - start) / 1E3;
    final double avgC = (1.0 * totalC) / totalTrials;
    final double avgCoK = avgC / k;
    final double avgWords = (1.0 * totalW) / totalTrials;
    final double avgBytes = (4.0 * totalW) / totalTrials;

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


    final int len = unCompressedSketches.length;
    final Flavor finFlavor = unCompressedSketches[len - 1].getFlavor();
    final String offStr = Integer.toString(unCompressedSketches[len - 1].windowOffset);
    final String flavorOff = finFlavor.toString() + String.format("%2s", offStr);
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

  private void printf(final String format, final Object ... args) {
    if (ps != null) { ps.printf(format, args); }
    if (pw != null) { pw.printf(format, args); }
  }

  private void assembleFormats() {
    final String[][] assy = {
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
    final int cols = assy.length;
    hStrArr = new String[cols];
    final StringBuilder headerFmt = new StringBuilder();
    final StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nCompression Characterization\n");
    for (int i = 0; i < cols; i++) {
      hStrArr[i] = assy[i][0];
      headerFmt.append(assy[i][1]);
      headerFmt.append((i < (cols - 1)) ? "\t" : "\n");
      dataFmt.append(assy[i][2]);
      dataFmt.append((i < (cols - 1)) ? "\t" : "\n");
    }
    hfmt = headerFmt.toString();
    dfmt = dataFmt.toString();
  }
}
