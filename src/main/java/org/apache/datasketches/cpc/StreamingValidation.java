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

import static org.apache.datasketches.Util.iGoldenU64;
import static org.apache.datasketches.Util.pwrLawNextDouble;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssertEquals;

import java.io.PrintStream;
import java.io.PrintWriter;

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
  private BitMatrix matrix = null;

  public StreamingValidation(final int lgMinK, final int lgMaxK, final int trials, final int ppoN,
      final PrintStream pS, final PrintWriter pW) {
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

  private void doRangeOfNAtLgK(final int lgK) {
    long n = 1;
    final long maxN = 64L * (1L << lgK); //1200
    while (n < maxN) {
      doTrialsAtLgKAtN(lgK, n);
      n = Math.round(pwrLawNextDouble(ppoN, n, true, 2.0));
    }
  }

  /**
   * Performs the given number of trials at a lgK and at an N.
   * @param lgK the configured lgK
   * @param n the current value of n
   */
  private void doTrialsAtLgKAtN(final int lgK, final long n) {
    double sumC = 0.0;
    double sumIconEst = 0.0;
    double sumHipEst = 0.0;
    sketch = new CpcSketch(lgK);
    matrix = new BitMatrix(lgK);

    for (int t = 0; t < trials; t++) {
      sketch.reset();
      matrix.reset();
      for (long i = 0; i < n; i++) {
        final long in = (vIn += iGoldenU64);
        sketch.update(in);
        matrix.update(in);
      }
      sumC   += sketch.numCoupons;
      sumIconEst += IconEstimator.getIconEstimate(lgK, sketch.numCoupons);
      sumHipEst  += sketch.hipEstAccum;
      rtAssertEquals(sketch.numCoupons, matrix.getNumCoupons());
      final long[] bitMatrix = CpcUtil.bitMatrixOfSketch(sketch);
      rtAssertEquals(bitMatrix, matrix.getMatrix());
    }
    final long finC = sketch.numCoupons;
    final Flavor finFlavor = sketch.getFlavor();
    final int finOff = sketch.windowOffset;
    final double avgC = sumC / trials;
    final double avgIconEst = sumIconEst / trials;
    final double avgHipEst = sumHipEst / trials;
    printf(dfmt, lgK, trials, n, finC, finFlavor, finOff, avgC, avgIconEst, avgHipEst);
  }

  private void printf(final String format, final Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleStrings() {
    final String[][] assy = {
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
    final int cols = assy.length;
    hStrArr = new String[cols];
    final StringBuilder headerFmt = new StringBuilder();
    final StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nStreaming Validation\n");
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
