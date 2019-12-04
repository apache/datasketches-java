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
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * This code is used both by unit tests, for short running tests,
 * and by the characterization repository for longer running, more exhaustive testing. To be
 * accessible for both, this code is part of the main hierarchy. It is not used during normal
 * production runtime.
 *
 * <p>This test of merging is the equal K case and is less exhaustive than TestAlltest
 * but is more practical for large values of K.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
@SuppressWarnings("javadoc")
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

  private void multiQuickTest(final int lgK) {
    final int k = 1 << lgK;
    final int[] targetC = new int[] { 0, 1, ((3 * k) / 32) - 1, k / 3, k, (7 * k) / 2 };
    final int len = targetC.length;
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        quickTest(lgK, targetC[i], targetC[j]);
      }
    }
  }

  void quickTest(final int lgK, final long cA, final long cB) {
    final CpcSketch skA = new CpcSketch(lgK);
    final CpcSketch skB = new CpcSketch(lgK);
    final CpcSketch skD = new CpcSketch(lgK); // direct sketch

    final long t0, t1, t2, t3, t4, t5;

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

    final CpcUnion ugM = new CpcUnion(lgK);
    ugM.update(skA);
    t3 = System.nanoTime();

    ugM.update(skB);
    t4 = System.nanoTime();

    final CpcSketch skR = ugM.getResult();
    t5 = System.nanoTime();

    rtAssert(TestUtil.specialEquals(skD, skR, false, true));
    final Flavor fA = skA.getFlavor();
    final Flavor fB = skB.getFlavor();
    final Flavor fR = skR.getFlavor();
    final String aOff = Integer.toString(skA.windowOffset);
    final String bOff = Integer.toString(skB.windowOffset);
    final String rOff = Integer.toString(skR.windowOffset);
    final String fAoff = fA + String.format("%2s",aOff);
    final String fBoff = fB + String.format("%2s",bOff);
    final String fRoff = fR + String.format("%2s",rOff);
    final double updA_mS = (t1 - t0) / 2E6;  //update A,D to cA
    final double updB_mS = (t2 - t1) / 2E6;  //update B,D to cB
    final double mrgA_mS = (t3 - t2) / 1E6;  //merge A
    final double mrgB_mS = (t4 - t3) / 1E6;  //merge B
    final double rslt_mS = (t5 - t4) / 1E6;  //get Result

    printf(dfmt, lgK, cA, cB, fAoff, fBoff, fRoff,
        updA_mS, updB_mS, mrgA_mS, mrgB_mS, rslt_mS);
  }


  private void printf(final String format, final Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleFormats() {
    final String[][] assy = {
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
    final int cols = assy.length;
    hStrArr = new String[cols];
    final StringBuilder headerFmt = new StringBuilder();
    final StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nQuick Merging Validation\n");
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
