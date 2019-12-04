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
import static org.apache.datasketches.cpc.IconEstimator.getIconEstimate;
import static org.apache.datasketches.cpc.RuntimeAsserts.rtAssert;
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

  public MergingValidation(final int lgMinK, final int lgMaxK, final int lgMulK, final int uPPO,
      final int incLgK, final PrintStream pS, final PrintWriter pW) {
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
        nB = Math.round(pwrLawNextDouble(uPPO, nB, true, 2.0));
      }
      nA = Math.round(pwrLawNextDouble(uPPO, nA, true, 2.0));
    }
  }

  private void testMerging(final int lgKm, final int lgKa, final int lgKb, final long nA,
      final long nB) {
    final CpcUnion ugM = new CpcUnion(lgKm);

    // int lgKd = ((nA != 0) && (lgKa < lgKm)) ? lgKa : lgKm;
    // lgKd =     ((nB != 0) && (lgKb < lgKd)) ? lgKb : lgKd;
    int lgKd = lgKm;
    if ((lgKa < lgKd) && (nA != 0)) { lgKd = lgKa; } //d = min(a,m) : m
    if ((lgKb < lgKd) && (nB != 0)) { lgKd = lgKb; } //d = min(b,d) : d

    final CpcSketch skD = new CpcSketch(lgKd); // direct sketch, updated with both streams

    final CpcSketch skA = new CpcSketch(lgKa);
    final CpcSketch skB = new CpcSketch(lgKb);

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

    ugM.update(skA);
    ugM.update(skB);

    final int finalLgKm = ugM.getLgK();
    final long[] matrixM = CpcUnion.getBitMatrix(ugM);

    final long cM = ugM.getNumCoupons();//countBitsSetInMatrix(matrixM);
    final long cD = skD.numCoupons;
    final Flavor flavorD = skD.getFlavor();
    final Flavor flavorA = skA.getFlavor();
    final Flavor flavorB = skB.getFlavor();
    final String dOff = Integer.toString(skD.windowOffset);
    final String aOff = Integer.toString(skA.windowOffset);
    final String bOff = Integer.toString(skB.windowOffset);
    final String flavorDoff = flavorD + String.format("%2s",dOff);
    final String flavorAoff = flavorA + String.format("%2s",aOff);
    final String flavorBoff = flavorB + String.format("%2s",bOff);
    final double iconEstD = getIconEstimate(lgKd, cD);

    rtAssert(finalLgKm <= lgKm);
    rtAssert(cM <= (skA.numCoupons + skB.numCoupons));
    rtAssertEquals(cM, cD);

    rtAssertEquals(finalLgKm, lgKd);
    final long[] matrixD = CpcUtil.bitMatrixOfSketch(skD);
    rtAssertEquals(matrixM, matrixD);

    final CpcSketch skR = ugM.getResult();
    final double iconEstR = getIconEstimate(skR.lgK, skR.numCoupons);
    rtAssertEquals(iconEstD, iconEstR, 0.0);
    rtAssert(TestUtil.specialEquals(skD, skR, false, true));

    printf(dfmt, lgKm, lgKa, lgKb, lgKd, nA, nB, (nA + nB),
        flavorAoff, flavorBoff, flavorDoff,
        skA.numCoupons, skB.numCoupons, cD, iconEstR);
  }

  private void printf(final String format, final Object ... args) {
    if (printStream != null) { printStream.printf(format, args); }
    if (printWriter != null) { printWriter.printf(format, args); }
  }

  private void assembleFormats() {
    final String[][] assy = {
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
    final int cols = assy.length;
    hStrArr = new String[cols];
    final StringBuilder headerFmt = new StringBuilder();
    final StringBuilder dataFmt = new StringBuilder();
    headerFmt.append("\nMerging Validation\n");
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
