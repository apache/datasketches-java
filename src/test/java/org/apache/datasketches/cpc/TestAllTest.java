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
import static org.testng.Assert.assertEquals;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class TestAllTest {
  // Enable these as desired for all tests.
  private PrintStream ps = null; //System.out; //prints to console
  private PrintWriter pw = null; //prints to file (optional)

  //STREAMING

  @Test //scope = Test
  public void streamingCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int trials = 10;
    int ppoN = 1;

    StreamingValidation sVal = new StreamingValidation(
        lgMinK, lgMaxK, trials, ppoN, ps, pw);
    sVal.start();
  }

  //Matrix
  @Test
  public void matrixCouponCountCheck() {
    long pat = 0xA5A5A5A5_5A5A5A5AL;
    int len = 16;
    long[] arr = new long[len];
    Arrays.fill(arr, pat);
    long trueCount = len * Long.bitCount(pat);
    long testCount = BitMatrix.countCoupons(arr);
    assertEquals(testCount, trueCount);
  }

  //COMPRESSION

  @Test //scope = Test
  public void compressionCharacterizationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int lgMaxT = 5; //Trials at start
    int lgMinT = 2; //Trials at end
    int lgMulK = 7;
    int uPPO = 1;
    int incLgK = 1;

    CompressionCharacterization cc = new CompressionCharacterization(
        lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK, ps, pw);
    cc.start();
  }

  //@Test //used for troubleshooting a specific rowCol problems
  public void singleRowColCheck() {
    int lgK = 20;
    CpcSketch srcSketch = new CpcSketch(lgK);
    int rowCol = 54746379;
    srcSketch.rowColUpdate(rowCol);
    ps.println(srcSketch.toString(true));

    CompressedState state = CompressedState.compress(srcSketch);
    ps.println(CompressedState.toString(state, true));
    CpcSketch uncSketch = CpcSketch.uncompress(state, DEFAULT_UPDATE_SEED);
    ps.println(uncSketch.toString(true));
  }

  //MERGING

  @Test //longer test. use for characterization
  public void mergingValidationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10; //inclusive
    int lgMulK = 5;  //5
    int uPPO = 1; //16
    int incLgK = 1;

    MergingValidation mv = new MergingValidation(
        lgMinK, lgMaxK, lgMulK, uPPO, incLgK, ps, pw);
    mv.start();
  }

  @Test //scope = Test
  public void quickMergingValidationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int incLgK = 1;

    QuickMergingValidation qmv = new QuickMergingValidation(
        lgMinK, lgMaxK, incLgK, ps, pw);
    qmv.start();
  }

  @Test
  public void checkPwrLaw10NextDouble() {
    double next = TestUtil.pwrLaw10NextDouble(1, 10.0);
    assertEquals(next, 100.0);
  }

}
