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

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class TestAllTest {

  //STREAMING

  @Test
  public void streamingCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int trials = 10;
    int ppoN = 1;
    PrintStream ps = null;//System.out;
    PrintWriter pw = null;

    StreamingValidation sVal = new StreamingValidation(
        lgMinK, lgMaxK, trials, ppoN, ps, pw);
    sVal.start();
  }

  //COMPRESSION

  @Test
  public void compressionCharacterizationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int lgMaxT = 5; //Trials at start
    int lgMinT = 2; //Trials at end
    int lgMulK = 7;
    int uPPO = 1;
    int incLgK = 1;
    PrintStream ps = null; //System.out;
    PrintWriter pw = null;

    CompressionCharacterization cc = new CompressionCharacterization(
        lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK, ps, pw);
    cc.start();
  }

  //@Test //used for troubleshooting a specific rowCol problems
  public void singleRowColCheck() {
    PrintStream ps = System.out;
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

  //@Test //long test. use for characterization
  public void mergingValidationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10; //inclusive
    int lgMulK = 5;  //5
    int uPPO = 1; //16
    int incLgK = 1;
    PrintStream ps = null;//System.out;
    PrintWriter pw = null;

    MergingValidation mv = new MergingValidation(
        lgMinK, lgMaxK, lgMulK, uPPO, incLgK, ps, pw);
    mv.start();
  }

  @Test
  public void quickMergingValidationCheck() {
    int lgMinK = 10;
    int lgMaxK = 10;
    int incLgK = 1;
    PrintStream ps = null;//System.out;
    PrintWriter pw = null;

    QuickMergingValidation qmv = new QuickMergingValidation(
        lgMinK, lgMaxK, incLgK, ps, pw);
    qmv.start();
  }

}
