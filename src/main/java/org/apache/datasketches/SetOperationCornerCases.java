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

package org.apache.datasketches;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class SetOperationCornerCases {
  private static final long MAX = Long.MAX_VALUE;

  public enum InterResult {
    NEW_1_0_T(1, "New{1.0, 0, T}"),
    NEW_MIN_0_F(2, "New{MinTheta, 0, F}"),
    FULL_INTER(6, "Full Intersect");

    private int interRid;
    private String desc;

    private InterResult(final int interRid, final String desc) {
      this.interRid = interRid;
      this.desc = desc;
    }

    public int getInterResultId() {
      return interRid;
    }

    public String getDesc() {
      return desc;
    }
  }

  public enum AnotbResult {
    NEW_1_0_T(1, "New{1.0, 0, T}"),
    NEW_MIN_0_F(2, "New{MinTheta, 0, F}"),
    NEW_THA_0_F(3, "New{ThetaA, 0, F}"),
    SKA_TRIM(4, "Trim Sketch A by MinTheta"),
    SKETCH_A(5, "Sketch A Exactly"),
    FULL_ANOTB(7, "Full AnotB");

    private int aNbRid;
    private String desc;

    private AnotbResult(final int aNbRid, final String desc) {
      this.aNbRid = aNbRid;
      this.desc = desc;
    }

    public int getAnotbResultId() {
      return aNbRid;
    }

    public String getDesc() {
      return desc;
    }
  }

  public enum CornerCase {
    ResultDegen_ResultDegen(0,  "A{>1.0, 0, F} ; B{>1.0, 0, F}", InterResult.NEW_MIN_0_F, AnotbResult.NEW_MIN_0_F), //0
    ResultDegen_NewDegen(01,    "A{>1.0, 0, F} ; B{>1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_THA_0_F),   //1
    ResultDegen_Estimation(02,  "A{>1.0, 0, F} ; B{>1.0,>0, F}", InterResult.NEW_MIN_0_F, AnotbResult.NEW_MIN_0_F), //2
    ResultDegen_New(05,         "A{>1.0, 0, F} ; B{ 1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_THA_0_F),   //5
    ResultDegen_Exact(06,       "A{>1.0, 0, F} ; B{ 1.0,>0, F}", InterResult.NEW_MIN_0_F, AnotbResult.NEW_THA_0_F), //6

    NewDegen_ResultDegen(010,   "A{>1.0, 0, T} ; B{>1.0, 0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),     //8
    NewDegen_NewDegen(011,      "A{>1.0, 0, T} ; B{>1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),     //9
    NewDegen_Estimation(012,    "A{>1.0, 0, T} ; B{>1.0,>0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //10
    NewDegen_New(015,           "A{>1.0, 0, T} ; B{ 1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //13
    NewDegen_Exact(016,         "A{>1.0, 0, T} ; B{ 1.0,>0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //14

    Estimation_ResultDegen(020, "A{>1.0,>0, F} ; B{>1.0, 0, F}", InterResult.NEW_MIN_0_F, AnotbResult.SKA_TRIM),   //16
    Estimation_NewDegen(021,    "A{>1.0,>0, F} ; B{>1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.SKETCH_A),     //17
    Estimation_Estimation(022,  "A{>1.0,>0, F} ; B{>1.0,>0, F}", InterResult.FULL_INTER, AnotbResult.FULL_ANOTB),  //18
    Estimation_New(025,         "A{>1.0,>0, F} ; B{ 1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.SKETCH_A),     //21
    Estimation_Exact(026,       "A{>1.0,>0, F} ; B{ 1.0,>0, F}", InterResult.FULL_INTER, AnotbResult.FULL_ANOTB),  //22

    New_ResultDegen(050,        "A{ 1.0, 0, T} ; B{>1.0, 0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //40
    New_NewDegen(051,           "A{ 1.0, 0, T} ; B{>1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //41
    New_Estimation(052,         "A{ 1.0, 0, T} ; B{>1.0,>0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //42
    New_New(055,                "A{ 1.0, 0, T} ; B{ 1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //45
    New_Exact(056,              "A{ 1.0, 0, T} ; B{ 1.0,>0, F}", InterResult.NEW_1_0_T, AnotbResult.NEW_1_0_T),    //46

    Exact_ResultDegen(060,      "A{ 1.0,>0, F} ; B{>1.0, 0, F}", InterResult.NEW_MIN_0_F, AnotbResult.SKA_TRIM),   //48
    Exact_NewDegen(061,         "A{ 1.0,>0, F} ; B{>1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.SKETCH_A),     //49
    Exact_Estimation(062,       "A{ 1.0,>0, F} ; B{>1.0,>0, F}", InterResult.FULL_INTER, AnotbResult.FULL_ANOTB),  //50
    Exact_New(065,              "A{ 1.0,>0, F} ; B{ 1.0, 0, T}", InterResult.NEW_1_0_T, AnotbResult.SKETCH_A),     //53
    Exact_Exact(066,            "A{ 1.0,>0, F} ; B{ 1.0,>0, F}", InterResult.FULL_INTER, AnotbResult.FULL_ANOTB);  //54

    private static final Map<Integer, CornerCase> idToCornerCaseMap = new HashMap<>();
    private int id;
    private String inputStr;
    private InterResult interResult;
    private AnotbResult anotbResult;

    static {
      for (final CornerCase cc : values()) {
        idToCornerCaseMap.put(cc.getId(), cc);
      }
    }

    private CornerCase(final int id, final String desc,
        final InterResult interResult, final AnotbResult anotbResult) {
      this.id = id;
      this.inputStr = desc;
      this.interResult = interResult;
      this.anotbResult = anotbResult;
    }

    public int getId() {
      return id;
    }

    public String getDesc() {
      return inputStr;
    }

    public InterResult getInterResult() {
      return interResult;
    }

    public AnotbResult getAnotbResult() {
      return anotbResult;
    }

    public static CornerCase idToCornerCase(final int id) {
      final CornerCase cc = idToCornerCaseMap.get(id);
      if (cc == null) {
        throw new SketchesArgumentException("Possible Corruption: Illegal CornerCase ID: " + id);
      }
      return cc;
    }
  } //end of enum

  public static int createCornerCaseId(
      final long thetaLongA, final int countA, final boolean emptyA,
      final long thetaLongB, final int countB, final boolean emptyB) {
    return ((thetaLongA < MAX) ? 0 : 1 << 5)
         | ((countA == 0)      ? 0 : 1 << 4)
         | (!emptyA            ? 0 : 1 << 3)
         | ((thetaLongB < MAX) ? 0 : 1 << 2)
         | ((countB == 0)      ? 0 : 1 << 1)
         | (!emptyB            ? 0 : 1);
  }

  @Test
  public void checkById() {
    final int[] ids = {0,1,2, 5, 6 };
    final int len = ids.length;
    for (int i = 0; i < len; i++) {
      for (int j = 0; j < len; j++) {
        final int id = ids[i] << 3 | ids[j];
        final CornerCase cCase = CornerCase.idToCornerCase(id);
        final String interResStr = cCase.getInterResult().getDesc();
        final String anotbResStr = cCase.getAnotbResult().getDesc();
        println(Integer.toOctalString(id) + "\t" + cCase + "\t" + cCase.getDesc()
         + "\t" + interResStr + "\t" + anotbResStr);
      }
    }
  }

  private static void println(final Object o) { System.out.println(o.toString()); }
}
