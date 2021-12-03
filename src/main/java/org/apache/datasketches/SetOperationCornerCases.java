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

public class SetOperationCornerCases {
  private static final long MAX = Long.MAX_VALUE;

  public enum IntersectResult {
    EMPTY_1_0_T(1, "Empty{1.0, 0, T}"),
    DEGEN_MIN_0_F(2, "Degenerate{MinTheta, 0, F}"),
    FULL_INTER(6, "Full Intersect");

    private int interRid;
    private String desc;

    private IntersectResult(final int interRid, final String desc) {
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
    EMPTY_1_0_T(1, "Empty{1.0, 0, T}"),
    DEGEN_MIN_0_F(2, "Degenerate{MinTheta, 0, F}"),
    DEGEN_THA_0_F(3, "Degenerate{ThetaA, 0, F}"),
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

  public enum UnionResult {

  }

  public enum CornerCase {
    Empty_Empty(055, "A{ 1.0, 0, T} ; B{ 1.0, 0, T}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.EMPTY_1_0_T),
    Empty_Exact(056, "A{ 1.0, 0, T} ; B{ 1.0,>0, F}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.EMPTY_1_0_T),
    Empty_Estimation(052, "A{ 1.0, 0, T} ; B{<1.0,>0, F}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.EMPTY_1_0_T),
    Empty_Degen(050, "A{ 1.0, 0, T} ; B{<1.0, 0, F}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.EMPTY_1_0_T),

    Exact_Empty(065, "A{ 1.0,>0, F} ; B{ 1.0, 0, T}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.SKETCH_A),
    Exact_Exact(066, "A{ 1.0,>0, F} ; B{ 1.0,>0, F}",
        IntersectResult.FULL_INTER, AnotbResult.FULL_ANOTB),
    Exact_Estimation(062, "A{ 1.0,>0, F} ; B{<1.0,>0, F}",
        IntersectResult.FULL_INTER, AnotbResult.FULL_ANOTB),
    Exact_Degen(060, "A{ 1.0,>0, F} ; B{<1.0, 0, F}",
        IntersectResult.DEGEN_MIN_0_F, AnotbResult.SKA_TRIM),

    Estimation_Empty(025, "A{<1.0,>0, F} ; B{ 1.0, 0, T}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.SKETCH_A),
    Estimation_Exact(026, "A{<1.0,>0, F} ; B{ 1.0,>0, F}",
        IntersectResult.FULL_INTER, AnotbResult.FULL_ANOTB),
    Estimation_Estimation(022, "A{<1.0,>0, F} ; B{<1.0,>0, F}",
        IntersectResult.FULL_INTER, AnotbResult.FULL_ANOTB),
    Estimation_Degen(020, "A{<1.0,>0, F} ; B{<1.0, 0, F}",
        IntersectResult.DEGEN_MIN_0_F, AnotbResult.SKA_TRIM),

    Degen_Empty(005, "A{<1.0, 0, F} ; B{ 1.0, 0, T}",
        IntersectResult.EMPTY_1_0_T, AnotbResult.DEGEN_THA_0_F),
    Degen_Exact(006, "A{<1.0, 0, F} ; B{ 1.0,>0, F}",
        IntersectResult.DEGEN_MIN_0_F, AnotbResult.DEGEN_THA_0_F),
    Degen_Estimation(002, "A{<1.0, 0, F} ; B{<1.0,>0, F}",
        IntersectResult.DEGEN_MIN_0_F, AnotbResult.DEGEN_MIN_0_F),
    Degen_Degen(000, "A{<1.0, 0, F} ; B{<1.0, 0, F}",
        IntersectResult.DEGEN_MIN_0_F, AnotbResult.DEGEN_MIN_0_F);

    private static final Map<Integer, CornerCase> idToCornerCaseMap = new HashMap<>();
    private int id;
    private String inputStr;
    private IntersectResult interResult;
    private AnotbResult anotbResult;
    private UnionResult unionResult;

    static {
      for (final CornerCase cc : values()) {
        idToCornerCaseMap.put(cc.getId(), cc);
      }
    }

    private CornerCase(final int id, final String desc,
        final IntersectResult interResult, final AnotbResult anotbResult) {
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

    public IntersectResult getInterResult() {
      return interResult;
    }

    public AnotbResult getAnotbResult() {
      return anotbResult;
    }

    //See checkById test in /tuple/MiscTest.
    public static CornerCase idToCornerCase(final int id) {
      final CornerCase cc = idToCornerCaseMap.get(id);
      if (cc == null) {
        throw new SketchesArgumentException("Possible Corruption: Illegal CornerCase ID: " + Integer.toOctalString(id));
      }
      return cc;
    }
  } //end of enum CornerCase

  public static int createCornerCaseId(
      final long thetaLongA, final int countA, final boolean emptyA,
      final long thetaLongB, final int countB, final boolean emptyB) {
    return (sketchStateId(emptyA, countA, thetaLongA) << 3) | sketchStateId(emptyB, countB, thetaLongB);
  }

  public static int sketchStateId(final boolean isEmpty, final int numRetained, final long theta) {
    // assume theta = MAX if empty
    return (((theta == MAX) || isEmpty) ? 4 : 0) | ((numRetained > 0) ? 2 : 0) | (isEmpty ? 1 : 0);
  }
}
