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

  public enum IntersectAction {
    DEGEN_MIN_0_F("D", "Degenerate{MinTheta, 0, F}"),
    EMPTY_1_0_T("E", "Empty{1.0, 0, T}"),
    FULL_INTERSECT("I", "Full Intersect");

    private String actionId;
    private String actionDescription;

    private IntersectAction(final String actionId, final String actionDescription) {
      this.actionId = actionId;
      this.actionDescription = actionDescription;
    }

    public String getActionId() {
      return actionId;
    }

    public String getActionDescription() {
      return actionDescription;
    }
  }

  public enum AnotbAction {
    SKETCH_A("A", "Sketch A Exactly"),
    TRIM_A("TA", "Trim Sketch A by MinTheta"),
    DEGEN_MIN_0_F("D", "Degenerate{MinTheta, 0, F}"),
    DEGEN_THA_0_F("DA", "Degenerate{ThetaA, 0, F}"),
    EMPTY_1_0_T("E", "Empty{1.0, 0, T}"),
    FULL_ANOTB("N", "Full AnotB");

    private String actionId;
    private String actionDescription;

    private AnotbAction(final String actionId, final String actionDescription) {
      this.actionId = actionId;
      this.actionDescription = actionDescription;
    }

    public String getActionId() {
      return actionId;
    }

    public String getActionDescription() {
      return actionDescription;
    }
  }

  public enum UnionAction {
    SKETCH_A("A", "Sketch A Exactly"),
    TRIM_A("TA", "Trim Sketch A by MinTheta"),
    SKETCH_B("B", "Sketch B Exactly"),
    TRIM_B("TB", "Trim Sketch B by MinTheta"),
    DEGEN_MIN_0_F("D", "Degenerate{MinTheta, 0, F}"),
    DEGEN_THA_0_F("DA", "Degenerate{ThetaA, 0, F}"),
    DEGEN_THB_0_F("DB", "Degenerate{ThetaB, 0, F}"),
    EMPTY_1_0_T("E", "Empty{1.0, 0, T}"),
    FULL_UNION("N", "Full Union");

    private String actionId;
    private String actionDescription;

    private UnionAction(final String actionId, final String actionDescription) {
      this.actionId = actionId;
      this.actionDescription = actionDescription;
    }

    public String getActionId() {
      return actionId;
    }

    public String getActionDescription() {
      return actionDescription;
    }
  }

  public enum CornerCase {
    Empty_Empty(055, "A{ 1.0, 0, T} ; B{ 1.0, 0, T}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.EMPTY_1_0_T, UnionAction.EMPTY_1_0_T),
    Empty_Exact(056, "A{ 1.0, 0, T} ; B{ 1.0,>0, F}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.EMPTY_1_0_T, UnionAction.SKETCH_B),
    Empty_Estimation(052, "A{ 1.0, 0, T} ; B{<1.0,>0, F",
        IntersectAction.EMPTY_1_0_T, AnotbAction.EMPTY_1_0_T, UnionAction.SKETCH_B),
    Empty_Degen(050, "A{ 1.0, 0, T} ; B{<1.0, 0, F}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.EMPTY_1_0_T, UnionAction.DEGEN_THB_0_F),

    Exact_Empty(065, "A{ 1.0,>0, F} ; B{ 1.0, 0, T}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.SKETCH_A, UnionAction.SKETCH_A),
    Exact_Exact(066, "A{ 1.0,>0, F} ; B{ 1.0,>0, F}",
        IntersectAction.FULL_INTERSECT, AnotbAction.FULL_ANOTB, UnionAction.FULL_UNION),
    Exact_Estimation(062, "A{ 1.0,>0, F} ; B{<1.0,>0, F}",
        IntersectAction.FULL_INTERSECT, AnotbAction.FULL_ANOTB, UnionAction.FULL_UNION),
    Exact_Degen(060, "A{ 1.0,>0, F} ; B{<1.0, 0, F}",
        IntersectAction.DEGEN_MIN_0_F, AnotbAction.TRIM_A, UnionAction.TRIM_A),

    Estimation_Empty(025, "A{<1.0,>0, F} ; B{ 1.0, 0, T}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.SKETCH_A, UnionAction.SKETCH_A),
    Estimation_Exact(026, "A{<1.0,>0, F} ; B{ 1.0,>0, F}",
        IntersectAction.FULL_INTERSECT, AnotbAction.FULL_ANOTB, UnionAction.FULL_UNION),
    Estimation_Estimation(022, "A{<1.0,>0, F} ; B{<1.0,>0, F}",
        IntersectAction.FULL_INTERSECT, AnotbAction.FULL_ANOTB, UnionAction.FULL_UNION),
    Estimation_Degen(020, "A{<1.0,>0, F} ; B{<1.0, 0, F}",
        IntersectAction.DEGEN_MIN_0_F, AnotbAction.TRIM_A, UnionAction.TRIM_A),

    Degen_Empty(005, "A{<1.0, 0, F} ; B{ 1.0, 0, T}",
        IntersectAction.EMPTY_1_0_T, AnotbAction.DEGEN_THA_0_F, UnionAction.DEGEN_THA_0_F),
    Degen_Exact(006, "A{<1.0, 0, F} ; B{ 1.0,>0, F}",
        IntersectAction.DEGEN_MIN_0_F, AnotbAction.DEGEN_THA_0_F, UnionAction.TRIM_B),
    Degen_Estimation(002, "A{<1.0, 0, F} ; B{<1.0,>0, F}",
        IntersectAction.DEGEN_MIN_0_F, AnotbAction.DEGEN_MIN_0_F, UnionAction.TRIM_B),
    Degen_Degen(000, "A{<1.0, 0, F} ; B{<1.0, 0, F}",
        IntersectAction.DEGEN_MIN_0_F, AnotbAction.DEGEN_MIN_0_F, UnionAction.DEGEN_MIN_0_F);

    private static final Map<Integer, CornerCase> caseIdToCornerCaseMap = new HashMap<>();
    private int caseId;
    private String caseDescription;
    private IntersectAction intersectAction;
    private AnotbAction anotbAction;
    private UnionAction unionAction;

    static {
      for (final CornerCase cc : values()) {
        caseIdToCornerCaseMap.put(cc.getId(), cc);
      }
    }

    private CornerCase(final int caseId, final String caseDescription,
        final IntersectAction intersectAction, final AnotbAction anotbAction, final UnionAction unionAction) {
      this.caseId = caseId;
      this.caseDescription = caseDescription;
      this.intersectAction = intersectAction;
      this.anotbAction = anotbAction;
      this.unionAction = unionAction;
    }

    public int getId() {
      return caseId;
    }

    public String getCaseDescription() {
      return caseDescription;
    }

    public IntersectAction getIntersectAction() {
      return intersectAction;
    }

    public AnotbAction getAnotbAction() {
      return anotbAction;
    }

    public UnionAction getUnionAction() {
      return unionAction;
    }

    //See checkById test in /tuple/MiscTest.
    public static CornerCase caseIdToCornerCase(final int id) {
      final CornerCase cc = caseIdToCornerCaseMap.get(id);
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

  public static int sketchStateId(final boolean isEmpty, final int numRetained, final long thetaLong) {
    // assume thetaLong = MAX if empty
    return (((thetaLong == MAX) || isEmpty) ? 4 : 0) | ((numRetained > 0) ? 2 : 0) | (isEmpty ? 1 : 0);
  }
}
