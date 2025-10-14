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

package org.apache.datasketches.tuple.aninteger;

import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleIntersection;
import org.apache.datasketches.tuple.TupleAnotB;
import org.testng.annotations.Test;

/**
 * Issue #368, from Mikhail Lavrinovich 12 OCT 2021
 * The failure was TupleAnotB(estimating {&lt;1.0,1,F}, TupleIntersect(estimating{&lt;1.0,1,F}, newDegenerative{&lt;1.0,0,T},
 * Which should be equal to TupleAnotB(estimating{&lt;1.0,1,F}, new{1.0,0,T} = estimating{&lt;1.0, 1, F}.
 * The TupleAnotB threw a null pointer exception because it was not properly handling sketches with zero entries.
 */
public class MikhailsBugTupleTest {

  @Test
  public void mikhailsBug() {
    IntegerTupleSketch x = new IntegerTupleSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    IntegerTupleSketch y = new IntegerTupleSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    x.update(1L, 1);
    IntegerSummarySetOperations setOperations =
        new IntegerSummarySetOperations(IntegerSummary.Mode.Min, IntegerSummary.Mode.Min);
    TupleIntersection<IntegerSummary> intersection = new TupleIntersection<>(setOperations);
    CompactTupleSketch<IntegerSummary> intersect = intersection.intersect(x, y);
    TupleAnotB.aNotB(x, intersect); // NPE was here
  }

  //@Test
  public void withTuple() {
    IntegerTupleSketch x = new IntegerTupleSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    IntegerTupleSketch y = new IntegerTupleSketch(12, 2, 0.1f, IntegerSummary.Mode.Min);
    x.update(1L, 1);
    println("Tuple x: Estimating {<1.0,1,F}");
    println(x.toString());
    println("Tuple y: NewDegenerative {<1.0,0,T}");
    println(y.toString());
    IntegerSummarySetOperations setOperations =
        new IntegerSummarySetOperations(IntegerSummary.Mode.Min, IntegerSummary.Mode.Min);
    TupleIntersection<IntegerSummary> intersection = new TupleIntersection<>(setOperations);
    CompactTupleSketch<IntegerSummary> intersect = intersection.intersect(x, y);
    println("TupleIntersect(Estimating, NewDegen) = new {1.0, 0, T}");
    println(intersect.toString());
    CompactTupleSketch<IntegerSummary> csk = TupleAnotB.aNotB(x, intersect);
    println("TupleAnotB(Estimating, New) = estimating {<1.0, 1, F}");
    println(csk.toString());
  }

  /**
   * Println an object
   * @param o object to print
   */
  private static void println(Object o) {
    //System.out.println(o.toString()); //disable here
  }
}