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

import static org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode.Min;
import static org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode.Sum;

import org.apache.datasketches.tuple.AnotB;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
//import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Union;
import org.testng.annotations.Test;

/**
 * These tests check to make sure that no summary objects, which are mutable, and created
 * as needed internally within a tuple sketch never leak into the result sketch.
 *
 * @author Lee Rhodes
 *
 */
public class ParameterLeakageTest {
  IntegerSummarySetOperations setOps = new IntegerSummarySetOperations(Sum, Min);

  @Test
  public void checkUnion() {
    IntegerSketch sk1 = new IntegerSketch(4, Sum);
    sk1.update(1, 1);
    IntegerSummary sk1sum = captureSummaries(sk1)[0];

    IntegerSketch sk2 = new IntegerSketch(4, Sum);
    sk2.update(2, 1);
    IntegerSummary sk2sum = captureSummaries(sk2)[0];


    Union<IntegerSummary> union = new Union<>(setOps);

    CompactSketch<IntegerSummary> csk = union.union(sk1, sk2);
    IntegerSummary[] summaries = captureSummaries(csk);
    println("Union Count: " + summaries.length);

    for (IntegerSummary isum : summaries) {
      if ((isum == sk1sum) || (isum == sk2sum)) {
        throw new IllegalArgumentException("Parameter Leakage");
      }
    }
  }

  @Test
  public void checkIntersectStateless() {
    IntegerSketch sk1 = new IntegerSketch(4, Sum);
    sk1.update(1, 1);
    IntegerSummary sk1sum = captureSummaries(sk1)[0];

    IntegerSketch sk2 = new IntegerSketch(4, Sum);
    sk2.update(1, 1);
    IntegerSummary sk2sum = captureSummaries(sk2)[0];

    Intersection<IntegerSummary> intersect = new Intersection<>(setOps);

    CompactSketch<IntegerSummary> csk = intersect.intersect(sk1, sk2);
    IntegerSummary[] summaries = captureSummaries(csk);
    println("Intersect Stateless Count: " + summaries.length);

    for (IntegerSummary isum : summaries) {
      if ((isum == sk1sum) || (isum == sk2sum)) {
        throw new IllegalArgumentException("Parameter Leakage");
      }
    }
  }

  @Test
  public void checkIntersectStateful() {
    IntegerSketch sk1 = new IntegerSketch(4, Sum);
    sk1.update(1, 1);
    IntegerSummary sk1sum = captureSummaries(sk1)[0];

    IntegerSketch sk2 = new IntegerSketch(4, Sum);
    sk2.update(1, 1);
    IntegerSummary sk2sum = captureSummaries(sk2)[0];

    Intersection<IntegerSummary> intersect = new Intersection<>(setOps);

    intersect.intersect(sk1);
    intersect.intersect(sk2);
    CompactSketch<IntegerSummary> csk = intersect.getResult();

    IntegerSummary[] summaries = captureSummaries(csk);
    println("Intersect Stateful Count: " + summaries.length);

    for (IntegerSummary isum : summaries) {
      if ((isum == sk1sum) || (isum == sk2sum)) {
        throw new IllegalArgumentException("Parameter Leakage");
      }
    }
  }

  @Test
  public void checkAnotbStateless() {
    IntegerSketch sk1 = new IntegerSketch(4, Sum);
    sk1.update(1, 1);
    CompactSketch<IntegerSummary> csk1 = sk1.compact();
    IntegerSummary sk1sum = captureSummaries(csk1)[0];

    IntegerSketch sk2 = new IntegerSketch(4, Sum); //EMPTY

    CompactSketch<IntegerSummary> csk = AnotB.aNotB(csk1, sk2);
    IntegerSummary[] summaries = captureSummaries(csk);
    println("AnotB Stateless Count: " + summaries.length);

    for (IntegerSummary isum : summaries) {
      if (isum == sk1sum) {
        throw new IllegalArgumentException("Parameter Leakage");
      }
    }
  }

  @Test
  public void checkAnotbStateful() {
    IntegerSketch sk1 = new IntegerSketch(4, Sum);
    sk1.update(1, 1);
    CompactSketch<IntegerSummary> csk1 = sk1.compact();
    IntegerSummary sk1sum = captureSummaries(csk1)[0];

    IntegerSketch sk2 = new IntegerSketch(4, Sum); //EMPTY

    AnotB<IntegerSummary> anotb = new AnotB<>();

    anotb.setA(csk1);
    anotb.notB(sk2);

    CompactSketch<IntegerSummary> csk = anotb.getResult(true);
    IntegerSummary[] summaries = captureSummaries(csk);
    println("AnotB Stateful Count: " + summaries.length);

    for (IntegerSummary isum : summaries) {
      if (isum == sk1sum) {
        throw new IllegalArgumentException("Parameter Leakage");
      }
    }
  }

  private static IntegerSummary[] captureSummaries(Sketch<IntegerSummary> sk) {
    int entries = sk.getRetainedEntries();
    IntegerSummary[] intSumArr = new IntegerSummary[entries];
    int cnt = 0;
    SketchIterator<IntegerSummary> it = sk.iterator();
    while (it.next()) {
      intSumArr[cnt] = it.getSummary();
      cnt++;
    }
    return intSumArr;
  }

  /**
   * @param o Object to print
   */
  static void println(Object o) {
    //System.out.println(o.toString()); //disable
  }
}
