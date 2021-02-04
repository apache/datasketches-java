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

package org.apache.datasketches.tuple;

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryFactory;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.testng.annotations.Test;


@SuppressWarnings("javadoc")
public class TupleExamplesTest {
  private final IntegerSummary.Mode umode = Mode.Sum;
  private final IntegerSummary.Mode imode = Mode.AlwaysOne;
  private final IntegerSummarySetOperations isso = new IntegerSummarySetOperations(umode, imode);
  private final IntegerSummaryFactory ufactory = new IntegerSummaryFactory(umode);
  private final IntegerSummaryFactory ifactory = new IntegerSummaryFactory(imode);
  private final UpdateSketchBuilder thetaBldr = UpdateSketch.builder();
  private final UpdatableSketchBuilder<Integer, IntegerSummary> tupleBldr =
      new UpdatableSketchBuilder<>(ufactory);


  @Test
  public void tuple2dot0Examples() {
    //Load source sketches
    final UpdatableSketch<Integer, IntegerSummary> tupleSk = tupleBldr.build();
    final UpdateSketch thetaSk = thetaBldr.build();
    for (int i = 1; i <= 12; i++) {
      tupleSk.update(i, 1);
      thetaSk.update(i + 3);
    }

    //Union
    final Union<IntegerSummary> union = new Union<>(isso);
    union.update(tupleSk);
    union.update(thetaSk, ufactory.newSummary().update(1));
    final CompactSketch<IntegerSummary> ucsk = union.getResult();
    int entries = ucsk.getRetainedEntries();
    println("Union: " + entries);
    final SketchIterator<IntegerSummary> uiter = ucsk.iterator();
    int counter = 1;
    while (uiter.next()) {
      final int i = uiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
    }

    //Intersection
    final Intersection<IntegerSummary> inter = new Intersection<>(isso);
    inter.update(tupleSk);
    inter.update(thetaSk, ifactory.newSummary().update(1));
    final CompactSketch<IntegerSummary> icsk = inter.getResult();
    entries = icsk.getRetainedEntries();
    println("Intersection: " + entries);
    final SketchIterator<IntegerSummary> iiter = icsk.iterator();
    counter = 1;
    while (iiter.next()) {
      final int i = iiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 1
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    System.out.println(s); //enable/disable here
  }
}
