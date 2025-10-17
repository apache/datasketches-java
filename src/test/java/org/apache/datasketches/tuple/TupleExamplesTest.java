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

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummary.Mode;
import org.apache.datasketches.tuple.aninteger.IntegerSummaryFactory;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.testng.annotations.Test;

/**
 * Tests for Version 2.0.0
 * @author Lee Rhodes
 */
public class TupleExamplesTest {
  private final IntegerSummary.Mode umode = Mode.Sum;
  private final IntegerSummary.Mode imode = Mode.AlwaysOne;
  private final IntegerSummarySetOperations isso = new IntegerSummarySetOperations(umode, imode);
  private final IntegerSummaryFactory ufactory = new IntegerSummaryFactory(umode);
  private final IntegerSummaryFactory ifactory = new IntegerSummaryFactory(imode);
  private final UpdatableThetaSketchBuilder thetaBldr = UpdatableThetaSketch.builder();
  private final UpdatableTupleSketchBuilder<Integer, IntegerSummary> tupleBldr =
      new UpdatableTupleSketchBuilder<>(ufactory);


  @Test
  public void example1() {
    //Load source sketches
    final UpdatableTupleSketch<Integer, IntegerSummary> tupleSk = tupleBldr.build();
    final UpdatableThetaSketch thetaSk = thetaBldr.build();
    for (int i = 1; i <= 12; i++) {
      tupleSk.update(i, 1);
      thetaSk.update(i + 3);
    }

    //TupleUnion stateful: tuple, theta
    final TupleUnion<IntegerSummary> union = new TupleUnion<>(isso);
    union.union(tupleSk);
    union.union(thetaSk, ufactory.newSummary().update(1));
    final CompactTupleSketch<IntegerSummary> ucsk = union.getResult();
    int entries = ucsk.getRetainedEntries();
    println("TupleUnion Stateful: tuple, theta: " + entries);
    final TupleSketchIterator<IntegerSummary> uiter = ucsk.iterator();
    int counter = 1;
    int twos = 0;
    int ones = 0;
    while (uiter.next()) {
      final int i = uiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
      if (i == 1) { ones++; }
      if (i == 2) { twos++; }
    }
    assertEquals(ones, 6);
    assertEquals(twos, 9);

    //TupleIntersection stateful: tuple, theta
    final TupleIntersection<IntegerSummary> inter = new TupleIntersection<>(isso);
    inter.intersect(tupleSk);
    inter.intersect(thetaSk, ifactory.newSummary().update(1));
    final CompactTupleSketch<IntegerSummary> icsk = inter.getResult();
    entries = icsk.getRetainedEntries();
    println("TupleIntersection Stateful: tuple, theta: " + entries);
    final TupleSketchIterator<IntegerSummary> iiter = icsk.iterator();
    counter = 1;
    while (iiter.next()) {
      final int i = iiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 1
      assertEquals(i, 1);
    }
  }

  @Test
  public void example2() {
    //Load source sketches
    final UpdatableTupleSketch<Integer, IntegerSummary> tupleSk1 = tupleBldr.build();
    final UpdatableTupleSketch<Integer, IntegerSummary> tupleSk2 = tupleBldr.build();

    for (int i = 1; i <= 12; i++) {
      tupleSk1.update(i, 1);
      tupleSk2.update(i + 3, 1);
    }

    //TupleUnion, stateless: tuple1, tuple2
    final TupleUnion<IntegerSummary> union = new TupleUnion<>(isso);
    final CompactTupleSketch<IntegerSummary> ucsk = union.union(tupleSk1, tupleSk2);
    int entries = ucsk.getRetainedEntries();
    println("TupleUnion: " + entries);
    final TupleSketchIterator<IntegerSummary> uiter = ucsk.iterator();
    int counter = 1;
    int twos = 0;
    int ones = 0;
    while (uiter.next()) {
      final int i = uiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
      if (i == 1) { ones++; }
      if (i == 2) { twos++; }
    }
    assertEquals(ones, 6);
    assertEquals(twos, 9);

    //TupleIntersection stateless: tuple1, tuple2
    final TupleIntersection<IntegerSummary> inter = new TupleIntersection<>(isso);
    final CompactTupleSketch<IntegerSummary> icsk = inter.intersect(tupleSk1, tupleSk2);
    entries = icsk.getRetainedEntries();
    println("TupleIntersection: " + entries);
    final TupleSketchIterator<IntegerSummary> iiter = icsk.iterator();
    counter = 1;
    while (iiter.next()) {
      final int i = iiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2
      assertEquals(i, 1);
    }
  }

  @Test
  public void example3() {
    //Load source sketches
    final UpdatableTupleSketch<Integer, IntegerSummary> tupleSk = tupleBldr.build();
    final UpdatableThetaSketch thetaSk = thetaBldr.build();
    for (int i = 1; i <= 12; i++) {
      tupleSk.update(i, 1);
      thetaSk.update(i + 3);
    }

    //TupleUnion, stateless: tuple1, tuple2
    final TupleUnion<IntegerSummary> union = new TupleUnion<>(isso);
    final CompactTupleSketch<IntegerSummary> ucsk =
        union.union(tupleSk, thetaSk, ufactory.newSummary().update(1));
    int entries = ucsk.getRetainedEntries();
    println("TupleUnion: " + entries);
    final TupleSketchIterator<IntegerSummary> uiter = ucsk.iterator();
    int counter = 1;
    int twos = 0;
    int ones = 0;
    while (uiter.next()) {
      final int i = uiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
      if (i == 1) { ones++; }
      if (i == 2) { twos++; }
    }
    assertEquals(ones, 6);
    assertEquals(twos, 9);

    //TupleIntersection stateless: tuple1, tuple2
    final TupleIntersection<IntegerSummary> inter = new TupleIntersection<>(isso);
    final CompactTupleSketch<IntegerSummary> icsk =
        inter.intersect(tupleSk, thetaSk, ufactory.newSummary().update(1));
    entries = icsk.getRetainedEntries();
    println("TupleIntersection: " + entries);
    final TupleSketchIterator<IntegerSummary> iiter = icsk.iterator();
    counter = 1;
    while (iiter.next()) {
      final int i = iiter.getSummary().getValue();
      println(counter++ + ", " + i); //9 entries = 2
      assertEquals(i, 1);
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
    //System.out.println(s); //enable/disable here
  }
}
