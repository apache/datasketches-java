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

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.testng.annotations.Test;

  /**
   * Tests for Version 2.0.0
   * @author Lee Rhodes
   */
  public class TupleExamples2Test {
    private final DoubleSummary.Mode umode = Mode.Sum;
    private final DoubleSummary.Mode imode = Mode.AlwaysOne;
    private final DoubleSummarySetOperations dsso0 = new DoubleSummarySetOperations();
    private final DoubleSummarySetOperations dsso1 = new DoubleSummarySetOperations(umode);
    private final DoubleSummarySetOperations dsso2 = new DoubleSummarySetOperations(umode, imode);
    private final DoubleSummaryFactory ufactory = new DoubleSummaryFactory(umode);
    private final DoubleSummaryFactory ifactory = new DoubleSummaryFactory(imode);
    private final UpdateSketchBuilder thetaBldr = UpdateSketch.builder();
    private final UpdatableSketchBuilder<Double, DoubleSummary> tupleBldr =
        new UpdatableSketchBuilder<>(ufactory);


    @Test
    public void example1() { // stateful: tuple, theta, use dsso2
      //Load source sketches
      final UpdatableSketch<Double, DoubleSummary> tupleSk = tupleBldr.build();
      final UpdateSketch thetaSk = thetaBldr.build();
      for (int i = 1; i <= 12; i++) {
        tupleSk.update(i, 1.0);
        thetaSk.update(i + 3);
      }

      //Union
      final Union<DoubleSummary> union = new Union<>(dsso2);
      union.union(tupleSk);
      union.union(thetaSk, ufactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> ucsk = union.getResult();
      int entries = ucsk.getRetainedEntries();
      println("Union Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> uiter = ucsk.iterator();
      int counter = 1;
      int twos = 0;
      int ones = 0;
      while (uiter.next()) {
        final int i = (int)uiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
        if (i == 1) { ones++; }
        if (i == 2) { twos++; }
      }
      assertEquals(ones, 6);
      assertEquals(twos, 9);

      //Intersection
      final Intersection<DoubleSummary> inter = new Intersection<>(dsso2);
      inter.intersect(tupleSk);
      inter.intersect(thetaSk, ifactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> icsk = inter.getResult();
      entries = icsk.getRetainedEntries();
      println("Intersection Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> iiter = icsk.iterator();
      counter = 1;
      while (iiter.next()) {
        final int i = (int)iiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 1
        assertEquals(i, 1);
      }
    }

    @Test
    public void example2() { //stateless: tuple1, tuple2, use dsso2
      //Load source sketches
      final UpdatableSketch<Double, DoubleSummary> tupleSk1 = tupleBldr.build();
      final UpdatableSketch<Double, DoubleSummary> tupleSk2 = tupleBldr.build();

      for (int i = 1; i <= 12; i++) {
        tupleSk1.update(i, 1.0);
        tupleSk2.update(i + 3, 1.0);
      }

      //Union
      final Union<DoubleSummary> union = new Union<>(dsso2);
      final CompactSketch<DoubleSummary> ucsk = union.union(tupleSk1, tupleSk2);
      int entries = ucsk.getRetainedEntries();
      println("Union: " + entries);
      final SketchIterator<DoubleSummary> uiter = ucsk.iterator();
      int counter = 1;
      int twos = 0;
      int ones = 0;
      while (uiter.next()) {
        final int i = (int)uiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
        if (i == 1) { ones++; }
        if (i == 2) { twos++; }
      }
      assertEquals(ones, 6);
      assertEquals(twos, 9);

      //Intersection
      final Intersection<DoubleSummary> inter = new Intersection<>(dsso2);
      final CompactSketch<DoubleSummary> icsk = inter.intersect(tupleSk1, tupleSk2);
      entries = icsk.getRetainedEntries();
      println("Intersection: " + entries);
      final SketchIterator<DoubleSummary> iiter = icsk.iterator();
      counter = 1;
      while (iiter.next()) {
        final int i = (int)iiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2
        assertEquals(i, 1);
      }
    }

    @Test
    public void example3() { //stateless: tuple1, tuple2, use dsso2
      //Load source sketches
      final UpdatableSketch<Double, DoubleSummary> tupleSk = tupleBldr.build();
      final UpdateSketch thetaSk = thetaBldr.build();
      for (int i = 1; i <= 12; i++) {
        tupleSk.update(i, 1.0);
        thetaSk.update(i + 3);
      }

      //Union
      final Union<DoubleSummary> union = new Union<>(dsso2);
      final CompactSketch<DoubleSummary> ucsk =
          union.union(tupleSk, thetaSk, ufactory.newSummary().update(1.0));
      int entries = ucsk.getRetainedEntries();
      println("Union: " + entries);
      final SketchIterator<DoubleSummary> uiter = ucsk.iterator();
      int counter = 1;
      int twos = 0;
      int ones = 0;
      while (uiter.next()) {
        final int i = (int)uiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
        if (i == 1) { ones++; }
        if (i == 2) { twos++; }
      }
      assertEquals(ones, 6);
      assertEquals(twos, 9);

      //Intersection
      final Intersection<DoubleSummary> inter = new Intersection<>(dsso2);
      final CompactSketch<DoubleSummary> icsk =
          inter.intersect(tupleSk, thetaSk, ufactory.newSummary().update(1.0));
      entries = icsk.getRetainedEntries();
      println("Intersection: " + entries);
      final SketchIterator<DoubleSummary> iiter = icsk.iterator();
      counter = 1;
      while (iiter.next()) {
        final int i = (int)iiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2
        assertEquals(i, 1);
      }
    }

    @Test
    public void example4() { //stateful: tuple, theta, Mode=sum for both, use dsso0
      //Load source sketches
      final UpdatableSketch<Double, DoubleSummary> tupleSk = tupleBldr.build();
      final UpdateSketch thetaSk = thetaBldr.build();
      for (int i = 1; i <= 12; i++) {
        tupleSk.update(i, 1.0);
        thetaSk.update(i + 3);
      }

      //Union
      final Union<DoubleSummary> union = new Union<>(dsso0);
      union.union(tupleSk);
      union.union(thetaSk, ufactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> ucsk = union.getResult();
      int entries = ucsk.getRetainedEntries();
      println("Union Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> uiter = ucsk.iterator();
      int counter = 1;
      int twos = 0;
      int ones = 0;
      while (uiter.next()) {
        final int i = (int)uiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
        if (i == 1) { ones++; }
        if (i == 2) { twos++; }
      }
      assertEquals(ones, 6);
      assertEquals(twos, 9);

      //Intersection
      final Intersection<DoubleSummary> inter = new Intersection<>(dsso0);
      inter.intersect(tupleSk);
      inter.intersect(thetaSk, ifactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> icsk = inter.getResult();
      entries = icsk.getRetainedEntries();
      println("Intersection Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> iiter = icsk.iterator();
      counter = 1;
      while (iiter.next()) {
        final int i = (int)iiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 1
        assertEquals(i, 2);
      }
    }

    @Test
    public void example5() { //stateful, tuple, theta, Mode=sum for both, use dsso1
      //Load source sketches
      final UpdatableSketch<Double, DoubleSummary> tupleSk = tupleBldr.build();
      final UpdateSketch thetaSk = thetaBldr.build();
      for (int i = 1; i <= 12; i++) {
        tupleSk.update(i, 1.0);
        thetaSk.update(i + 3);
      }

      //Union
      final Union<DoubleSummary> union = new Union<>(dsso1);
      union.union(tupleSk);
      union.union(thetaSk, ufactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> ucsk = union.getResult();
      int entries = ucsk.getRetainedEntries();
      println("Union Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> uiter = ucsk.iterator();
      int counter = 1;
      int twos = 0;
      int ones = 0;
      while (uiter.next()) {
        final int i = (int)uiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 2, 6 entries = 1
        if (i == 1) { ones++; }
        if (i == 2) { twos++; }
      }
      assertEquals(ones, 6);
      assertEquals(twos, 9);

      //Intersection
      final Intersection<DoubleSummary> inter = new Intersection<>(dsso1);
      inter.intersect(tupleSk);
      inter.intersect(thetaSk, ifactory.newSummary().update(1.0));
      final CompactSketch<DoubleSummary> icsk = inter.getResult();
      entries = icsk.getRetainedEntries();
      println("Intersection Stateful: tuple, theta: " + entries);
      final SketchIterator<DoubleSummary> iiter = icsk.iterator();
      counter = 1;
      while (iiter.next()) {
        final int i = (int)iiter.getSummary().getValue();
        println(counter++ + ", " + i); //9 entries = 1
        assertEquals(i, 2);
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
