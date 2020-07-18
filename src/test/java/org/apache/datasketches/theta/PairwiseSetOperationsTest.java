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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.SketchesArgumentException;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","deprecation"})
public class PairwiseSetOperationsTest {

  // Intersection

  @Test
  public void checkIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) { //<k so est is exact
      usk1.update(i);
      usk2.update(i + k);
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Sketch rsk = PairwiseSetOperations.intersect(csk1, csk2);
    assertEquals(rsk.getEstimate(), 0.0);
  }

  @Test
  public void checkIntersectionFullOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) { //<k so est is exact
      usk1.update(i);
      usk2.update(i);
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Sketch rsk = PairwiseSetOperations.intersect(csk1, csk2);
    assertEquals(rsk.getEstimate(), k, 0.0);
  }

  @Test
  public void checkIntersectionEarlyStop() {
    int lgK = 10;
    int k = 1<<lgK;
    int u = 4*k;
    long v = 0;
    int trials = 10;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
    Intersection inter = SetOperation.builder().buildIntersection();

    for (int t = 0; t < trials; t++) {
      for (int i=0; i<u; i++) {
        usk1.update(i + v);
        usk2.update(i + v + (u/2));
      }
      v += u + (u/2);

      CompactSketch csk1 = usk1.compact(true, null);
      CompactSketch csk2 = usk2.compact(true, null);

      Sketch rsk = PairwiseSetOperations.intersect(csk1, csk2);
      double result1 = rsk.getEstimate();

      inter.intersect(csk1);
      inter.intersect(csk2);
      CompactSketch csk3 = inter.getResult(true, null);
      double result2 = csk3.getEstimate();

      assertEquals(result1, result2, 0.0);

      usk1.reset();
      usk2.reset();
      inter.reset();
    }
  }

// A and not B

  @Test
  public void checkAnotBNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
      usk2.update(i + k);
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Sketch rsk = PairwiseSetOperations.aNotB(csk1, csk2);
    assertEquals(rsk.getEstimate(), k, 0.0);
  }

  @Test
  public void checkAnotBFullOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

    for (int i=0; i<k; i++) {
      usk1.update(i);
      usk2.update(i);
    }

    CompactSketch csk1 = usk1.compact(true, null);
    CompactSketch csk2 = usk2.compact(true, null);

    Sketch rsk = PairwiseSetOperations.aNotB(csk1, csk2);
    assertEquals(rsk.getEstimate(), 0.0, 0.0);
  }

  @Test
  public void checkAnotBEarlyStop() {
    int lgK = 10;
    int k = 1<<lgK;
    int u = 4*k;
    long v = 0;
    int trials = 10;

    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
    UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
    AnotB aNotB = SetOperation.builder().buildANotB();

    for (int t = 0; t < trials; t++) {
      for (int i=0; i<u; i++) {
        usk1.update(i + v);
        usk2.update(i + v + (u/2));
      }
      v += u + (u/2);

      CompactSketch csk1 = usk1.compact(true, null);
      CompactSketch csk2 = usk2.compact(true, null);

      Sketch rsk = PairwiseSetOperations.aNotB(csk1, csk2);
      double result1 = rsk.getEstimate();

      CompactSketch csk3 = aNotB.aNotB(csk1, csk2);
      double result2 = csk3.getEstimate();

      assertEquals(result1, result2, 0.0);

      usk1.reset();
      usk2.reset();
    }
  }

//Union

 @Test
 public void checkUnionNoOverlap() {
   int lgK = 9;
   int k = 1<<lgK;

   UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
   Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();

   for (int i=0; i<k; i++) {
     usk1.update(i);
     usk2.update(i + k);
   }

   CompactSketch csk1 = usk1.compact(true, null);
   CompactSketch csk2 = usk2.compact(true, null);

   union.update(csk1);
   union.update(csk2);
   Sketch stdSk = union.getResult(true, null);

   Sketch pwSk = PairwiseSetOperations.union(csk1, csk2, k);

   assertEquals(pwSk.getEstimate(), stdSk.getEstimate(), 0.0);
 }

 @Test
 public void checkUnionFullOverlap() {
   int lgK = 9;
   int k = 1<<lgK;

   UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();

   for (int i=0; i<k; i++) {
     usk1.update(i);
     usk2.update(i);
   }

   CompactSketch csk1 = usk1.compact(true, null);
   CompactSketch csk2 = usk2.compact(true, null);

   Sketch rsk = PairwiseSetOperations.union(csk1, csk2, k);
   assertEquals(rsk.getEstimate(), k, 0.0);
 }

 @Test
 public void checkUnionEarlyStop() {
   int lgK = 10;
   int k = 1<<lgK;
   int u = 4*k;
   long v = 0;
   int trials = 10;

   UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
   Union union = SetOperation.builder().setNominalEntries(2 * k).buildUnion();

   for (int t = 0; t < trials; t++) {
     for (int i=0; i<u; i++) {
       usk1.update(i + v);
       usk2.update(i + v + (u/2));
     }
     v += u + (u/2);

     CompactSketch csk1 = usk1.compact(true, null);
     CompactSketch csk2 = usk2.compact(true, null);

     Sketch pwSk = PairwiseSetOperations.union(csk1, csk2, 2 * k);
     double pwEst = pwSk.getEstimate();

     union.update(csk1);
     union.update(csk2);
     CompactSketch stdSk = union.getResult(true, null);
     double stdEst = stdSk.getEstimate();

     assertEquals(pwEst, stdEst, 0.0);

     usk1.reset();
     usk2.reset();
     union.reset();
   }
 }

 @Test
 public void checkUnionCutbackToK() {
   int lgK = 10;
   int k = 1<<lgK;
   int u = (3 * k);

   UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch usk2 = UpdateSketch.builder().setNominalEntries(k).build();
   Union union = SetOperation.builder().setNominalEntries(k).buildUnion();

   for (int i=0; i < u; i++) {
     usk1.update(i);
     usk2.update(i + (2 * u));
   }

   CompactSketch csk1 = usk1.compact(true, null);
   CompactSketch csk2 = usk2.compact(true, null);

   Sketch pwSk = PairwiseSetOperations.union(csk1, csk2, k);
   double pwEst = pwSk.getEstimate();

   union.update(csk1);
   union.update(csk2);
   CompactSketch stdSk = union.getResult(true, null);
   double stdEst = stdSk.getEstimate();

   assertEquals(pwEst, stdEst, stdEst * .06);

   usk1.reset();
   usk2.reset();
   union.reset();

 }

 @Test
 public void checkNullRules() {
   int k = 16;
   UpdateSketch uskA = UpdateSketch.builder().setNominalEntries(k).build();
   CompactSketch cskAempty = uskA.compact();
   CompactSketch cskAnull = null;

   AnotB aNotB = SetOperation.builder().buildANotB();
   Intersection inter = SetOperation.builder().buildIntersection();

   try {
     checkIntersection(inter, cskAnull, cskAempty);
     fail();
   } catch (SketchesArgumentException e) { }
   try {
     checkIntersection(inter, cskAempty, cskAnull);
     fail();
   } catch (SketchesArgumentException e) { }
   try {
     checkIntersection(inter, cskAnull, cskAnull);
     fail();
   } catch (SketchesArgumentException e) { }

   try {
     checkAnotB(aNotB, cskAnull, cskAempty);
     fail();
   } catch (SketchesArgumentException e) { }
   try {
     checkAnotB(aNotB, cskAempty, cskAnull);
     fail();
   } catch (SketchesArgumentException e) { }
   try {
     checkAnotB(aNotB, cskAnull, cskAnull);
     fail();
   } catch (SketchesArgumentException e) { }

 }

 @Test
 public void checkEmptyValidRules() {
   int k = 16;
   UpdateSketch uskA = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch uskB = UpdateSketch.builder().setNominalEntries(k).build();
   CompactSketch cskAempty = uskA.compact();
   CompactSketch cskBempty = uskB.compact();
   uskA.update(1);
   CompactSketch cskA1 = uskA.compact();

   Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
   AnotB aNotB = SetOperation.builder().buildANotB();
   Intersection inter = SetOperation.builder().buildIntersection();

   checkSetOps(union, inter, aNotB, k, cskAempty, cskBempty); //Empty, Empty
   checkSetOps(union, inter, aNotB, k, cskA1, cskBempty);     //NotEmpty, Empty
   checkSetOps(union, inter, aNotB, k, cskAempty, cskA1);     //Empty, NotEmpty
 }

 private static void checkSetOps(Union union, Intersection inter, AnotB aNotB, int k,
     CompactSketch cskA, CompactSketch cskB) {
   checkUnion(union, cskA, cskB, k);
   checkIntersection(inter, cskA, cskB);
   checkAnotB(aNotB, cskA, cskB);
 }

 private static void checkUnion(Union union, CompactSketch cskA, CompactSketch cskB, int k) {
   union.update(cskA);
   union.update(cskB);
   CompactSketch cskU = union.getResult();
   CompactSketch cskP = PairwiseSetOperations.union(cskA, cskB, k);
   assertEquals(cskU.isEmpty(), cskP.isEmpty());
   union.reset();
 }

 private static void checkIntersection(Intersection inter, CompactSketch cskA, CompactSketch cskB) {
   inter.intersect(cskA);
   inter.intersect(cskB);
   CompactSketch cskI = inter.getResult();
   CompactSketch cskP = PairwiseSetOperations.intersect(cskA, cskB);
   assertEquals(cskI.isEmpty(), cskP.isEmpty());
   inter.reset();
 }

 private static void checkAnotB(AnotB aNotB, CompactSketch cskA, CompactSketch cskB) {
   CompactSketch cskD = aNotB.aNotB(cskA, cskB);
   CompactSketch cskP = PairwiseSetOperations.aNotB(cskA, cskB);
   assertEquals(cskD.isEmpty(), cskP.isEmpty());
 }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
