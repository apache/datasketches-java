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

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

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

      inter.update(csk1);
      inter.update(csk2);
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
    int lgK = 10; //6
    int k = 1<<lgK;
    int u = 4*k; //1K + 57 fails
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

      aNotB.update(csk1, csk2);
      CompactSketch csk3 = aNotB.getResult(true, null);
      double result2 = csk3.getEstimate();

      assertEquals(result1, result2, 0.0);

      usk1.reset();
      usk2.reset();
      //aNotB is stateless, so no reset()
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
   int u = 4*k; //4096 + 2048 = 6144
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
 public void checkEmptyNullRules() {
   int k = 16;
   UpdateSketch uskA = UpdateSketch.builder().setNominalEntries(k).build();
   UpdateSketch uskB = UpdateSketch.builder().setNominalEntries(k).build();
   CompactSketch cskAempty = uskA.compact();
   CompactSketch cskBempty = uskB.compact();
   CompactSketch cskAnull = null;
   CompactSketch cskBnull = null;
   Union union = SetOperation.builder().setNominalEntries(k).buildUnion();
   AnotB aNotB = SetOperation.builder().buildANotB();
   Intersection inter = SetOperation.builder().buildIntersection();
   CompactSketch cskC, cskR;

   //Null, Null
   union.update(cskAnull);
   union.update(cskBnull);
   cskC = union.getResult();
   cskR = PairwiseSetOperations.union(cskAnull, cskBnull, k);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   union.reset();

   inter.update(cskAnull);
   inter.update(cskBnull);
   cskC = inter.getResult();
   cskR = PairwiseSetOperations.intersect(cskAnull, cskBnull);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   inter.reset();

   aNotB.update(cskAnull, cskBnull);
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskAnull, cskBnull);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());

   //Null, Empty
   union.update(cskAnull);
   union.update(cskBempty);
   cskC = union.getResult();
   cskR = PairwiseSetOperations.union(cskAnull, cskBempty, k);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   union.reset();

   inter.update(cskAnull);
   inter.update(cskBempty);
   cskC = inter.getResult();
   cskR = PairwiseSetOperations.intersect(cskAnull, cskBempty);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   inter.reset();

   aNotB.update(cskAnull, cskBempty);
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskAnull, cskBempty);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());

   //Empty, Null
   union.update(cskAempty);
   union.update(cskBnull);
   cskC = union.getResult();
   cskR = PairwiseSetOperations.union(cskAempty, cskBnull, k);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   union.reset();

   inter.update(cskAempty);
   inter.update(cskBnull);
   cskC = inter.getResult();
   cskR = PairwiseSetOperations.intersect(cskAempty, cskBnull);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   inter.reset();

   aNotB.update(cskAempty, cskBnull);
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskAempty, cskBnull);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());

   //Empty, Empty
   union.update(cskAempty);
   union.update(cskBempty);
   cskC = union.getResult();
   cskR = PairwiseSetOperations.union(cskAempty, cskBempty, k);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   union.reset();

   inter.update(cskAempty);
   inter.update(cskBempty);
   cskC = inter.getResult();
   cskR = PairwiseSetOperations.intersect(cskAempty, cskBempty);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   inter.reset();

   aNotB.update(cskAempty, cskBempty);
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskAempty, cskBempty);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());

   //NotEmpty, Empty
   uskA.update(1);
   CompactSketch cskA1 = uskA.compact();

   union.update(cskA1);
   union.update(cskBempty);
   cskC = union.getResult();
   cskR = PairwiseSetOperations.union(cskA1, cskBempty, k);
   assertEquals(!cskC.isEmpty(), !cskR.isEmpty());
   union.reset();

   inter.update(cskA1);
   inter.update(cskBempty);
   cskC = inter.getResult();
   cskR = PairwiseSetOperations.intersect(cskA1, cskBempty);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
   inter.reset();

   aNotB.update(cskA1, cskBempty);
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskA1, cskBempty);
   assertEquals(!cskC.isEmpty(), !cskR.isEmpty());

   aNotB.update(cskBempty, cskA1);  //check the reverse
   cskC = aNotB.getResult();
   cskR = PairwiseSetOperations.aNotB(cskBempty, cskA1);
   assertEquals(cskC.isEmpty(), cskR.isEmpty());
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
