/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

public class PairwiseSetOperationsTest {

  // Intersection

  @Test
  public void checkIntersectionNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);

    for (int i=0; i<k; i++) {
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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);

    for (int i=0; i<k; i++) {
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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    Intersection inter = SetOperation.builder().buildIntersection();

    for (int t = 0; t < trials; t++) {
      for (int i=0; i<u; i++) {
        usk1.update(i + v);
        usk2.update(i + v + u/2);
      }
      v += u + u/2;

      CompactSketch csk1 = usk1.compact(true, null);
      CompactSketch csk2 = usk2.compact(true, null);

      Sketch rsk = PairwiseSetOperations.intersect(csk1, csk2);
      double result1 = rsk.getEstimate();

      inter.update(csk1);
      inter.update(csk2);
      CompactSketch csk3 = inter.getResult(true, null);
      double result2 = csk3.getEstimate();

      println(result1 + ", "+ result2);
      assertEquals(result1, result2, 0.0);
      usk1.reset();
      usk2.reset();
      inter.reset();
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIntersectionBadArguments() {
    int lgK = 10;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    PairwiseSetOperations.intersect(usk1, usk2);
  }

// A and not B

  @Test
  public void checkAnotBNoOverlap() {
    int lgK = 9;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);

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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);

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

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    AnotB aNotB = SetOperation.builder().buildANotB();

    for (int t = 0; t < trials; t++) {
      for (int i=0; i<u; i++) {
        usk1.update(i + v);
        usk2.update(i + v + u/2);
      }
      v += u + u/2;

      CompactSketch csk1 = usk1.compact(true, null);
      CompactSketch csk2 = usk2.compact(true, null);

      Sketch rsk = PairwiseSetOperations.aNotB(csk1, csk2);
      double result1 = rsk.getEstimate();

      aNotB.update(csk1, csk2);
      CompactSketch csk3 = aNotB.getResult(true, null);
      double result2 = csk3.getEstimate();

      println(result1 + ", "+ result2);
      assertEquals(result1, result2, 0.0);
      usk1.reset();
      usk2.reset();
      //aNotB is stateless, so not resetable
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAnotBBadArguments() {
    int lgK = 10;
    int k = 1<<lgK;

    UpdateSketch usk1 = UpdateSketch.builder().build(k);
    UpdateSketch usk2 = UpdateSketch.builder().build(k);
    PairwiseSetOperations.aNotB(usk1, usk2);
  }

//Union

 @Test
 public void checkUnionNoOverlap() {
   int lgK = 9;
   int k = 1<<lgK;

   UpdateSketch usk1 = UpdateSketch.builder().build(k);
   UpdateSketch usk2 = UpdateSketch.builder().build(k);

   for (int i=0; i<k; i++) {
     usk1.update(i);
     usk2.update(i + k);
   }

   CompactSketch csk1 = usk1.compact(true, null);
   CompactSketch csk2 = usk2.compact(true, null);

   Sketch rsk = PairwiseSetOperations.union(csk1, csk2);
   assertEquals(rsk.getEstimate(), 2*k, 0.0);
 }

 @Test
 public void checkUnionFullOverlap() {
   int lgK = 9;
   int k = 1<<lgK;

   UpdateSketch usk1 = UpdateSketch.builder().build(k);
   UpdateSketch usk2 = UpdateSketch.builder().build(k);

   for (int i=0; i<k; i++) {
     usk1.update(i);
     usk2.update(i);
   }

   CompactSketch csk1 = usk1.compact(true, null);
   CompactSketch csk2 = usk2.compact(true, null);

   Sketch rsk = PairwiseSetOperations.union(csk1, csk2);
   assertEquals(rsk.getEstimate(), k, 0.0);
 }

 @Test
 public void checkUnionEarlyStop() {
   int lgK = 10;
   int k = 1<<lgK;
   int u = 4*k; //4096 + 2048 = 6144
   long v = 0;
   int trials = 10;

   UpdateSketch usk1 = UpdateSketch.builder().build(k);
   UpdateSketch usk2 = UpdateSketch.builder().build(k);
   Union union = SetOperation.builder().buildUnion(2 * k);

   for (int t = 0; t < trials; t++) {
     for (int i=0; i<u; i++) {
       usk1.update(i + v);
       usk2.update(i + v + u/2);
     }
     v += u + u/2;

     CompactSketch csk1 = usk1.compact(true, null);
     CompactSketch csk2 = usk2.compact(true, null);

     Sketch rsk = PairwiseSetOperations.union(csk1, csk2);
     double result1 = rsk.getEstimate();

     union.update(csk1);
     union.update(csk2);
     CompactSketch csk3 = union.getResult(true, null);
     double result2 = csk3.getEstimate();

     println(result1 + ", "+ result2);
     assertEquals(result1, result2, 0.0);
     usk1.reset();
     usk2.reset();
     union.reset();
   }
 }

 @Test(expectedExceptions = SketchesArgumentException.class)
 public void checkUnionBadArguments() {
   int lgK = 10;
   int k = 1<<lgK;

   UpdateSketch usk1 = UpdateSketch.builder().build(k);
   UpdateSketch usk2 = UpdateSketch.builder().build(k);
   PairwiseSetOperations.union(usk1, usk2);
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
