/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.NativeMemory;

public class PairwiseCornerCasesTest {

  Random rand = new Random(9001); //deterministic

  @Test
  public void checkSetOps() {
    int hiA = 0, loB = 0, hiB = 0;
    for (int i = 0; i < 1000; i++) {
      hiA = 0 + rand.nextInt(128);
      loB = rand.nextInt(64);
      hiB = 0 + loB + rand.nextInt(64);
      compareSetOps(64, 0, hiA, loB, hiB);
    }
  }

  private static void compareSetOps(int k, int loA, int hiA, int loB, int hiB) {
    UpdateSketch skA = Sketches.updateSketchBuilder().build(k);
    UpdateSketch skB = Sketches.updateSketchBuilder().build(k);
    Union union = Sketches.setOperationBuilder().buildUnion(k);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();

    for (int i = loA; i < hiA; i++) {
      skA.update(i);
    }

    for (int i = loB; i < hiB; i++) {
      skB.update(i);
    }

    union.update(skA);
    union.update(skB);
    CompactSketch comp = union.getResult(true, null);
    double unionEst = comp.getEstimate();

    CompactSketch cskA = skA.compact();
    CompactSketch cskB = skB.compact();
    CompactSketch pwComp = PairwiseSetOperations.union(cskA, cskB, k);
    double pwUnionEst = pwComp.getEstimate();
    Assert.assertEquals(pwUnionEst, unionEst, 0.0);

    inter.update(skA);
    inter.update(skB);
    comp = inter.getResult(true, null);
    double interEst = comp.getEstimate();
    cskA = skA.compact();
    cskB = skB.compact();
    pwComp = PairwiseSetOperations.intersect(cskA, cskB);
    double pwInterEst = pwComp.getEstimate();
    Assert.assertEquals(pwInterEst, interEst, 0.0);

    aNotB.update(skA, skB);
    comp = aNotB.getResult(true, null);
    double aNbEst = comp.getEstimate();
    cskA = skA.compact();
    cskB = skB.compact();
    pwComp = PairwiseSetOperations.aNotB(cskA, cskB);
    double pwAnBEst = pwComp.getEstimate();
    Assert.assertEquals(pwAnBEst, aNbEst);
  }

  @Test
  public void checkCornerCases() {
    int k = 64;
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        cornerCaseChecks(i, j, k);
      }
    }
  }


  private static void cornerCaseChecks(int stateA, int stateB, int k) {
    //println("StateA: " + stateA + ", StateB: " + stateB);
    CompactSketch cskA = generate(stateA, k);
    CompactSketch cskB = generate(stateB, k);
    Union union = Sketches.setOperationBuilder().buildUnion(k);
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();

    //UNION
    union.update(cskA);
    union.update(cskB);
    CompactSketch comp = union.getResult(true, null);
    double stdEst = comp.getEstimate();
    boolean stdEmpty = comp.isEmpty();
    double stdTheta = comp.getTheta();
    int stdEnt = comp.getRetainedEntries(true);

    CompactSketch pwComp = PairwiseSetOperations.union(cskA, cskB, k);
    double pwEst = (pwComp != null)? pwComp.getEstimate() : -1.0;
    boolean pwEmpty = (pwComp != null)? pwComp.isEmpty() : true;
    double pwTheta = (pwComp != null)? pwComp.getTheta() : 1.0;
    int pwEnt = (pwComp != null)? pwComp.getRetainedEntries(true) : 0;

    if (stateA == 0 && stateB == 0) {
      Assert.assertEquals(pwEst, -1.0, 0.0);
      Assert.assertEquals(stdEst, 0.0, 0.0);
    } else {
      Assert.assertEquals(pwEst, stdEst, 0.0);
    }
    Assert.assertEquals(pwEmpty, stdEmpty);
    Assert.assertEquals(pwTheta, stdTheta, 0.0);
    Assert.assertEquals(pwEnt, stdEnt);

    //INTERSECT
    inter.update(cskA);
    inter.update(cskB);
    comp = inter.getResult(true, null);
    stdEst = comp.getEstimate();
    stdEmpty = comp.isEmpty();
    stdTheta = comp.getTheta();
    stdEnt = comp.getRetainedEntries(true);

    pwComp = PairwiseSetOperations.intersect(cskA, cskB);
    pwEst = (pwComp != null)? pwComp.getEstimate() : -1.0;
    pwEmpty = (pwComp != null)? pwComp.isEmpty() : true;
    pwTheta = (pwComp != null)? pwComp.getTheta() : 1.0;
    pwEnt = (pwComp != null)? pwComp.getRetainedEntries(true) : 0;

    if (stateA == 0 && stateB == 0) {
      Assert.assertEquals(pwEst, -1.0, 0.0);
      Assert.assertEquals(stdEst, 0.0, 0.0);
    } else {
      Assert.assertEquals(pwEst, stdEst, 0.0);
    }
    Assert.assertEquals(pwEmpty, stdEmpty);
    Assert.assertEquals(pwTheta, stdTheta, 0.0);
    Assert.assertEquals(pwEnt, stdEnt);

    //A NOT B
    aNotB.update(cskA, cskB);
    comp = aNotB.getResult(true, null);
    stdEst =comp.getEstimate();
    stdEmpty = comp.isEmpty();
    stdTheta = comp.getTheta();
    stdEnt = comp.getRetainedEntries(true);

    pwComp = PairwiseSetOperations.aNotB(cskA, cskB);
    pwEst = (pwComp != null)? pwComp.getEstimate() : -1.0;
    pwEmpty = (pwComp != null)? pwComp.isEmpty() : true;
    pwTheta = (pwComp != null)? pwComp.getTheta() : 1.0;
    pwEnt = (pwComp != null)? pwComp.getRetainedEntries(true) : 0;

    if (stateA == 0 && stateB == 0) {
      Assert.assertEquals(pwEst, -1.0, 0.0);
      Assert.assertEquals(stdEst, 0.0, 0.0);
    } else {
      Assert.assertEquals(pwEst, stdEst, 0.0);
    }
    Assert.assertEquals(pwEmpty, stdEmpty);
    Assert.assertEquals(pwTheta, stdTheta, 0.0);
    Assert.assertEquals(pwEnt, stdEnt);
  }
  final static int NULL     = 0;
  final static int EMPTY    = 1;
  final static int EXACT    = 2;
  final static int EST_HEAP = 3;
  final static int EST_DIR  = 4;
  final static int EST_HEAP_UNORDERED = 5;

  private static CompactSketch generate(int state, int k) {
    if (state == NULL) return null;
    if (state == EMPTY) return Sketches.updateSketchBuilder().build(k).compact(true, null);
    if (state == EXACT) {
      UpdateSketch sk = Sketches.updateSketchBuilder().build(k);
      for (int i = 0; i < k; i++) sk.update(i);
      return sk.compact(true, null);
    }
    if (state == EST_HEAP) {
      UpdateSketch sk = Sketches.updateSketchBuilder().build(k);
      for (int i = 0; i < 4*k; i++) sk.update(i);
      return sk.compact(true, null);
    }
    if (state == EST_DIR) {
      UpdateSketch sk = Sketches.updateSketchBuilder().build(k);
      for (int i = 0; i < 4 * k; i++) sk.update(i);
      int bytes = Sketch.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
      byte[] byteArr = new byte[bytes];
      NativeMemory mem = new NativeMemory(byteArr);
      return sk.compact(true, mem);
    }
    if (state == EST_HEAP_UNORDERED) {
      UpdateSketch sk = Sketches.updateSketchBuilder().build(k);
      for (int i = 0; i < 4 * k; i++) sk.update(i);
      int bytes = Sketch.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
      byte[] byteArr = new byte[bytes];
      NativeMemory mem = new NativeMemory(byteArr);
      return sk.compact(false, mem);
    }

    return null;
  }

  @Test
  public void checkNotOrdered() {
    int k = 64;
    CompactSketch skNull = generate(NULL, k);
    CompactSketch skHeap = generate(EST_HEAP, k);
    CompactSketch skHeapUO = generate(EST_HEAP_UNORDERED, k);
    try {
      PairwiseSetOperations.union(skHeap, skHeapUO, k);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeapUO, skHeap, k);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.union(skNull, skHeapUO, k);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeapUO, skNull, k);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.aNotB(skHeap, skHeapUO);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skHeapUO, skHeap);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.aNotB(skNull, skHeapUO);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skHeapUO, skNull);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.intersect(skHeap, skHeapUO);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skHeapUO, skHeap);
      Assert.fail();
    } catch (Exception e) { } //pass
  }

  @Test
  public void checkReduceToK() {
    int k = 64;
    CompactSketch skNull = generate(NULL, k);
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap = generate(EST_HEAP, 2 * k);
    CompactSketch skDir = generate(EST_DIR, 2 * k);

    PairwiseSetOperations.union(skEmpty, skHeap, k);
    PairwiseSetOperations.union(skNull, skHeap, k);
    PairwiseSetOperations.union(skHeap, skNull, k);
    PairwiseSetOperations.union(skNull, skDir, k);
    PairwiseSetOperations.union(skDir, skNull, k);
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
