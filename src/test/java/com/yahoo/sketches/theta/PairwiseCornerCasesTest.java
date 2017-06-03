/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EMPTY;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EST_DIR;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EST_HEAP;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EST_HEAP_UNORDERED;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.NULL;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

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
    UpdateSketch skA = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    UpdateSketch skB = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
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
  public void checkCornerCases() { //Check all corner cases except unordered, which is not allowed
    int k = 64;
    for (State stateA : State.values()) {
      for (State stateB : State.values()) {
        if ((stateA == EST_HEAP_UNORDERED) || (stateB == EST_HEAP_UNORDERED)) { continue; }
        cornerCaseChecks(stateA, stateB, k);
      }
    }
  }

  private static void cornerCaseChecks(State stateA, State stateB, int k) {
    println("StateA: " + stateA + ", StateB: " + stateB);
    CompactSketch cskA = generate(stateA, k);
    CompactSketch cskB = generate(stateB, k);
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
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

    if ((stateA == NULL) && (stateB == NULL)) {
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

    if ((stateA == NULL) && (stateB == NULL)) {
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

    if ((stateA == NULL) && (stateB == NULL)) {
      Assert.assertEquals(pwEst, -1.0, 0.0);
      Assert.assertEquals(stdEst, 0.0, 0.0);
    } else {
      Assert.assertEquals(pwEst, stdEst, 0.0);
    }
    Assert.assertEquals(pwEmpty, stdEmpty);
    Assert.assertEquals(pwTheta, stdTheta, 0.0);
    Assert.assertEquals(pwEnt, stdEnt);
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
    PairwiseSetOperations.union(skDir, skEmpty, k);
    PairwiseSetOperations.union(skEmpty, skDir, k);
  }

  @Test
  public void checkDefaultK() {
    CompactSketch skHeap1 = generate(EST_HEAP, 4096);
    CompactSketch skHeap2 = generate(EST_HEAP, 4096);
    PairwiseSetOperations.union(skHeap1, skHeap2);
    Assert.assertEquals(skHeap1.getEstimate(), skHeap2.getEstimate());
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

  enum State {NULL, EMPTY, EXACT, EST_HEAP, EST_DIR, EMPTY_THLT0, EST_HEAP_UNORDERED}

  private static CompactSketch generate(State state, int k) {
    UpdateSketch sk = null;
    CompactSketch csk = null;

    switch(state) {
      case NULL : {
        //already null
        break;
      }
      case EMPTY : {
        csk = Sketches.updateSketchBuilder().setNominalEntries(k).build().compact(true, null);
        break;
      }
      case EXACT : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < k; i++) sk.update(i);
        csk = sk.compact(true, null);
        break;
      }
      case EST_HEAP : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < 4*k; i++) sk.update(i);
        csk = sk.compact(true, null);
        break;
      }
      case EST_DIR : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < 4 * k; i++) sk.update(i);
        int bytes = Sketch.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
        byte[] byteArr = new byte[bytes];
        WritableMemory mem = WritableMemory.wrap(byteArr);
        csk = sk.compact(true, mem);
        break;
      }
      case EMPTY_THLT0 : {
        csk = Sketches.updateSketchBuilder().setP((float)0.5).setNominalEntries(k).build().compact(true, null);
        break;
      }
      case EST_HEAP_UNORDERED : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < 4 * k; i++) sk.update(i);
        int bytes = Sketch.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
        byte[] byteArr = new byte[bytes];
        WritableMemory mem = WritableMemory.wrap(byteArr);
        csk = sk.compact(false, mem);
        break;
      }
    }
    return csk;
  }

}
