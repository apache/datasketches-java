/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EMPTY;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EST_HEAP;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.EST_HEAP_UNORDERED;
import static com.yahoo.sketches.theta.PairwiseCornerCasesTest.State.NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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
  //Check all corner cases against standard Union, except unordered, which is not allowed
  public void compareCornerCases() {
    int k = 64;
    for (State stateA : State.values()) {
      for (State stateB : State.values()) {
        if ((stateA == EST_HEAP_UNORDERED) || (stateB == EST_HEAP_UNORDERED)) { continue; }
        cornerCaseChecks(stateA, stateB, k);
      }
    }
  }

  @Test
  public void checkNull_CNT0_THLT1() {
    cornerCaseChecks(State.EXACT, State.CNT0_THLT1, 64);
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
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap = generate(EST_HEAP, k);
    CompactSketch skHeapUO = generate(EST_HEAP_UNORDERED, k);

    try {
      PairwiseSetOperations.union(skNull, skHeapUO, k);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skEmpty, skHeapUO, k);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.union(skHeapUO, skNull, k);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeapUO, skEmpty, k);
      Assert.fail();
    } catch (Exception e) { } //pass

    try {
      PairwiseSetOperations.union(skHeapUO, skHeap, k);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeap, skHeapUO, k);
      Assert.fail();
    } catch (Exception e) { } //pass
  }

  @Test
  public void checkSeedHash() {
    int k = 64;
    CompactSketch skEmptyS2 =
        Sketches.updateSketchBuilder().setNominalEntries(k).setSeed(123).build().compact(true, null);
    UpdateSketch tmp = Sketches.updateSketchBuilder().setNominalEntries(k).setSeed(123).build();
    tmp.update(1);
    CompactSketch skHeapS2 = tmp.compact(true, null);
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap = generate(EST_HEAP, 2 * k);
    //Intersect
    try {
      PairwiseSetOperations.intersect(skEmpty, skEmptyS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skEmpty, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skHeapS2, skEmpty);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skHeap, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    //A NOT B
    try {
      PairwiseSetOperations.aNotB(skEmpty, skEmptyS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skEmpty, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skHeapS2, skEmpty);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skHeap, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    //Union
    try {
      PairwiseSetOperations.union(skEmpty, skEmptyS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skEmpty, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeapS2, skEmpty);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeap, skHeapS2);
      Assert.fail();
    } catch (Exception e) { } //pass

  }

  @Test
  public void checkReduceToK() {
    int k = 16;
    CompactSketch skNull = generate(NULL, k);
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap1 = generate(EST_HEAP, k);
    CompactSketch skHeap2 = generate(EST_HEAP, k);
    CompactSketch csk;
    csk = PairwiseSetOperations.union(skNull, skHeap1, k);
    Assert.assertEquals(csk.getRetainedEntries(), k);
    csk = PairwiseSetOperations.union(skEmpty, skHeap1, k);
    Assert.assertEquals(csk.getRetainedEntries(), k);
    csk = PairwiseSetOperations.union(skHeap1, skNull, k);
    Assert.assertEquals(csk.getRetainedEntries(), k);
    csk = PairwiseSetOperations.union(skHeap1, skEmpty, k);
    Assert.assertEquals(csk.getRetainedEntries(), k);
    csk = PairwiseSetOperations.union(skHeap1, skHeap2, k);
    Assert.assertEquals(csk.getRetainedEntries(), k);
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

  @Test
  public void checkGenerate() {
    int k = 16;
    CompactSketch csk;

    csk = generate(State.NULL, 0);
    assertNull(csk);

    csk = generate(State.EMPTY, k);
    assertEquals(csk.isEmpty(), true);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(), 0);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EXACT, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(), k);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_HEAP, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries() > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.CNT0_THLT1, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(), 0);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_HEAP_UNORDERED, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries() > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.isOrdered(), false);
  }

  enum State {NULL, EMPTY, EXACT, EST_HEAP, CNT0_THLT1, EST_HEAP_UNORDERED}

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
        for (int i = 0; i < k; i++) {
          sk.update(i);
        }
        csk = sk.compact(true, null);
        break;
      }
      case EST_HEAP : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < (4 * k); i++) {
          sk.update(i);
        }
        csk = sk.compact(true, null);
        break;
      }
      case CNT0_THLT1 : {
        sk = Sketches.updateSketchBuilder().setP((float)0.5).setNominalEntries(k).build();
        sk.update(7);
        assert(sk.getRetainedEntries() == 0);
        csk = sk.compact(true, null);
        break;
      }
      case EST_HEAP_UNORDERED : {
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        for (int i = 0; i < (4 * k); i++) {
          sk.update(i);
        }
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
