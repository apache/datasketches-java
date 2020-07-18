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

import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EMPTY;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EST_HEAP;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EST_MEMORY_UNORDERED;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EXACT;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Random;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings({"javadoc","deprecation"})
public class SetOpsCornerCasesTest {

  /*******************************************/
  Random rand = new Random(9001); //deterministic

  @Test
  public void checkSetOpsRandom() {
    int hiA = 0, loB = 0, hiB = 0;
    for (int i = 0; i < 1000; i++) {
      hiA = rand.nextInt(128);      //skA fed values between 0 and 127
      loB = rand.nextInt(64);
      hiB = loB + rand.nextInt(64); //skB fed up to 63 values starting at loB
      compareSetOpsRandom(64, 0, hiA, loB, hiB);
    }
  }

  private static void compareSetOpsRandom(int k, int loA, int hiA, int loB, int hiB) {
    UpdateSketch tskA = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    UpdateSketch tskB = Sketches.updateSketchBuilder().setNominalEntries(k).build();

    for (int i = loA; i < hiA; i++) { tskA.update(i); }
    for (int i = loB; i < hiB; i++) { tskB.update(i); }

    CompactSketch rcskStdU = doStdUnion(tskA, tskB, k, null);
    CompactSketch rcskPwU = doPwUnion(tskA, tskB, k);
    checkCornerCase(rcskPwU, rcskStdU);

    CompactSketch rcskStdPairU = doStdPairUnion(tskA, tskB, k, null);
    checkCornerCase(rcskStdPairU, rcskStdU);

    CompactSketch rcskStdI = doStdIntersection(tskA, tskB, null);
    CompactSketch rcskPwI = doPwIntersection(tskA, tskB);
    checkCornerCase(rcskPwI, rcskStdI);

    CompactSketch rcskStdPairI = doStdPairIntersection(tskA, tskB, null);
    checkCornerCase(rcskStdPairI, rcskStdI);

    CompactSketch rcskStdAnotB = doStdAnotB(tskA, tskB, null);
    CompactSketch rcskPwAnotB = doPwAnotB(tskA, tskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB);

    CompactSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tskA, tskB, null);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB);
  }

  /*******************************************/

  @Test
  //Check all corner cases against standard Union, Intersection, and AnotB.
  //The unordered case is not tested
  public void compareCornerCases() {
    int k = 64;
    for (State stateA : State.values()) {
      for (State stateB : State.values()) {
        if ((stateA == EST_MEMORY_UNORDERED) || (stateB == EST_MEMORY_UNORDERED)) { continue; }
        if ((stateA == NULL) || (stateB == NULL)) { continue; }
        cornerCaseChecks(stateA, stateB, k);
        cornerCaseChecksMemory(stateA, stateB, k);
      }
    }
  }

//  @Test
//  public void checkExactNullSpecificCase() {
//    cornerCaseChecksMemory(State.EXACT, State.NULL, 64);
//  }

  private static void cornerCaseChecksMemory(State stateA, State stateB, int k) {
    println("StateA: " + stateA + ", StateB: " + stateB);
    CompactSketch tcskA = generate(stateA, k);
    CompactSketch tcskB = generate(stateB, k);

    WritableMemory wmem = WritableMemory.allocate(SetOperation.getMaxUnionBytes(k));

    CompactSketch rcskStdU = doStdUnion(tcskA, tcskB, k, null);
    CompactSketch rcskPwU = doPwUnion(tcskA, tcskB, k);
    checkCornerCase(rcskPwU, rcskStdU); //heap, heap

    rcskStdU = doStdUnion(tcskA, tcskB, k, wmem);
    CompactSketch rcskStdPairU = doStdPairUnion(tcskA, tcskB, k, wmem);
    checkCornerCase(rcskStdPairU, rcskStdU); //direct, direct

    wmem = WritableMemory.allocate(SetOperation.getMaxIntersectionBytes(k));

    CompactSketch rcskStdI = doStdIntersection(tcskA, tcskB, null);
    CompactSketch rcskPwI = doPwIntersection(tcskA, tcskB);
    checkCornerCase(rcskPwI, rcskStdI); //empty, empty

    rcskStdI = doStdIntersection(tcskA, tcskB, wmem);
    CompactSketch rcskStdPairI = doStdPairIntersection(tcskA, tcskB, wmem);
    checkCornerCase(rcskStdPairI, rcskStdI); //empty, empty //direct, direct???

    wmem = WritableMemory.allocate(SetOperation.getMaxAnotBResultBytes(k));

    CompactSketch rcskStdAnotB = doStdAnotB(tcskA, tcskB, null);
    CompactSketch rcskPwAnotB = doPwAnotB(tcskA, tcskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB); //heap, heap

    rcskStdAnotB = doStdAnotB(tcskA, tcskB, wmem);
    CompactSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tcskA, tcskB, wmem);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB); //direct, heap
  }

  private static void cornerCaseChecks(State stateA, State stateB, int k) {
    println("StateA: " + stateA + ", StateB: " + stateB);
    CompactSketch tcskA = generate(stateA, k);
    CompactSketch tcskB = generate(stateB, k);

    CompactSketch rcskStdU = doStdUnion(tcskA, tcskB, k, null);
    CompactSketch rcskPwU = doPwUnion(tcskA, tcskB, k);
    checkCornerCase(rcskPwU, rcskStdU);

    CompactSketch rcskStdPairU = doStdPairUnion(tcskA, tcskB, k, null);
    checkCornerCase(rcskStdPairU, rcskStdU);

    CompactSketch rcskStdI = doStdIntersection(tcskA, tcskB, null);
    CompactSketch rcskPwI = doPwIntersection(tcskA, tcskB);
    checkCornerCase(rcskPwI, rcskStdI);

    CompactSketch rcskStdPairI = doStdPairIntersection(tcskA, tcskB, null);
    checkCornerCase(rcskStdPairI, rcskStdI);

    CompactSketch rcskStdAnotB = doStdAnotB(tcskA, tcskB, null);
    CompactSketch rcskPwAnotB = doPwAnotB(tcskA, tcskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB);

    CompactSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tcskA, tcskB, null);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB);
  }

  private static CompactSketch doStdUnion(Sketch tskA, Sketch tskB, int k, WritableMemory wmem) {
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(tskA);
    union.update(tskB);
    return union.getResult(true, wmem);
  }

  private static CompactSketch doStdPairUnion(Sketch tskA, Sketch tskB, int k, WritableMemory wmem) {
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    return union.union(tskA, tskB, true, wmem);
  }

  private static CompactSketch doStdIntersection(Sketch tskA, Sketch tskB, WritableMemory wmem) {
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    inter.intersect(tskA);
    inter.intersect(tskB);
    return inter.getResult(true, wmem);
  }

  private static CompactSketch doStdPairIntersection(Sketch tskA, Sketch tskB, WritableMemory wmem) {
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();
    return inter.intersect(tskA, tskB, true, wmem);
  }

  private static CompactSketch doStdAnotB(Sketch tskA, Sketch tskB, WritableMemory wmem) {
    AnotB anotb = Sketches.setOperationBuilder().buildANotB();
    return anotb.aNotB(tskA, tskB, true, wmem);
  }

  private static CompactSketch doStdStatefulAnotB(Sketch tskA, Sketch tskB, WritableMemory wmem) {
    AnotB anotb = Sketches.setOperationBuilder().buildANotB();
    anotb.setA(tskA);
    anotb.notB(tskB);
    anotb.getResult(false);
    return anotb.getResult(true, wmem, true);
  }

  private static CompactSketch doPwUnion(Sketch tskA, Sketch tskB, int k) {
    CompactSketch tcskA, tcskB;
    if (tskA == null) { tcskA = null; }
    else { tcskA = (tskA instanceof CompactSketch) ? (CompactSketch) tskA : tskA.compact(); }
    if (tskB == null) { tcskB = null; }
    else { tcskB = (tskB instanceof CompactSketch) ? (CompactSketch) tskB : tskB.compact(); }
    return PairwiseSetOperations.union(tcskA, tcskB, k);
  }

  private static CompactSketch doPwIntersection(Sketch tskA, Sketch tskB) {
    return PairwiseSetOperations.intersect(tskA, tskB);
  }

  private static CompactSketch doPwAnotB(Sketch tskA, Sketch tskB) {
    return PairwiseSetOperations.aNotB(tskA, tskB);
  }


  private static void checkCornerCase(Sketch rskA, Sketch rskB) {
    double estA = rskA.getEstimate();
    double estB = rskB.getEstimate();
    boolean emptyA = rskA.isEmpty();
    boolean emptyB = rskB.isEmpty();
    long thetaLongA = rskA.getThetaLong();
    long thetaLongB = rskB.getThetaLong();
    int countA = rskA.getRetainedEntries(true);
    int countB = rskB.getRetainedEntries(true);
    Assert.assertEquals(estB, estA, 0.0);
    Assert.assertEquals(emptyB, emptyA);
    Assert.assertEquals(thetaLongB, thetaLongA);
    Assert.assertEquals(countB, countA);
    Assert.assertEquals(rskA.getClass().getSimpleName(), rskB.getClass().getSimpleName());
  }

  /*******************************************/

  @Test
  public void checkUnionNotOrdered() {
    int k = 64;
    CompactSketch skNull = generate(NULL, k);
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap = generate(EST_HEAP, k);
    CompactSketch skHeapUO = generate(EST_MEMORY_UNORDERED, k);

    PairwiseSetOperations.union(skNull, skHeapUO, k);
    PairwiseSetOperations.union(skEmpty, skHeapUO, k);
    PairwiseSetOperations.union(skHeapUO, skNull, k);
    PairwiseSetOperations.union(skHeapUO, skEmpty, k);
    PairwiseSetOperations.union(skHeapUO, skHeap, k);
    PairwiseSetOperations.union(skHeap, skHeapUO, k);
  }

  @Test
  public void checkSeedHash() {
    int k = 64;
    UpdateSketch tmp1 = Sketches.updateSketchBuilder().setNominalEntries(k).setSeed(123).build();
    tmp1.update(1);
    tmp1.update(3);
    CompactSketch skSmallSeed2A = tmp1.compact(true, null);

    UpdateSketch tmp2 = Sketches.updateSketchBuilder().setNominalEntries(k).setSeed(123).build();
    tmp2.update(1);
    tmp2.update(2);
    CompactSketch skSmallSeed2B = tmp2.compact(true, null);

    CompactSketch skExact = generate(EXACT, k);
    CompactSketch skHeap = generate(EST_HEAP, 2 * k);
    //Intersect
    try {
      PairwiseSetOperations.intersect(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.intersect(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
    //A NOT B
    try {
      PairwiseSetOperations.aNotB(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.aNotB(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
    //Union
    try {
      PairwiseSetOperations.union(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (Exception e) { } //pass
    try {
      PairwiseSetOperations.union(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (Exception e) { } //pass
  }

  @Test
  public void checkPwUnionReduceToK() {
    int k = 16;
    CompactSketch skNull = generate(NULL, k);
    CompactSketch skEmpty = generate(EMPTY, k);
    CompactSketch skHeap1 = generate(EST_HEAP, k);
    CompactSketch skHeap2 = generate(EST_HEAP, k);
    CompactSketch csk;
    csk = PairwiseSetOperations.union(skNull, skHeap1, k);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = PairwiseSetOperations.union(skEmpty, skHeap1, k);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = PairwiseSetOperations.union(skHeap1, skNull, k);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = PairwiseSetOperations.union(skHeap1, skEmpty, k);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = PairwiseSetOperations.union(skHeap1, skHeap2, k);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
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
  public void checkGenerator() {
    int k = 16;
    CompactSketch csk;

    csk = generate(State.NULL, 0);
    assertNull(csk);

    csk = generate(State.EMPTY, k);
    assertEquals(csk.isEmpty(), true);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.SINGLE, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 1);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EXACT, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), k);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_HEAP, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true) > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.THLT1_CNT0_FALSE, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.THEQ1_CNT0_TRUE, k);
    assertEquals(csk.isEmpty(), true);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, false);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_MEMORY_UNORDERED, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true) > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isDirect(), false);
    assertEquals(csk.hasMemory(), true);
    assertEquals(csk.isOrdered(), false);
  }

  enum State {NULL, EMPTY, SINGLE, EXACT, EST_HEAP, THLT1_CNT0_FALSE, THEQ1_CNT0_TRUE, EST_MEMORY_UNORDERED}

  private static CompactSketch generate(State state, int k) {
    UpdateSketch sk = null;
    CompactSketch csk = null;

    switch(state) {
      case NULL : {
        //already null
        break;
      }
      case EMPTY : { //results in EmptyCompactSketch
        csk = Sketches.updateSketchBuilder().setNominalEntries(k).build().compact(true, null);
        break;
      }
      case SINGLE : { //results in SingleItemSketches most of the time
        sk = Sketches.updateSketchBuilder().setNominalEntries(k).build();
        sk.update(1);
        csk = sk.compact(true, null);
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
      case THLT1_CNT0_FALSE : {
        sk = Sketches.updateSketchBuilder().setP((float)0.5).setNominalEntries(k).build();
        sk.update(7); //above theta
        assert(sk.getRetainedEntries(true) == 0);
        csk = sk.compact(true, null); //compact as {Th < 1.0, 0, F}
        break;
      }
      case THEQ1_CNT0_TRUE : {
        sk = Sketches.updateSketchBuilder().setP((float)0.5).setNominalEntries(k).build();
        assert(sk.getRetainedEntries(true) == 0);
        csk = sk.compact(true, null); //compact as {Th < 1.0, 0, T}
        break;
      }
      case EST_MEMORY_UNORDERED : {
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
