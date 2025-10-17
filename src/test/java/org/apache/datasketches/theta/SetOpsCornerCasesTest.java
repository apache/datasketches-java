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
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EST_SEGMENT_UNORDERED;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.EXACT;
import static org.apache.datasketches.theta.SetOpsCornerCasesTest.State.NULL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

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

  private static void compareSetOpsRandom(final int k, final int loA, final int hiA, final int loB, final int hiB) {
    final UpdatableThetaSketch tskA = UpdatableThetaSketch.builder().setNominalEntries(k).build();
    final UpdatableThetaSketch tskB = UpdatableThetaSketch.builder().setNominalEntries(k).build();

    for (int i = loA; i < hiA; i++) { tskA.update(i); }
    for (int i = loB; i < hiB; i++) { tskB.update(i); }

    final CompactThetaSketch rcskStdU = doStdUnion(tskA, tskB, k, null);
    final CompactThetaSketch rcskPwU = doPwUnion(tskA, tskB, k);
    checkCornerCase(rcskPwU, rcskStdU);

    final CompactThetaSketch rcskStdPairU = doStdPairUnion(tskA, tskB, k, null);
    checkCornerCase(rcskStdPairU, rcskStdU);

    final CompactThetaSketch rcskStdI = doStdIntersection(tskA, tskB, null);
    final CompactThetaSketch rcskPwI = doPwIntersection(tskA, tskB);
    checkCornerCase(rcskPwI, rcskStdI);

    final CompactThetaSketch rcskStdPairI = doStdPairIntersection(tskA, tskB, null);
    checkCornerCase(rcskStdPairI, rcskStdI);

    final CompactThetaSketch rcskStdAnotB = doStdAnotB(tskA, tskB, null);
    final CompactThetaSketch rcskPwAnotB = doPwAnotB(tskA, tskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB);

    final CompactThetaSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tskA, tskB, null);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB);
  }

  /*******************************************/

  @Test
  //Check all corner cases against standard ThetaUnion, ThetaIntersection, and ThetaAnotB.
  //The unordered case is not tested
  public void compareCornerCases() {
    final int k = 64;
    for (final State stateA : State.values()) {
      for (final State stateB : State.values()) {
        if ((stateA == EST_SEGMENT_UNORDERED) || (stateB == EST_SEGMENT_UNORDERED) || (stateA == NULL) || (stateB == NULL)) { continue; }
        cornerCaseChecks(stateA, stateB, k);
        cornerCaseChecksMemorySegment(stateA, stateB, k);
      }
    }
  }

//  @Test
//  public void checkExactNullSpecificCase() {
//    cornerCaseChecksMemorySegment(State.EXACT, State.NULL, 64);
//  }

  private static void cornerCaseChecksMemorySegment(final State stateA, final State stateB, final int k) {
    println("StateA: " + stateA + ", StateB: " + stateB);
    final CompactThetaSketch tcskA = generate(stateA, k);
    final CompactThetaSketch tcskB = generate(stateB, k);

    MemorySegment wseg = MemorySegment.ofArray(new byte[ThetaSetOperation.getMaxUnionBytes(k)]);

    CompactThetaSketch rcskStdU = doStdUnion(tcskA, tcskB, k, null);
    final CompactThetaSketch rcskPwU = doPwUnion(tcskA, tcskB, k);
    checkCornerCase(rcskPwU, rcskStdU); //heap, heap

    rcskStdU = doStdUnion(tcskA, tcskB, k, wseg);
    final CompactThetaSketch rcskStdPairU = doStdPairUnion(tcskA, tcskB, k, wseg);
    checkCornerCase(rcskStdPairU, rcskStdU); //direct, direct

    wseg = MemorySegment.ofArray(new byte[ThetaSetOperation.getMaxIntersectionBytes(k)]);

    CompactThetaSketch rcskStdI = doStdIntersection(tcskA, tcskB, null);
    final CompactThetaSketch rcskPwI = doPwIntersection(tcskA, tcskB);
    checkCornerCase(rcskPwI, rcskStdI); //empty, empty

    rcskStdI = doStdIntersection(tcskA, tcskB, wseg);
    final CompactThetaSketch rcskStdPairI = doStdPairIntersection(tcskA, tcskB, wseg);
    checkCornerCase(rcskStdPairI, rcskStdI); //empty, empty //direct, direct???

    wseg = MemorySegment.ofArray(new byte[ThetaSetOperation.getMaxAnotBResultBytes(k)]);

    CompactThetaSketch rcskStdAnotB = doStdAnotB(tcskA, tcskB, null);
    final CompactThetaSketch rcskPwAnotB = doPwAnotB(tcskA, tcskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB); //heap, heap

    rcskStdAnotB = doStdAnotB(tcskA, tcskB, wseg);
    final CompactThetaSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tcskA, tcskB, wseg);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB); //direct, heap
  }

  private static void cornerCaseChecks(final State stateA, final State stateB, final int k) {
    println("StateA: " + stateA + ", StateB: " + stateB);
    final CompactThetaSketch tcskA = generate(stateA, k);
    final CompactThetaSketch tcskB = generate(stateB, k);

    final CompactThetaSketch rcskStdU = doStdUnion(tcskA, tcskB, k, null);
    final CompactThetaSketch rcskPwU = doPwUnion(tcskA, tcskB, k);
    checkCornerCase(rcskPwU, rcskStdU);

    final CompactThetaSketch rcskStdPairU = doStdPairUnion(tcskA, tcskB, k, null);
    checkCornerCase(rcskStdPairU, rcskStdU);

    final CompactThetaSketch rcskStdI = doStdIntersection(tcskA, tcskB, null);
    final CompactThetaSketch rcskPwI = doPwIntersection(tcskA, tcskB);
    checkCornerCase(rcskPwI, rcskStdI);

    final CompactThetaSketch rcskStdPairI = doStdPairIntersection(tcskA, tcskB, null);
    checkCornerCase(rcskStdPairI, rcskStdI);

    final CompactThetaSketch rcskStdAnotB = doStdAnotB(tcskA, tcskB, null);
    final CompactThetaSketch rcskPwAnotB = doPwAnotB(tcskA, tcskB);
    checkCornerCase(rcskPwAnotB, rcskStdAnotB);

    final CompactThetaSketch rcskStdStatefulAnotB = doStdStatefulAnotB(tcskA, tcskB, null);
    checkCornerCase(rcskStdStatefulAnotB, rcskStdAnotB);
  }

  private static CompactThetaSketch doStdUnion(final ThetaSketch tskA, final ThetaSketch tskB, final int k, final MemorySegment wseg) {
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    union.union(tskA);
    union.union(tskB);
    return union.getResult(true, wseg);
  }

  private static CompactThetaSketch doStdPairUnion(final ThetaSketch tskA, final ThetaSketch tskB, final int k, final MemorySegment wseg) {
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    return union.union(tskA, tskB, true, wseg);
  }

  private static CompactThetaSketch doStdIntersection(final ThetaSketch tskA, final ThetaSketch tskB, final MemorySegment wseg) {
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    inter.intersect(tskA);
    inter.intersect(tskB);
    return inter.getResult(true, wseg);
  }

  private static CompactThetaSketch doStdPairIntersection(final ThetaSketch tskA, final ThetaSketch tskB, final MemorySegment wseg) {
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    return inter.intersect(tskA, tskB, true, wseg);
  }

  private static CompactThetaSketch doStdAnotB(final ThetaSketch tskA, final ThetaSketch tskB, final MemorySegment wseg) {
    final ThetaAnotB anotb = ThetaSetOperation.builder().buildANotB();
    return anotb.aNotB(tskA, tskB, true, wseg);
  }

  private static CompactThetaSketch doStdStatefulAnotB(final ThetaSketch tskA, final ThetaSketch tskB, final MemorySegment wseg) {
    final ThetaAnotB anotb = ThetaSetOperation.builder().buildANotB();
    anotb.setA(tskA);
    anotb.notB(tskB);
    anotb.getResult(false);
    return anotb.getResult(true, wseg, true);
  }

  private static CompactThetaSketch doPwUnion(final ThetaSketch tskA, final ThetaSketch tskB, final int k) {
    CompactThetaSketch tcskA, tcskB;
    if (tskA == null) { tcskA = null; }
    else { tcskA = (tskA instanceof CompactThetaSketch) ? (CompactThetaSketch) tskA : tskA.compact(); }
    if (tskB == null) { tcskB = null; }
    else { tcskB = (tskB instanceof CompactThetaSketch) ? (CompactThetaSketch) tskB : tskB.compact(); }
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    return union.union(tcskA, tcskB);
  }

  private static CompactThetaSketch doPwIntersection(final ThetaSketch tskA, final ThetaSketch tskB) {
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    return inter.intersect(tskA, tskB);
  }

  private static CompactThetaSketch doPwAnotB(final ThetaSketch tskA, final ThetaSketch tskB) {
    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    return aNotB.aNotB(tskA, tskB);
  }


  private static void checkCornerCase(final ThetaSketch rskA, final ThetaSketch rskB) {
    final double estA = rskA.getEstimate();
    final double estB = rskB.getEstimate();
    final boolean emptyA = rskA.isEmpty();
    final boolean emptyB = rskB.isEmpty();
    final long thetaLongA = rskA.getThetaLong();
    final long thetaLongB = rskB.getThetaLong();
    final int countA = rskA.getRetainedEntries(true);
    final int countB = rskB.getRetainedEntries(true);
    Assert.assertEquals(estB, estA, 0.0);
    Assert.assertEquals(emptyB, emptyA);
    Assert.assertEquals(thetaLongB, thetaLongA);
    Assert.assertEquals(countB, countA);
    Assert.assertEquals(rskA.getClass().getSimpleName(), rskB.getClass().getSimpleName());
  }

  /*******************************************/

  @Test
  public void checkUnionNotOrdered() {
    final int k = 64;
    final CompactThetaSketch skNull = generate(NULL, k);
    final CompactThetaSketch skEmpty = generate(EMPTY, k);
    final CompactThetaSketch skHeap = generate(EST_HEAP, k);
    final CompactThetaSketch skHeapUO = generate(EST_SEGMENT_UNORDERED, k);
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    union.union(skNull, skHeapUO);
    union.union(skEmpty, skHeapUO);
    union.union(skHeapUO, skNull);
    union.union(skHeapUO, skEmpty);
    union.union(skHeapUO, skHeap);
    union.union(skHeap, skHeapUO);
  }

  @Test
  public void checkSeedHash() {
    final int k = 64;
    final UpdatableThetaSketch tmp1 = UpdatableThetaSketch.builder().setNominalEntries(k).setSeed(123).build();
    tmp1.update(1);
    tmp1.update(3);
    final CompactThetaSketch skSmallSeed2A = tmp1.compact(true, null);

    final UpdatableThetaSketch tmp2 = UpdatableThetaSketch.builder().setNominalEntries(k).setSeed(123).build();
    tmp2.update(1);
    tmp2.update(2);
    final CompactThetaSketch skSmallSeed2B = tmp2.compact(true, null);

    final CompactThetaSketch skExact = generate(EXACT, k);
    final CompactThetaSketch skHeap = generate(EST_HEAP, 2 * k);

    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();
    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();

    //Intersect
    try {
      inter.intersect(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      inter.intersect(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      inter.intersect(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      inter.intersect(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
    //A NOT B
    try {
      aNotB.aNotB(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      aNotB.aNotB(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      aNotB.aNotB(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      aNotB.aNotB(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
    //ThetaUnion
    try {
      union.union(skExact, skSmallSeed2A);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      union.union(skExact, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      union.union(skSmallSeed2B, skExact);
      Assert.fail();
    } catch (final Exception e) { } //pass
    try {
      union.union(skHeap, skSmallSeed2B);
      Assert.fail();
    } catch (final Exception e) { } //pass
  }

  @Test
  public void checkPwUnionReduceToK() {
    final int k = 16;
    final CompactThetaSketch skNull = generate(NULL, k);
    final CompactThetaSketch skEmpty = generate(EMPTY, k);
    final CompactThetaSketch skHeap1 = generate(EST_HEAP, k);
    final CompactThetaSketch skHeap2 = generate(EST_HEAP, k);
    final ThetaUnion union = ThetaSetOperation.builder().setNominalEntries(k).buildUnion();
    CompactThetaSketch csk;
    csk = union.union(skNull, skHeap1);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = union.union(skEmpty, skHeap1);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = union.union(skHeap1, skNull);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = union.union(skHeap1, skEmpty);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
    csk = union.union(skHeap1, skHeap2);
    Assert.assertEquals(csk.getRetainedEntries(true), k);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

  @Test
  public void checkGenerator() {
    final int k = 16;
    CompactThetaSketch csk;

    csk = generate(State.NULL, 0);
    assertNull(csk);

    csk = generate(State.EMPTY, k);
    assertEquals(csk.isEmpty(), true);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.SINGLE, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 1);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EXACT, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), k);
    assertEquals(csk.getThetaLong(), Long.MAX_VALUE);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_HEAP, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true) > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.THLT1_CNT0_FALSE, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.THEQ1_CNT0_TRUE, k);
    assertEquals(csk.isEmpty(), true);
    assertEquals(csk.isEstimationMode(), false);
    assertEquals(csk.getRetainedEntries(true), 0);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, false);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), false);
    assertEquals(csk.isOrdered(), true);

    csk = generate(State.EST_SEGMENT_UNORDERED, k);
    assertEquals(csk.isEmpty(), false);
    assertEquals(csk.isEstimationMode(), true);
    assertEquals(csk.getRetainedEntries(true) > k, true);
    assertEquals(csk.getThetaLong() < Long.MAX_VALUE, true);
    assertEquals(csk.isOffHeap(), false);
    assertEquals(csk.hasMemorySegment(), true);
    assertEquals(csk.isOrdered(), false);
  }

  enum State {NULL, EMPTY, SINGLE, EXACT, EST_HEAP, THLT1_CNT0_FALSE, THEQ1_CNT0_TRUE, EST_SEGMENT_UNORDERED}

  private static CompactThetaSketch generate(final State state, final int k) {
    UpdatableThetaSketch sk = null;
    CompactThetaSketch csk = null;

    switch(state) {
      case NULL : {
        //already null
        break;
      }
      case EMPTY : { //results in EmptyCompactSketch
        csk = UpdatableThetaSketch.builder().setNominalEntries(k).build().compact(true, null);
        break;
      }
      case SINGLE : { //results in SingleItemSketches most of the time
        sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
        sk.update(1);
        csk = sk.compact(true, null);
        break;
      }
      case EXACT : {
        sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
        for (int i = 0; i < k; i++) {
          sk.update(i);
        }
        csk = sk.compact(true, null);
        break;
      }
      case EST_HEAP : {
        sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
        for (int i = 0; i < (4 * k); i++) {
          sk.update(i);
        }
        csk = sk.compact(true, null);
        break;
      }
      case THLT1_CNT0_FALSE : {
        sk = UpdatableThetaSketch.builder().setP((float)0.5).setNominalEntries(k).build();
        sk.update(7); //above theta
        assert(sk.getRetainedEntries(true) == 0);
        csk = sk.compact(true, null); //compact as {Th < 1.0, 0, F}
        break;
      }
      case THEQ1_CNT0_TRUE : {
        sk = UpdatableThetaSketch.builder().setP((float)0.5).setNominalEntries(k).build();
        assert(sk.getRetainedEntries(true) == 0);
        csk = sk.compact(true, null); //compact as {Th < 1.0, 0, T}
        break;
      }
      case EST_SEGMENT_UNORDERED : {
        sk = UpdatableThetaSketch.builder().setNominalEntries(k).build();
        for (int i = 0; i < (4 * k); i++) {
          sk.update(i);
        }
        final int bytes = ThetaSketch.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
        final byte[] byteArr = new byte[bytes];
        final MemorySegment wseg = MemorySegment.ofArray(byteArr);
        csk = sk.compact(false, wseg);
        break;
      }
    }
    return csk;
  }

}
