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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

/**
 * @author eshcar
 */
public class ConcurrentHeapQuickSelectSketchTest {


  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = k;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);

    final byte[]  serArr = shared.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(serArr);
    final Sketch sk = Sketch.heapify(seg, sl.seed);
    assertTrue(sk instanceof HeapQuickSelectSketch); //Intentional promotion to Parent

    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    Sketch.heapify(seg, sl.seed);
  }

  @Test
  public void checkPropagationNotOrdered() {
    final int lgK = 8;
    final int k = 1 << lgK;
    final int u = 200*k;
    final SharedLocal sl = new SharedLocal(lgK, 4, false, false);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertEquals((sl.bldr.getConCurLgNominalEntries()), 4);
    assertTrue(local.isEmpty());

    for (int i = 0; i < u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertTrue(shared.getRetainedEntries(true) <= u);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = k;
    final SharedLocal sl = new SharedLocal(lgK);

    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());
    assertTrue(shared instanceof ConcurrentHeapQuickSelectSketch);
    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    assertTrue(shared.compact().isCompact());

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
    final byte[] byteArray = shared.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corrupted seg
    Sketch.heapify(seg, sl.seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    final int lgK = 9;
    final long seed = 1021;
    final long seed2 = Util.DEFAULT_UPDATE_SEED;
    final SharedLocal sl = new SharedLocal(lgK, lgK, seed);
    final byte[] byteArray = sl.shared.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
    Sketch.heapify(srcSeg, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    final int lgK = 4;
    final SharedLocal sl = new SharedLocal(lgK);
    final byte[]  serArr = sl.shared.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(serArr);
    srcSeg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    final int lgK = 4;
    final SharedLocal sl = new SharedLocal(lgK);
    sl.shared.hashUpdate(1);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = k;
    final SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    final byte[]  serArr = shared.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    final Sketch recoveredShared = UpdateSketch.heapify(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), u, 0.0);
    assertEquals(local2.getLowerBound(2), u, 0.0);
    assertEquals(local2.getUpperBound(2), u, 0.0);
    assertFalse(local2.isEmpty());
    assertFalse(local2.isEstimationMode());
    assertEquals(local2.getResizeFactor(), local.getResizeFactor());
    local2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final int u = 2*k;

    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());
    final byte[]  serArr = shared.toByteArray();

    final MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    final UpdateSketch recoveredShared = UpdateSketch.heapify(srcSeg, sl.seed);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);
    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertFalse(local2.isEmpty());
    assertTrue(local2.isEstimationMode());
    assertEquals(local2.getResizeFactor(), local.getResizeFactor());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 2*k; //thus estimating

    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());
    assertFalse(local.isOffHeap());
    assertFalse(local.hasMemorySegment());

    final byte[]  serArr = shared.toByteArray();

    final MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    final UpdateSketch recoveredShared = UpdateSketch.heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertEquals(local2.isEmpty(), false);
    assertTrue(local2.isEstimationMode());
  }

  @Test
  public void checkHQStoCompactForms() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 4*k; //thus estimating

    final int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isOffHeap());
    assertFalse(local.hasMemorySegment());

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    shared.rebuild(); //forces size back to k

    //get baseline values
    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
    final int sharedBytes = shared.getCurrentBytes();
    final int sharedCompBytes = shared.getCompactBytes();
    assertEquals(sharedBytes, maxBytes);
    assertTrue(local.isEstimationMode());

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = shared.compact(false,  null);

    assertEquals(comp1.getEstimate(), localEst);
    assertEquals(comp1.getLowerBound(2), localLB);
    assertEquals(comp1.getUpperBound(2), localUB);
    assertEquals(comp1.isEmpty(), false);
    assertTrue(comp1.isEstimationMode());
    assertEquals(comp1.getCompactBytes(), sharedCompBytes);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactSketch");

    comp2 = shared.compact(true, null);

    assertEquals(comp2.getEstimate(), localEst);
    assertEquals(comp2.getLowerBound(2), localLB);
    assertEquals(comp2.getUpperBound(2), localUB);
    assertEquals(comp2.isEmpty(), false);
    assertTrue(comp2.isEstimationMode());
    assertEquals(comp2.getCompactBytes(), sharedCompBytes);
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactSketch");

    final byte[] segArr2 = new byte[sharedCompBytes];
    final MemorySegment seg2 = MemorySegment.ofArray(segArr2);  //allocate seg for compact form

    comp3 = shared.compact(false,  seg2);  //load the seg2

    assertEquals(comp3.getEstimate(), localEst);
    assertEquals(comp3.getLowerBound(2), localLB);
    assertEquals(comp3.getUpperBound(2), localUB);
    assertEquals(comp3.isEmpty(), false);
    assertTrue(comp3.isEstimationMode());
    assertEquals(comp3.getCompactBytes(), sharedCompBytes);
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactSketch");

    Util.clear(seg2);
    comp4 = shared.compact(true, seg2);

    assertEquals(comp4.getEstimate(), localEst);
    assertEquals(comp4.getLowerBound(2), localLB);
    assertEquals(comp4.getUpperBound(2), localUB);
    assertEquals(comp4.isEmpty(), false);
    assertTrue(comp4.isEstimationMode());
    assertEquals(comp4.getCompactBytes(), sharedCompBytes);
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactSketch");
    comp4.toString(false, true, 0, false);
  }

  @Test
  public void checkHQStoCompactEmptyForms() {
    final int lgK = 9;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    println("lgArr: "+ local.getLgArrLongs());

    //empty
    local.toString(false, true, 0, false);
    final boolean estimating = false;
    assertTrue(local instanceof ConcurrentHeapThetaBuffer);
    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
    //int currentUSBytes = local.getCurrentBytes(false);
    //assertEquals(currentUSBytes, (32*8) + 24);  // clumsy, but a function of RF and TCF
    final int compBytes = local.getCompactBytes(); //compact form
    assertEquals(compBytes, 8);
    assertEquals(local.isEstimationMode(), estimating);

    final byte[] arr2 = new byte[compBytes];
    final MemorySegment seg2 = MemorySegment.ofArray(arr2);

    final CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertTrue(csk2.isOrdered());

    final CompactSketch csk3 = shared.compact(true, seg2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), localEst);
    assertEquals(csk3.getLowerBound(2), localLB);
    assertEquals(csk3.getUpperBound(2), localUB);
    assertEquals(csk3.isEmpty(), true);
    assertEquals(csk3.isEstimationMode(), estimating);
    assertTrue(csk3.isOrdered());
  }

  @Test
  public void checkExactMode() {
    final int lgK = 12;
    final int u = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstMode() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    final int u = 3*k;
    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);
    final int retained = shared.getRetainedEntries(false);
    assertTrue(retained > k);
    // it could be exactly k, but in this case must be greater
  }

  @Test
  public void checkErrorBounds() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch local = sl.local;
    final UpdateSketch shared = sl.shared;

    //Exact mode
    //int limit = (int)ConcurrentSharedThetaSketch.computeExactLimit(lim, 0); //? ask Eshcar
    for (int i = 0; i < k; i++ ) {
      local.update(i);
    }

    double est = local.getEstimate();
    double lb = local.getLowerBound(2);
    double ub = local.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    final int u = 2 * k;
    for (int i = k; i < u; i++ ) {
      local.update(i);
      local.update(i); //test duplicate rejection
    }
    waitForBgPropagationToComplete(shared);
    est = local.getEstimate();
    lb = local.getLowerBound(2);
    ub = local.getUpperBound(2);
    assertTrue(est <= ub);
    assertTrue(est >= lb);
  }

  @Test
  public void checkRebuild() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    //must build shared first
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    final int t = ((ConcurrentHeapThetaBuffer)local).getHashTableThreshold();

    for (int i = 0; i< t; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertTrue(local.getEstimate() > 0.0);
    assertTrue(shared.getRetainedEntries(false) > k);

    shared.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
    shared.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
  }

  @Test
  public void checkBuilder() {
    final int lgK = 4;
    final SharedLocal sl = new SharedLocal(lgK);
    assertEquals(sl.bldr.getConCurLgNominalEntries(), lgK);
    assertEquals(sl.bldr.getLgNominalEntries(), lgK);
    println(sl.bldr.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderSmallNominal() {
    final int lgK = 2; //too small
    new SharedLocal(lgK);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    final int lgK = 9;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch local = sl.local;
    local.hashUpdate(-1L);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    final int u = 3*k;

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertTrue(shared.getRetainedEntries(false) >= k);
    assertTrue(local.getThetaLong() < Long.MAX_VALUE);

    shared.reset();
    local.reset();
    assertTrue(local.isEmpty());
    assertEquals(shared.getRetainedEntries(false), 0);
    assertEquals(local.getEstimate(), 0.0, 0.0);
    assertEquals(local.getThetaLong(), Long.MAX_VALUE);
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    final int lgK = 9;
    final SharedLocal sl = new SharedLocal(lgK);
    final UpdateSketch local = sl.local;
    final UpdateSketch shared = sl.shared;

    //empty
    local.toString(false, true, 0, false); //exercise toString
    assertTrue(local instanceof ConcurrentHeapThetaBuffer);
    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double uskUB  = local.getUpperBound(2);
    assertFalse(local.isEstimationMode());

    final int bytes = local.getCompactBytes();
    assertEquals(bytes, 8);
    final byte[] segArr2 = new byte[bytes];
    final MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    final CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertTrue(csk2.isOrdered());

    final CompactSketch csk3 = shared.compact(true, seg2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), localEst);
    assertEquals(csk3.getLowerBound(2), localLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
    assertTrue(csk3.isEmpty());
    assertFalse(csk3.isEstimationMode());
    assertTrue(csk2.isOrdered());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMinReqBytes() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    for (int i = 0; i < (4 * k); i++) { sl.local.update(i); }
    waitForBgPropagationToComplete(sl.shared);
    final byte[] byteArray = sl.shared.toByteArray();
    final byte[] badBytes = Arrays.copyOfRange(byteArray, 0, 24); //corrupt no. bytes
    final MemorySegment seg = MemorySegment.ofArray(badBytes).asReadOnly();
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkThetaAndLgArrLongs() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final SharedLocal sl = new SharedLocal(lgK);
    for (int i = 0; i < k; i++) { sl.local.update(i); }
    waitForBgPropagationToComplete(sl.shared);
    final byte[] badArray = sl.shared.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(badArray);
    PreambleUtil.insertLgArrLongs(seg, 4); //corrupt
    PreambleUtil.insertThetaLong(seg, Long.MAX_VALUE / 2); //corrupt
    Sketch.heapify(seg);
  }

  @Test
  public void checkFamily() {
    final SharedLocal sl = new SharedLocal();
    final UpdateSketch local = sl.local;
    assertEquals(local.getFamily(), Family.QUICKSELECT);
  }

  @Test
  public void checkBackgroundPropagation() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final int u = 5*k;
    final SharedLocal sl = new SharedLocal(lgK);
    assertTrue(sl.local.isEmpty());

    int i = 0;
    for (; i < k; i++) { sl.local.update(i); } //exact
    waitForBgPropagationToComplete(sl.shared);

    assertFalse(sl.local.isEmpty());
    assertTrue(sl.local.getEstimate() > 0.0);
    final long theta1 = sl.sharedIf.getVolatileTheta();

    for (; i < u; i++) { sl.local.update(i); } //continue, make it estimating
    waitForBgPropagationToComplete(sl.shared);

    final long theta2 = sl.sharedIf.getVolatileTheta();
    final int entries = sl.shared.getRetainedEntries(false);
    assertTrue((entries > k) || (theta2 < theta1),
        "entries= " + entries + " k= " + k + " theta1= " + theta1 + " theta2= " + theta2);

    sl.shared.rebuild();
    assertEquals(sl.shared.getRetainedEntries(false), k);
    assertEquals(sl.shared.getRetainedEntries(true), k);
    sl.local.rebuild();
    assertEquals(sl.shared.getRetainedEntries(false), k);
    assertEquals(sl.shared.getRetainedEntries(true), k);
  }

  @Test
  public void checkBuilderExceptions() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    try {
      bldr.setNominalEntries(8);
      fail();
    } catch (final SketchesArgumentException e) { }
    try {
      bldr.setConCurNominalEntries(8);
      fail();
    } catch (final SketchesArgumentException e) { }
    try {
      bldr.setConCurLogNominalEntries(3);
      fail();
    } catch (final SketchesArgumentException e) { }
    bldr.setNumPoolThreads(4);
    assertEquals(bldr.getNumPoolThreads(), 4);
    bldr.setMaxConcurrencyError(0.04);
    assertEquals(bldr.getMaxConcurrencyError(), 0.04);
    bldr.setMaxNumLocalThreads(4);
    assertEquals(bldr.getMaxNumLocalThreads(), 4);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkToByteArray() {
    final SharedLocal sl = new SharedLocal();
    sl.local.toByteArray();
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

  static class SharedLocal {
    static final long DefaultSeed = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch shared;
    final ConcurrentSharedThetaSketch sharedIf;
    final UpdateSketch local;
    final int sharedLgK;
    final int localLgK;
    final long seed;
    final MemorySegment wseg;
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();

    SharedLocal() {
      this(9, 9, DefaultSeed, false, true, 1);
    }

    SharedLocal(final int lgK) {
      this(lgK, lgK, DefaultSeed, false, true, 1);
    }

    SharedLocal(final int sharedLgK, final int localLgK) {
      this(sharedLgK, localLgK, DefaultSeed, false, true, 1);
    }

    SharedLocal(final int sharedLgK, final int localLgK, final long seed) {
      this(sharedLgK, localLgK, seed, false, true, 1);
    }

    SharedLocal(final int sharedLgK, final int localLgK, final boolean useSeg) {
      this(sharedLgK, localLgK, DefaultSeed, useSeg, true, 1);
    }

    SharedLocal(final int sharedLgK, final int localLgK, final boolean useSeg, final boolean ordered) {
      this(sharedLgK, localLgK, DefaultSeed, useSeg, ordered, 1);
    }

    SharedLocal(final int sharedLgK, final int localLgK, final long seed, final boolean useSeg, final boolean ordered, final int segMult) {
      this.sharedLgK = sharedLgK;
      this.localLgK = localLgK;
      this.seed = seed;
      if (useSeg) {
        final int bytes = (((4 << sharedLgK) * segMult) + (Family.QUICKSELECT.getMaxPreLongs())) << 3;
        wseg = MemorySegment.ofArray(new byte[bytes]);
      } else {
        wseg = null;
      }
      bldr.setLogNominalEntries(sharedLgK);
      bldr.setConCurLogNominalEntries(localLgK);
      bldr.setPropagateOrderedCompact(ordered);
      bldr.setSeed(this.seed);
      shared = bldr.buildShared(wseg);
      local = bldr.buildLocal(shared);
      sharedIf = (ConcurrentSharedThetaSketch) shared;
    }
  }

  static void waitForBgPropagationToComplete(final UpdateSketch shared) {
    try {
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    final ConcurrentSharedThetaSketch csts = (ConcurrentSharedThetaSketch)shared;
    csts.awaitBgPropagationTermination();
    ConcurrentPropagationService.resetExecutorService(Thread.currentThread().getId());
    csts.initBgPropagationService();
  }

}
