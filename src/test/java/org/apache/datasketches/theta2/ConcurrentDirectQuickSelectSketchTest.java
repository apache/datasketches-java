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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.apache.datasketches.theta2.ConcurrentHeapQuickSelectSketchTest.waitForBgPropagationToComplete;
import static org.apache.datasketches.theta2.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta2.ConcurrentHeapQuickSelectSketchTest.SharedLocal;
import org.apache.datasketches.thetacommon2.HashOperations;
import org.testng.annotations.Test;

/**
 * @author eshcar
 */
public class ConcurrentDirectQuickSelectSketchTest {
  private static final long SEED = Util.DEFAULT_UPDATE_SEED;

  @Test
  public void checkDirectCompactConversion() {
    int lgK = 9;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    assertTrue(sl.shared instanceof ConcurrentDirectQuickSelectSketch);
    assertTrue(sl.shared.compact().isCompact());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    UpdateSketch shared = sl.shared; //off-heap
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(shared.getClass().getSimpleName(), "ConcurrentDirectQuickSelectSketch");
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");

    //This sharedHeap is not linked to the concurrent local buffer
    UpdateSketch sharedHeap = Sketches.heapifyUpdateSketch(sl.wseg);
    assertEquals(sharedHeap.getClass().getSimpleName(), "HeapQuickSelectSketch");

    checkMemorySegmentDirectProxyMethods(local, shared);
    checkOtherProxyMethods(local, shared);
    checkOtherProxyMethods(local, sharedHeap);

    int curCount1 = shared.getRetainedEntries(true);
    int curCount2 = sharedHeap.getRetainedEntries(true);
    assertEquals(curCount1, curCount2);
    long[] cache = sharedHeap.getCache();
    long thetaLong = sharedHeap.getThetaLong();
    int cacheCount = HashOperations.count(cache, thetaLong);
    assertEquals(curCount1, cacheCount);
    assertEquals(local.getCurrentPreambleLongs(), 3);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int lgK = 9;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i< k; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    byte[]  serArr = shared.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    Sketch recoveredShared = Sketch.heapify(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), k, 0.0);
    assertEquals(local2.getLowerBound(2), k, 0.0);
    assertEquals(local2.getUpperBound(2), k, 0.0);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), false);
    assertEquals(recoveredShared.getClass().getSimpleName(), "HeapQuickSelectSketch");

    // Run toString just to make sure that we can pull out all of the relevant information.
    // That is, this is being run for its side-effect of accessing things.
    // If something is wonky, it will generate an exception and fail the test.
    local2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    int lgK = 12;
    int k = 1 << lgK;
    int u = 2*k;

    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double uskEst = local.getEstimate();
    double uskLB  = local.getLowerBound(2);
    double uskUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), true);

    byte[]  serArr = shared.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    Sketch recoveredShared = Sketch.heapify(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), uskEst);
    assertEquals(local2.getLowerBound(2), uskLB);
    assertEquals(local2.getUpperBound(2), uskUB);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), true);
    assertEquals(recoveredShared.getClass().getSimpleName(), "HeapQuickSelectSketch");
  }

  @Test
  public void checkWrapMemorySegmentEst() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    //boolean estimating = (u > k);

    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double sk1est = local.getEstimate();
    double sk1lb  = local.getLowerBound(2);
    double sk1ub  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());

    Sketch local2 = Sketch.wrap(sl.wseg);

    assertEquals(local2.getEstimate(), sk1est);
    assertEquals(local2.getLowerBound(2), sk1lb);
    assertEquals(local2.getUpperBound(2), sk1ub);
    assertEquals(local2.isEmpty(), false);
    assertTrue(local2.isEstimationMode());
  }

  @Test
  public void checkDQStoCompactForms() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    //boolean estimating = (u > k);
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isDirect());
    assertTrue(local.hasMemorySegment());

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    shared.rebuild(); //forces size back to k

    //get baseline values
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());

    CompactSketch csk;

    csk = shared.compact(false,  null);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertTrue(csk.isEstimationMode());
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

    csk = shared.compact(true, null);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertTrue(csk.isEstimationMode());
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

    int bytes = shared.getCompactBytes();
    assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
    byte[] segArr2 = new byte[bytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    csk = shared.compact(false,  seg2);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertTrue(csk.isEstimationMode());
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");

    Util.clear(seg2);
    csk = shared.compact(true, seg2);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertTrue(csk.isEstimationMode());
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
    csk.toString(false, true, 0, false);
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    int lgK = 9;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    //empty
    local.toString(false, true, 0, false); //exercise toString
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertFalse(local.isEstimationMode());

    int bytes = local.getCompactBytes(); //compact form
    assertEquals(bytes, 8);
    byte[] segArr2 = new byte[bytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertTrue(csk2.isOrdered());
    CompactSketch csk3 = shared.compact(true, seg2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), localEst);
    assertEquals(csk3.getLowerBound(2), localLB);
    assertEquals(csk3.getUpperBound(2), localUB);
    assertTrue(csk3.isEmpty());
    assertFalse(csk3.isEstimationMode());
    assertTrue(csk2.isOrdered());
  }

  @Test
  public void checkEstMode() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    int u = 3*k;

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);
    assertTrue(shared.getRetainedEntries(false) > k);
  }

  @Test
  public void checkErrorBounds() {
    int lgK = 9;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    //Exact mode
    for (int i = 0; i < k; i++ ) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double est = local.getEstimate();
    double lb = local.getLowerBound(2);
    double ub = local.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    int u = 100*k;
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
  public void checkUpperAndLowerBounds() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i = 0; i < u; i++ ) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double est = local.getEstimate();
    double ub = local.getUpperBound(1);
    double lb = local.getLowerBound(1);
    assertTrue(ub > est);
    assertTrue(lb < est);
  }

  @Test
  public void checkRebuild() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertTrue(local.getEstimate() > 0.0);
    assertTrue(shared.getRetainedEntries(false) >= k);

    shared.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
    local.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int lgK = 9;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    int u = 4*k;
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
  public void checkExactModeMemorySegmentArr() {
    int lgK = 12;
    int k = 1 << lgK;
    int u = k;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstModeMemorySegmentArr() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    int u = 3*k;
    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double est = local.getEstimate();
    assertTrue((est < (u * 1.05)) && (est > (u * 0.95)));
    assertTrue(shared.getRetainedEntries(false) >= k);
  }

  @Test
  public void checkEstModeNativeMemorySegment() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    int u = 3*k;
    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);
    double est = local.getEstimate();
    assertTrue((est < (u * 1.05)) && (est > (u * 0.95)));
    assertTrue(shared.getRetainedEntries(false) >= k);
  }

  @Test
  public void checkConstructReconstructFromMemorySegment() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    int u = 3*k;

    for (int i = 0; i< u; i++) { local.update(i); } //force estimation
    waitForBgPropagationToComplete(shared);

    double est1 = local.getEstimate();
    int count1 = shared.getRetainedEntries(false);
    assertTrue((est1 < (u * 1.05)) && (est1 > (u * 0.95)));
    assertTrue(count1 >= k);

    byte[] serArr;
    double est2;

    serArr = shared.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(serArr);
    UpdateSketch recoveredShared = Sketches.wrapUpdateSketch(seg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);
    est2 = local2.getEstimate();

    assertEquals(est2, est1, 0.0);
  }

  @Test
  public void checkNullMemorySegment() {
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    final UpdateSketch sk = bldr.build();
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    final UpdateSketch shared = bldr.buildSharedFromSketch(sk, null);
    assertEquals(shared.getRetainedEntries(true), 1000);
    assertFalse(shared.hasMemorySegment());
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigSeg() {
    int lgK = 14;
    int u = 1 << 20;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, SEED, useSeg, true, 8); //seg is 8X larger than needed
    UpdateSketch local = sl.local;

    for (int i = 0; i < u; i++) { local.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    int lgK = 3;
    boolean useSeg = true;
    new SharedLocal(lgK, lgK, useSeg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorSegTooSmall() {
    int lgK = 4;
    int k = 1 << lgK;
    MemorySegment wseg = MemorySegment.ofArray(new byte[k/2]);
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.buildShared(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    int lgK = 9;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte
    //try to heapify the corrupted seg
    Sketch.heapify(sl.wseg); //catch in Sketch.constructHeapSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int lgK = 4;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    sl.wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(sl.wseg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkBackgroundPropagation() {
    int lgK = 4;
    int k = 1 << lgK;
    int u = 10*k;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());
    ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)local; //for internal checks

    int i = 0;
    for (; i< k; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);
    assertFalse(local.isEmpty());
    assertTrue(local.getEstimate() > 0.0);
    long theta1 = ((ConcurrentSharedThetaSketch)shared).getVolatileTheta();

    for (; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    long theta2 = ((ConcurrentSharedThetaSketch)shared).getVolatileTheta();
    int entries = shared.getRetainedEntries(false);
    assertTrue((entries > k) || (theta2 < theta1),
        "entries="+entries+" k="+k+" theta1="+theta1+" theta2="+theta2);

    shared.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
    sk1.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int lgK = 9;
    int k = 1 << lgK;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    for (int i = 0; i< k; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), k, 0.0);
    assertEquals(shared.getRetainedEntries(false), k);

    sl.wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    Sketch.wrap(sl.wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    int lgK = 9;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted seg
    Sketch.wrap(sl.wseg); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    int lgK = 9;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted seg
    DirectQuickSelectSketch.writableWrap(sl.wseg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int lgK = 9;
    long seed1 = 1021;
    long seed2 = Util.DEFAULT_UPDATE_SEED;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, seed1, useSeg, true, 1);
    UpdateSketch shared = sl.shared;

    MemorySegment srcSeg = MemorySegment.ofArray(shared.toByteArray()).asReadOnly();
    Sketch.heapify(srcSeg, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    int lgK = 4;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(sl.wseg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    int lgK = 4;
    boolean useSeg = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    shared.hashUpdate(1);
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

  private static void checkMemorySegmentDirectProxyMethods(Sketch local, Sketch shared) {
    assertEquals(
        local.hasMemorySegment(),
        shared.hasMemorySegment());
    assertEquals(local.isDirect(), shared.isDirect());
  }

  //Does not check hasMemorySegment(), isDirect()
  private static void checkOtherProxyMethods(Sketch local, Sketch shared) {
    assertEquals(local.getCompactBytes(), shared.getCompactBytes());
    assertEquals(local.getCurrentBytes(), shared.getCurrentBytes());
    assertEquals(local.getEstimate(), shared.getEstimate());
    assertEquals(local.getLowerBound(2), shared.getLowerBound(2));
    assertEquals(local.getUpperBound(2), shared.getUpperBound(2));
    assertEquals(local.isEmpty(), shared.isEmpty());
    assertEquals(local.isEstimationMode(), shared.isEstimationMode());
  }

}
