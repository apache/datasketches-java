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
import static org.apache.datasketches.theta.ConcurrentHeapQuickSelectSketchTest.waitForBgPropagationToComplete;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.ConcurrentDirectQuickSelectSketch;
import org.apache.datasketches.theta.ConcurrentHeapThetaBuffer;
import org.apache.datasketches.theta.ConcurrentSharedThetaSketch;
import org.apache.datasketches.theta.DirectQuickSelectSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.theta.ConcurrentHeapQuickSelectSketchTest.SharedLocal;
import org.apache.datasketches.thetacommon.HashOperations;
import org.testng.annotations.Test;

/**
 * @author eshcar
 */
public class ConcurrentDirectQuickSelectSketchTest {
  private static final long SEED = Util.DEFAULT_UPDATE_SEED;

  @Test
  public void checkDirectCompactConversion() {
    final int lgK = 9;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    assertTrue(sl.shared instanceof ConcurrentDirectQuickSelectSketch);
    assertTrue(sl.shared.compact().isCompact());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 2*k;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    final UpdateSketch shared = sl.shared; //off-heap
    final UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(shared.getClass().getSimpleName(), "ConcurrentDirectQuickSelectSketch");
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");

    //This sharedHeap is not linked to the concurrent local buffer
    final UpdateSketch sharedHeap = Sketches.heapifyUpdateSketch(sl.wseg);
    assertEquals(sharedHeap.getClass().getSimpleName(), "HeapQuickSelectSketch");

    checkMemorySegmentDirectProxyMethods(local, shared);
    checkOtherProxyMethods(local, shared);
    checkOtherProxyMethods(local, sharedHeap);

    final int curCount1 = shared.getRetainedEntries(true);
    final int curCount2 = sharedHeap.getRetainedEntries(true);
    assertEquals(curCount1, curCount2);
    final long[] cache = sharedHeap.getCache();
    final long thetaLong = sharedHeap.getThetaLong();
    final int cacheCount = HashOperations.count(cache, thetaLong);
    assertEquals(curCount1, cacheCount);
    assertEquals(local.getCurrentPreambleLongs(), 3);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    for (int i=0; i< k; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    final byte[]  serArr = shared.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    final Sketch recoveredShared = Sketch.heapify(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);

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
    final int lgK = 12;
    final int k = 1 << lgK;
    final int u = 2*k;

    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    final double uskEst = local.getEstimate();
    final double uskLB  = local.getLowerBound(2);
    final double uskUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), true);

    final byte[]  serArr = shared.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    final Sketch recoveredShared = Sketch.heapify(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), uskEst);
    assertEquals(local2.getLowerBound(2), uskLB);
    assertEquals(local2.getUpperBound(2), uskUB);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), true);
    assertEquals(recoveredShared.getClass().getSimpleName(), "HeapQuickSelectSketch");
  }

  @Test
  public void checkWrapMemorySegmentEst() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 2*k;
    //boolean estimating = (u > k);

    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    final double sk1est = local.getEstimate();
    final double sk1lb  = local.getLowerBound(2);
    final double sk1ub  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());

    final Sketch local2 = Sketch.wrap(sl.wseg);

    assertEquals(local2.getEstimate(), sk1est);
    assertEquals(local2.getLowerBound(2), sk1lb);
    assertEquals(local2.getUpperBound(2), sk1ub);
    assertEquals(local2.isEmpty(), false);
    assertTrue(local2.isEstimationMode());
  }

  @Test
  public void checkDQStoCompactForms() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 4*k;
    //boolean estimating = (u > k);
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isOffHeap());
    assertTrue(local.hasMemorySegment());

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    shared.rebuild(); //forces size back to k

    //get baseline values
    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
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

    final int bytes = shared.getCompactBytes();
    assertEquals(bytes, k*8 + (Family.COMPACT.getMaxPreLongs() << 3));
    final byte[] segArr2 = new byte[bytes];
    final MemorySegment seg2 = MemorySegment.ofArray(segArr2);

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
    final int lgK = 9;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    //empty
    local.toString(false, true, 0, false); //exercise toString
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    final double localEst = local.getEstimate();
    final double localLB  = local.getLowerBound(2);
    final double localUB  = local.getUpperBound(2);
    assertFalse(local.isEstimationMode());

    final int bytes = local.getCompactBytes(); //compact form
    assertEquals(bytes, 8);
    final byte[] segArr2 = new byte[bytes];
    final MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    final CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertTrue(csk2.isOrdered());
    final CompactSketch csk3 = shared.compact(true, seg2);
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
    final int lgK = 12;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    final int u = 3*k;

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);
    assertTrue(shared.getRetainedEntries(false) > k);
  }

  @Test
  public void checkErrorBounds() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    //Exact mode
    for (int i = 0; i < k; i++ ) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double est = local.getEstimate();
    double lb = local.getLowerBound(2);
    double ub = local.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    final int u = 100*k;
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
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 2*k;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    for (int i = 0; i < u; i++ ) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    final double est = local.getEstimate();
    final double ub = local.getUpperBound(1);
    final double lb = local.getLowerBound(1);
    assertTrue(ub > est);
    assertTrue(lb < est);
  }

  @Test
  public void checkRebuild() {
    final int lgK = 9;
    final int k = 1 << lgK;
    final int u = 4*k;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

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
    final int lgK = 9;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    final int u = 4*k;
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
    final int lgK = 12;
    final int k = 1 << lgK;
    final int u = k;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstModeMemorySegmentArr() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    final int u = 3*k;
    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    final double est = local.getEstimate();
    assertTrue(est < u * 1.05 && est > u * 0.95);
    assertTrue(shared.getRetainedEntries(false) >= k);
  }

  @Test
  public void checkEstModeNativeMemorySegment() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    final int u = 3*k;
    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);
    final double est = local.getEstimate();
    assertTrue(est < u * 1.05 && est > u * 0.95);
    assertTrue(shared.getRetainedEntries(false) >= k);
  }

  @Test
  public void checkConstructReconstructFromMemorySegment() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    final int u = 3*k;

    for (int i = 0; i< u; i++) { local.update(i); } //force estimation
    waitForBgPropagationToComplete(shared);

    final double est1 = local.getEstimate();
    final int count1 = shared.getRetainedEntries(false);
    assertTrue(est1 < u * 1.05 && est1 > u * 0.95);
    assertTrue(count1 >= k);

    byte[] serArr;
    double est2;

    serArr = shared.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(serArr);
    final UpdateSketch recoveredShared = Sketches.wrapUpdateSketch(seg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    final UpdateSketch local2 = sl.bldr.buildLocal(shared);
    est2 = local2.getEstimate();

    assertEquals(est2, est1, 0.0);
  }

  @Test
  public void checkNullMemorySegment() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    final UpdateSketch sk = bldr.build();
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    final UpdateSketch shared = bldr.buildSharedFromSketch(sk, null);
    assertEquals(shared.getRetainedEntries(true), 1000);
    assertFalse(shared.hasMemorySegment());
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigSeg() {
    final int lgK = 14;
    final int u = 1 << 20;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, SEED, useSeg, true, 8); //seg is 8X larger than needed
    final UpdateSketch local = sl.local;

    for (int i = 0; i < u; i++) { local.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    final int lgK = 3;
    final boolean useSeg = true;
    new SharedLocal(lgK, lgK, useSeg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorSegTooSmall() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final MemorySegment wseg = MemorySegment.ofArray(new byte[k/2]);
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.buildShared(wseg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    final int lgK = 9;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte
    //try to heapify the corrupted seg
    Sketch.heapify(sl.wseg); //catch in Sketch.constructHeapSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    final int lgK = 4;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    sl.wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(sl.wseg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkBackgroundPropagation() {
    final int lgK = 4;
    final int k = 1 << lgK;
    final int u = 10*k;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());
    final ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)local; //for internal checks

    int i = 0;
    for (; i< k; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);
    assertFalse(local.isEmpty());
    assertTrue(local.getEstimate() > 0.0);
    final long theta1 = ((ConcurrentSharedThetaSketch)shared).getVolatileTheta();

    for (; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    final long theta2 = ((ConcurrentSharedThetaSketch)shared).getVolatileTheta();
    final int entries = shared.getRetainedEntries(false);
    assertTrue(entries > k || theta2 < theta1,
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
    final int lgK = 9;
    final int k = 1 << lgK;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    final UpdateSketch local = sl.local;
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
    final int lgK = 9;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted seg
    Sketch.wrap(sl.wseg); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    final int lgK = 9;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted seg
    DirectQuickSelectSketch.writableWrap(sl.wseg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    final int lgK = 9;
    final long seed1 = 1021;
    final long seed2 = Util.DEFAULT_UPDATE_SEED;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, seed1, useSeg, true, 1);
    final UpdateSketch shared = sl.shared;

    final MemorySegment srcSeg = MemorySegment.ofArray(shared.toByteArray()).asReadOnly();
    Sketch.heapify(srcSeg, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    final int lgK = 4;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);

    sl.wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(sl.wseg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    final int lgK = 4;
    final boolean useSeg = true;
    final SharedLocal sl = new SharedLocal(lgK, lgK, useSeg);
    final UpdateSketch shared = sl.shared;
    shared.hashUpdate(1);
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

  private static void checkMemorySegmentDirectProxyMethods(final Sketch local, final Sketch shared) {
    assertEquals(
        local.hasMemorySegment(),
        shared.hasMemorySegment());
    assertEquals(local.isOffHeap(), shared.isOffHeap());
  }

  //Does not check hasMemorySegment(), isOffHeap()
  private static void checkOtherProxyMethods(final Sketch local, final Sketch shared) {
    assertEquals(local.getCompactBytes(), shared.getCompactBytes());
    assertEquals(local.getCurrentBytes(), shared.getCurrentBytes());
    assertEquals(local.getEstimate(), shared.getEstimate());
    assertEquals(local.getLowerBound(2), shared.getLowerBound(2));
    assertEquals(local.getUpperBound(2), shared.getUpperBound(2));
    assertEquals(local.isEmpty(), shared.isEmpty());
    assertEquals(local.isEstimationMode(), shared.isEstimationMode());
  }

}
