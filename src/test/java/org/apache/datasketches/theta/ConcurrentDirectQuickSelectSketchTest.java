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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.theta.ConcurrentHeapQuickSelectSketchTest.waitForBgPropagationToComplete;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.Family;
import org.apache.datasketches.HashOperations;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.ConcurrentHeapQuickSelectSketchTest.SharedLocal;
import org.testng.annotations.Test;

/**
 * @author eshcar
 */
@SuppressWarnings("javadoc")
public class ConcurrentDirectQuickSelectSketchTest {
  private static final long SEED = DEFAULT_UPDATE_SEED;

  @Test
  public void checkDirectCompactConversion() {
    int lgK = 9;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    assertTrue(sl.shared instanceof ConcurrentDirectQuickSelectSketch);
    assertTrue(sl.shared.compact().isCompact());
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);

    UpdateSketch shared = sl.shared; //off-heap
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(shared.getClass().getSimpleName(), "ConcurrentDirectQuickSelectSketch");
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");

    //This sharedHeap is not linked to the concurrent local buffer
    UpdateSketch sharedHeap = Sketches.heapifyUpdateSketch(sl.wmem);
    assertEquals(sharedHeap.getClass().getSimpleName(), "HeapQuickSelectSketch");

    checkMemoryDirectProxyMethods(local, shared);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i< k; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    byte[]  serArr = shared.toByteArray();
    Memory srcMem = Memory.wrap(serArr);
    Sketch recoveredShared = Sketch.heapify(srcMem);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wmem);
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

    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double uskEst = local.getEstimate();
    double uskLB  = local.getLowerBound(2);
    double uskUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), true);

    byte[]  serArr = shared.toByteArray();
    Memory srcMem = Memory.wrap(serArr);
    Sketch recoveredShared = Sketch.heapify(srcMem);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wmem);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), uskEst);
    assertEquals(local2.getLowerBound(2), uskLB);
    assertEquals(local2.getUpperBound(2), uskUB);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), true);
    assertEquals(recoveredShared.getClass().getSimpleName(), "HeapQuickSelectSketch");
  }

  @Test
  public void checkWrapMemoryEst() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean estimating = (u > k);

    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    double sk1est = local.getEstimate();
    double sk1lb  = local.getLowerBound(2);
    double sk1ub  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), estimating);

    Sketch local2 = Sketch.wrap(sl.wmem);

    assertEquals(local2.getEstimate(), sk1est);
    assertEquals(local2.getLowerBound(2), sk1lb);
    assertEquals(local2.getUpperBound(2), sk1ub);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), estimating);
  }

  @Test
  public void checkDQStoCompactForms() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    boolean estimating = (u > k);
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isDirect());
    assertTrue(local.hasMemory());

    for (int i=0; i<u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    shared.rebuild(); //forces size back to k

    //get baseline values
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), estimating);

    CompactSketch csk;

    csk = shared.compact(false,  null);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertEquals(csk.isEstimationMode(), estimating);
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

    csk = shared.compact(true, null);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertEquals(csk.isEstimationMode(), estimating);
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

    int bytes = shared.getCompactBytes();
    assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
    byte[] memArr2 = new byte[bytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);

    csk = shared.compact(false,  mem2);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertEquals(csk.isEstimationMode(), estimating);
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");

    mem2.clear();
    csk = shared.compact(true, mem2);
    assertEquals(csk.getEstimate(), localEst);
    assertEquals(csk.getLowerBound(2), localLB);
    assertEquals(csk.getUpperBound(2), localUB);
    assertFalse(csk.isEmpty());
    assertEquals(csk.isEstimationMode(), estimating);
    assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
    csk.toString(false, true, 0, false);
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    int lgK = 9;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    byte[] memArr2 = new byte[bytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);

    CompactSketch csk2 = shared.compact(false,  mem2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertTrue(csk2.isOrdered());
    CompactSketch csk3 = shared.compact(true, mem2);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
  public void checkExactModeMemoryArr() {
    int lgK = 12;
    int k = 1 << lgK;
    int u = k;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstModeMemoryArr() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
  public void checkEstModeNativeMemory() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
  public void checkConstructReconstructFromMemory() {
    int lgK = 12;
    int k = 1 << lgK;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    WritableMemory mem = WritableMemory.wrap(serArr);
    UpdateSketch recoveredShared = Sketches.wrapUpdateSketch(mem);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wmem);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);
    est2 = local2.getEstimate();

    assertEquals(est2, est1, 0.0);
  }

  @Test
  public void checkNullMemory() {
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    final UpdateSketch sk = bldr.build();
    for (int i = 0; i < 1000; i++) { sk.update(i); }
    final UpdateSketch shared = bldr.buildSharedFromSketch(sk, null);
    assertEquals(shared.getRetainedEntries(true), 1000);
    assertFalse(shared.hasMemory());
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigMem() {
    int lgK = 14;
    int u = 1 << 20;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, SEED, useMem, true, 8); //mem is 8X larger than needed
    UpdateSketch local = sl.local;

    for (int i = 0; i < u; i++) { local.update(i); }
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    int lgK = 3;
    boolean useMem = true;
    new SharedLocal(lgK, lgK, useMem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorMemTooSmall() {
    int lgK = 4;
    int k = 1 << lgK;
    WritableMemory wmem = WritableMemory.allocate(k/2);
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.buildShared(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    int lgK = 9;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    sl.wmem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte
    //try to heapify the corrupted mem
    Sketch.heapify(sl.wmem); //catch in Sketch.constructHeapSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int lgK = 4;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    sl.wmem.putByte(LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(sl.wmem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkBackgroundPropagation() {
    int lgK = 4;
    int k = 1 << lgK;
    int u = 10*k;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());

    for (int i = 0; i< k; i++) { local.update(i); }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), k, 0.0);
    assertEquals(shared.getRetainedEntries(false), k);

    sl.wmem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    Sketch.wrap(sl.wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    int lgK = 9;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);

    sl.wmem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted mem
    Sketch.wrap(sl.wmem); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    int lgK = 9;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);

    sl.wmem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte
    //try to wrap the corrupted mem
    DirectQuickSelectSketch.writableWrap(sl.wmem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int lgK = 9;
    long seed1 = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, seed1, useMem, true, 1);
    UpdateSketch shared = sl.shared;

    Memory srcMem = Memory.wrap(shared.toByteArray());
    Sketch.heapify(srcMem, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    int lgK = 4;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);

    sl.wmem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(sl.wmem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    int lgK = 4;
    boolean useMem = true;
    SharedLocal sl = new SharedLocal(lgK, lgK, useMem);
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

  private static void checkMemoryDirectProxyMethods(Sketch local, Sketch shared) {
    assertEquals(local.hasMemory(), shared.hasMemory());
    assertEquals(local.isDirect(), shared.isDirect());
  }

  //Does not check hasMemory(), isDirect()
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
