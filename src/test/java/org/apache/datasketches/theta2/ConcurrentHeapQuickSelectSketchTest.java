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
import static org.apache.datasketches.theta2.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
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
    int lgK = 9;
    int k = 1 << lgK;
    int u = k;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);

    byte[]  serArr = shared.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(serArr);
    Sketch sk = Sketch.heapify(seg, sl.seed);
    assertTrue(sk instanceof HeapQuickSelectSketch); //Intentional promotion to Parent

    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    Sketch.heapify(seg, sl.seed);
  }

  @Test
  public void checkPropagationNotOrdered() {
    int lgK = 8;
    int k = 1 << lgK;
    int u = 200*k;
    SharedLocal sl = new SharedLocal(lgK, 4, false, false);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertEquals((sl.bldr.getLocalLgNominalEntries()), 4);
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
    int lgK = 9;
    int k = 1 << lgK;
    int u = k;
    SharedLocal sl = new SharedLocal(lgK);

    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    assertTrue(local.isEmpty());
    assertTrue(shared instanceof ConcurrentHeapQuickSelectSketch);
    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    assertTrue(shared.compact().isCompact());

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
    byte[] byteArray = shared.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corrupted seg
    Sketch.heapify(seg, sl.seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int lgK = 9;
    long seed = 1021;
    long seed2 = Util.DEFAULT_UPDATE_SEED;
    SharedLocal sl = new SharedLocal(lgK, lgK, seed);
    byte[] byteArray = sl.shared.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
    Sketch.heapify(srcSeg, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    int lgK = 4;
    SharedLocal sl = new SharedLocal(lgK);
    byte[]  serArr = sl.shared.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(serArr);
    srcSeg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    int lgK = 4;
    SharedLocal sl = new SharedLocal(lgK);
    sl.shared.hashUpdate(1);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = k;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    byte[]  serArr = shared.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    Sketch recoveredShared = Sketches.heapifyUpdateSketch(srcSeg);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);

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
    int lgK = 12;
    int k = 1 << lgK;
    int u = 2*k;

    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());
    byte[]  serArr = shared.toByteArray();

    MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    UpdateSketch recoveredShared = UpdateSketch.heapify(srcSeg, sl.seed);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);
    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertFalse(local2.isEmpty());
    assertTrue(local2.isEstimationMode());
    assertEquals(local2.getResizeFactor(), local.getResizeFactor());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 2*k; //thus estimating

    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertTrue(local.isEstimationMode());
    assertFalse(local.isDirect());
    assertFalse(local.hasMemorySegment());

    byte[]  serArr = shared.toByteArray();

    MemorySegment srcSeg = MemorySegment.ofArray(serArr).asReadOnly();
    UpdateSketch recoveredShared = UpdateSketch.heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    shared = sl.bldr.buildSharedFromSketch(recoveredShared, wseg);
    UpdateSketch local2 = sl.bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertEquals(local2.isEmpty(), false);
    assertTrue(local2.isEstimationMode());
  }

  @Test
  public void checkHQStoCompactForms() {
    int lgK = 9;
    int k = 1 << lgK;
    int u = 4*k; //thus estimating

    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isDirect());
    assertFalse(local.hasMemorySegment());

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete(shared);

    shared.rebuild(); //forces size back to k

    //get baseline values
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    int sharedBytes = shared.getCurrentBytes();
    int sharedCompBytes = shared.getCompactBytes();
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

    byte[] segArr2 = new byte[sharedCompBytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);  //allocate seg for compact form

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
    int lgK = 9;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;
    println("lgArr: "+ local.getLgArrLongs());

    //empty
    local.toString(false, true, 0, false);
    boolean estimating = false;
    assertTrue(local instanceof ConcurrentHeapThetaBuffer);
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    //int currentUSBytes = local.getCurrentBytes(false);
    //assertEquals(currentUSBytes, (32*8) + 24);  // clumsy, but a function of RF and TCF
    int compBytes = local.getCompactBytes(); //compact form
    assertEquals(compBytes, 8);
    assertEquals(local.isEstimationMode(), estimating);

    byte[] arr2 = new byte[compBytes];
    MemorySegment seg2 = MemorySegment.ofArray(arr2);

    CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertTrue(csk2.isOrdered());

    CompactSketch csk3 = shared.compact(true, seg2);
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
    int lgK = 12;
    int u = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

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
    int lgK = 12;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());

    int u = 3*k;
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
    int lgK = 9;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

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
    int u = 2 * k;
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
    int lgK = 4;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    //must build shared first
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    int t = ((ConcurrentHeapThetaBuffer)local).getHashTableThreshold();

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
    int lgK = 4;
    SharedLocal sl = new SharedLocal(lgK);
    assertEquals(sl.bldr.getLocalLgNominalEntries(), lgK);
    assertEquals(sl.bldr.getLgNominalEntries(), lgK);
    println(sl.bldr.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderSmallNominal() {
    int lgK = 2; //too small
    new SharedLocal(lgK);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    int lgK = 9;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch local = sl.local;
    local.hashUpdate(-1L);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int lgK = 9;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch shared = sl.shared;
    UpdateSketch local = sl.local;

    assertTrue(local.isEmpty());
    int u = 3*k;

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
    int lgK = 9;
    SharedLocal sl = new SharedLocal(lgK);
    UpdateSketch local = sl.local;
    UpdateSketch shared = sl.shared;

    //empty
    local.toString(false, true, 0, false); //exercise toString
    assertTrue(local instanceof ConcurrentHeapThetaBuffer);
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double uskUB  = local.getUpperBound(2);
    assertFalse(local.isEstimationMode());

    int bytes = local.getCompactBytes();
    assertEquals(bytes, 8);
    byte[] segArr2 = new byte[bytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    CompactSketch csk2 = shared.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertTrue(csk2.isOrdered());

    CompactSketch csk3 = shared.compact(true, seg2);
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
    int lgK = 4;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    for (int i = 0; i < (4 * k); i++) { sl.local.update(i); }
    waitForBgPropagationToComplete(sl.shared);
    byte[] byteArray = sl.shared.toByteArray();
    byte[] badBytes = Arrays.copyOfRange(byteArray, 0, 24); //corrupt no. bytes
    MemorySegment seg = MemorySegment.ofArray(badBytes).asReadOnly();
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkThetaAndLgArrLongs() {
    int lgK = 4;
    int k = 1 << lgK;
    SharedLocal sl = new SharedLocal(lgK);
    for (int i = 0; i < k; i++) { sl.local.update(i); }
    waitForBgPropagationToComplete(sl.shared);
    byte[] badArray = sl.shared.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(badArray);
    PreambleUtil.insertLgArrLongs(seg, 4); //corrupt
    PreambleUtil.insertThetaLong(seg, Long.MAX_VALUE / 2); //corrupt
    Sketch.heapify(seg);
  }

  @Test
  public void checkFamily() {
    SharedLocal sl = new SharedLocal();
    UpdateSketch local = sl.local;
    assertEquals(local.getFamily(), Family.QUICKSELECT);
  }

  @Test
  public void checkBackgroundPropagation() {
    int lgK = 4;
    int k = 1 << lgK;
    int u = 5*k;
    SharedLocal sl = new SharedLocal(lgK);
    assertTrue(sl.local.isEmpty());

    int i = 0;
    for (; i < k; i++) { sl.local.update(i); } //exact
    waitForBgPropagationToComplete(sl.shared);

    assertFalse(sl.local.isEmpty());
    assertTrue(sl.local.getEstimate() > 0.0);
    long theta1 = sl.sharedIf.getVolatileTheta();

    for (; i < u; i++) { sl.local.update(i); } //continue, make it estimating
    waitForBgPropagationToComplete(sl.shared);

    long theta2 = sl.sharedIf.getVolatileTheta();
    int entries = sl.shared.getRetainedEntries(false);
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
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    try {
      bldr.setNominalEntries(8);
      fail();
    } catch (SketchesArgumentException e) { }
    try {
      bldr.setLocalNominalEntries(8);
      fail();
    } catch (SketchesArgumentException e) { }
    try {
      bldr.setLocalLogNominalEntries(3);
      fail();
    } catch (SketchesArgumentException e) { }
    bldr.setNumPoolThreads(4);
    assertEquals(bldr.getNumPoolThreads(), 4);
    bldr.setMaxConcurrencyError(0.04);
    assertEquals(bldr.getMaxConcurrencyError(), 0.04);
    bldr.setMaxNumLocalThreads(4);
    assertEquals(bldr.getMaxNumLocalThreads(), 4);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkToByteArray() {
    SharedLocal sl = new SharedLocal();
    sl.local.toByteArray();
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

    SharedLocal(int lgK) {
      this(lgK, lgK, DefaultSeed, false, true, 1);
    }

    SharedLocal(int sharedLgK, int localLgK) {
      this(sharedLgK, localLgK, DefaultSeed, false, true, 1);
    }

    SharedLocal(int sharedLgK, int localLgK, long seed) {
      this(sharedLgK, localLgK, seed, false, true, 1);
    }

    SharedLocal(int sharedLgK, int localLgK, boolean useSeg) {
      this(sharedLgK, localLgK, DefaultSeed, useSeg, true, 1);
    }

    SharedLocal(int sharedLgK, int localLgK, boolean useSeg, boolean ordered) {
      this(sharedLgK, localLgK, DefaultSeed, useSeg, ordered, 1);
    }

    SharedLocal(int sharedLgK, int localLgK, long seed, boolean useSeg, boolean ordered, int segMult) {
      this.sharedLgK = sharedLgK;
      this.localLgK = localLgK;
      this.seed = seed;
      if (useSeg) {
        int bytes = (((4 << sharedLgK) * segMult) + (Family.QUICKSELECT.getMaxPreLongs())) << 3;
        wseg = MemorySegment.ofArray(new byte[bytes]);
      } else {
        wseg = null;
      }
      bldr.setLogNominalEntries(sharedLgK);
      bldr.setLocalLogNominalEntries(localLgK);
      bldr.setPropagateOrderedCompact(ordered);
      bldr.setSeed(this.seed);
      shared = bldr.buildShared(wseg);
      local = bldr.buildLocal(shared);
      sharedIf = (ConcurrentSharedThetaSketch) shared;
    }
  }

  static void waitForBgPropagationToComplete(UpdateSketch shared) {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ConcurrentSharedThetaSketch csts = (ConcurrentSharedThetaSketch)shared;
    csts.awaitBgPropagationTermination();
    ConcurrentPropagationService.resetExecutorService(Thread.currentThread().getId());
    csts.initBgPropagationService();
  }

}
