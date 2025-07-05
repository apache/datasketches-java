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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Family.QUICKSELECT;
import static org.apache.datasketches.common.ResizeFactor.X1;
import static org.apache.datasketches.common.ResizeFactor.X2;
import static org.apache.datasketches.common.ResizeFactor.X8;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.HeapQuickSelectSketch;
import org.apache.datasketches.theta.PreambleUtil;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class HeapQuickSelectSketchTest {
  private final Family fam_ = QUICKSELECT;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final int k = 512;
    final int u = k;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      sk1.update(i);
    }

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);

    final byte[] byteArray = usk.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

    Sketch.heapify(seg, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    final int k = 512;
    final int u = k;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks
    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
    final byte[] byteArray = usk.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corrupted seg
    Sketch.heapify(seg, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    final int k = 512;
    final long seed1 = 1021;
    final long seed2 = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed1).setNominalEntries(k).build();
    final byte[] byteArray = usk.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    Sketch.heapify(srcSeg, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    final UpdateSketch usk = UpdateSketch.builder().setNominalEntries(16).build();
    final MemorySegment srcSeg = MemorySegment.ofArray(usk.toByteArray());
    srcSeg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    final int k = 512;
    final int u = k;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    final int bytes = usk.getCurrentBytes();
    final byte[] byteArray = usk.toByteArray();
    assertEquals(bytes, byteArray.length);

    final MemorySegment srcSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    final UpdateSketch usk2 = Sketches.heapifyUpdateSketch(srcSeg, seed);
    assertEquals(usk2.getEstimate(), u, 0.0);
    assertEquals(usk2.getLowerBound(2), u, 0.0);
    assertEquals(usk2.getUpperBound(2), u, 0.0);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), false);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
    assertEquals(usk2.getResizeFactor(), usk.getResizeFactor());
    usk2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    final int k = 4096;
    final int u = 2*k;
    final long seed = Util.DEFAULT_UPDATE_SEED;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed).setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    final double uskEst = usk.getEstimate();
    final double uskLB  = usk.getLowerBound(2);
    final double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), true);
    final byte[] byteArray = usk.toByteArray();

    final MemorySegment srcSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    final UpdateSketch usk2 = UpdateSketch.heapify(srcSeg, seed);
    assertEquals(usk2.getEstimate(), uskEst);
    assertEquals(usk2.getLowerBound(2), uskLB);
    assertEquals(usk2.getUpperBound(2), uskUB);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), true);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
    assertEquals(usk2.getResizeFactor(), usk.getResizeFactor());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    final int k = 512;
    final int u = 2*k; //thus estimating
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final UpdateSketch sk1 = UpdateSketch.builder().setFamily(fam_).setSeed(seed).setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      sk1.update(i);
    }

    final double sk1est = sk1.getEstimate();
    final double sk1lb  = sk1.getLowerBound(2);
    final double sk1ub  = sk1.getUpperBound(2);
    assertTrue(sk1.isEstimationMode());

    final byte[] byteArray = sk1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArray).asReadOnly();

    final UpdateSketch sk2 = UpdateSketch.heapify(seg, Util.DEFAULT_UPDATE_SEED);

    assertEquals(sk2.getEstimate(), sk1est);
    assertEquals(sk2.getLowerBound(2), sk1lb);
    assertEquals(sk2.getUpperBound(2), sk1ub);
    assertEquals(sk2.isEmpty(), false);
    assertTrue(sk2.isEstimationMode());
    assertEquals(sk2.getClass().getSimpleName(), sk1.getClass().getSimpleName());
  }

  @Test
  public void checkHQStoCompactForms() {
    final int k = 512;
    final int u = 4*k; //thus estimating

    //boolean compact = false;
    final int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertEquals(usk.getClass().getSimpleName(), "HeapQuickSelectSketch");
    assertFalse(usk.isOffHeap());
    assertFalse(usk.hasMemorySegment());
    assertFalse(usk.isCompact());
    assertFalse(usk.isOrdered());

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    sk1.rebuild(); //forces size back to k

    //get baseline values
    final double uskEst = usk.getEstimate();
    final double uskLB  = usk.getLowerBound(2);
    final double uskUB  = usk.getUpperBound(2);
    final int uskBytes = usk.getCurrentBytes();    //size stored as UpdateSketch
    final int uskCompBytes = usk.getCompactBytes(); //size stored as CompactSketch
    assertEquals(uskBytes, maxBytes);
    assertTrue(usk.isEstimationMode());

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = usk.compact(false,  null);

    assertEquals(comp1.getEstimate(), uskEst);
    assertEquals(comp1.getLowerBound(2), uskLB);
    assertEquals(comp1.getUpperBound(2), uskUB);
    assertEquals(comp1.isEmpty(), false);
    assertTrue(comp1.isEstimationMode());
    assertEquals(comp1.getCompactBytes(), uskCompBytes);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactSketch");

    comp2 = usk.compact(true, null);

    assertEquals(comp2.getEstimate(), uskEst);
    assertEquals(comp2.getLowerBound(2), uskLB);
    assertEquals(comp2.getUpperBound(2), uskUB);
    assertEquals(comp2.isEmpty(), false);
    assertTrue(comp2.isEstimationMode());
    assertEquals(comp2.getCompactBytes(), uskCompBytes);
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactSketch");

    final byte[] segArr = new byte[uskCompBytes];
    final MemorySegment seg = MemorySegment.ofArray(segArr);  //allocate seg for compact form

    comp3 = usk.compact(false,  seg);  //load the seg2

    assertEquals(comp3.getEstimate(), uskEst);
    assertEquals(comp3.getLowerBound(2), uskLB);
    assertEquals(comp3.getUpperBound(2), uskUB);
    assertEquals(comp3.isEmpty(), false);
    assertTrue(comp3.isEstimationMode());
    assertEquals(comp3.getCompactBytes(), uskCompBytes);
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactSketch");

    Util.clear(seg);
    comp4 = usk.compact(true, seg);

    assertEquals(comp4.getEstimate(), uskEst);
    assertEquals(comp4.getLowerBound(2), uskLB);
    assertEquals(comp4.getUpperBound(2), uskUB);
    assertEquals(comp4.isEmpty(), false);
    assertTrue(comp4.isEstimationMode());
    assertEquals(comp4.getCompactBytes(), uskCompBytes);
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactSketch");
    comp4.toString(false, true, 0, false);
  }

  @Test
  public void checkHQStoCompactEmptyForms() {
    final int k = 512;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X2).setNominalEntries(k).build();
    println("lgArr: "+ usk.getLgArrLongs());

    //empty
    println(usk.toString(false, true, 0, false));
    final boolean estimating = false;
    assertEquals(usk.getClass().getSimpleName(), "HeapQuickSelectSketch");
    final double uskEst = usk.getEstimate();
    final double uskLB  = usk.getLowerBound(2);
    final double uskUB  = usk.getUpperBound(2);
    final int currentUSBytes = usk.getCurrentBytes();
    assertEquals(currentUSBytes, (32*8) + 24);  // clumsy, but a function of RF and TCF
    final int compBytes = usk.getCompactBytes(); //compact form
    assertEquals(compBytes, 8);
    assertEquals(usk.isEstimationMode(), estimating);

    final byte[] arr2 = new byte[compBytes];
    final MemorySegment seg = MemorySegment.ofArray(arr2);

    final CompactSketch csk2 = usk.compact(false,  seg);
    assertEquals(csk2.getEstimate(), uskEst);
    assertEquals(csk2.getLowerBound(2), uskLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactSketch");

    final CompactSketch csk3 = usk.compact(true, seg);
    println(csk3.toString(false, true, 0, false));
    println(csk3.toString());
    assertEquals(csk3.getEstimate(), uskEst);
    assertEquals(csk3.getLowerBound(2), uskLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
    assertEquals(csk3.isEmpty(), true);
    assertEquals(csk3.isEstimationMode(), estimating);
    assertEquals(csk3.getClass().getSimpleName(), "DirectCompactSketch");
  }

  @Test
  public void checkExactMode() {
    final int k = 4096;
    final int u = 4096;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstMode() {
    final int k = 4096;
    final int u = 2*k;
    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(ResizeFactor.X4).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertTrue(sk1.getRetainedEntries(false) > k); // in general it might be exactly k, but in this case must be greater
  }

  @Test
  public void checkSamplingMode() {
    final int k = 4096;
    final int u = k;
    final float p = (float)0.5;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setP(p).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    for (int i = 0; i < u; i++ ) {
      usk.update(i);
    }

    final double p2 = sk1.getP();
    final double theta = sk1.getTheta();
    assertTrue(theta <= p2);

    final double est = usk.getEstimate();
    final double kdbl = k;
    assertEquals(kdbl, est, kdbl*.05);
    final double ub = usk.getUpperBound(1);
    assertTrue(ub > est);
    final double lb = usk.getLowerBound(1);
    assertTrue(lb < est);
  }

  @Test
  public void checkErrorBounds() {
    final int k = 512;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X1).setNominalEntries(k).build();

    //Exact mode
    for (int i = 0; i < k; i++ ) {
      usk.update(i);
    }

    double est = usk.getEstimate();
    double lb = usk.getLowerBound(2);
    double ub = usk.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    final int u = 10*k;
    for (int i = k; i < u; i++ ) {
      usk.update(i);
      usk.update(i); //test duplicate rejection
    }
    est = usk.getEstimate();
    lb = usk.getLowerBound(2);
    ub = usk.getUpperBound(2);
    assertTrue(est <= ub);
    assertTrue(est >= lb);
  }

  //Empty Tests
  @Test
  public void checkEmptyAndP() {
    //virgin, p = 1.0
    final int k = 1024;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());
    usk.update(1);
    assertEquals(sk1.getRetainedEntries(true), 1);
    assertFalse(usk.isEmpty());

    //virgin, p = .001
    final UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_).setP((float)0.001).setNominalEntries(k).build();
    sk1 = (HeapQuickSelectSketch)usk2;
    assertTrue(usk2.isEmpty());
    usk2.update(1); //will be rejected
    assertEquals(sk1.getRetainedEntries(true), 0);
    assertFalse(usk2.isEmpty());
    final double est = usk2.getEstimate();
    //println("Est: "+est);
    assertEquals(est, 0.0, 0.0); //because curCount = 0
    final double ub = usk2.getUpperBound(2); //huge because theta is tiny!
    //println("UB: "+ub);
    assertTrue(ub > 0.0);
    final double lb = usk2.getLowerBound(2);
    assertTrue(lb <= est);
    //println("LB: "+lb);
  }

  @Test
  public void checkUpperAndLowerBounds() {
    final int k = 512;
    final int u = 2*k;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X2).setNominalEntries(k).build();

    for (int i = 0; i < u; i++ ) {
      usk.update(i);
    }

    final double est = usk.getEstimate();
    final double ub = usk.getUpperBound(1);
    final double lb = usk.getLowerBound(1);
    assertTrue(ub > est);
    assertTrue(lb < est);
  }

  @Test
  public void checkRebuild() {
    final int k = 16;
    final int u = 4*k;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    final HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertFalse(usk.isEmpty());
    assertTrue(usk.getEstimate() > 0.0);
    assertTrue(sk1.getRetainedEntries(false) > k);

    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), k);
    assertEquals(sk1.getRetainedEntries(true), k);
    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), k);
    assertEquals(sk1.getRetainedEntries(true), k);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    final int k = 1024;
    final int u = 4*k;

    final UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X8).setNominalEntries(k).build();
    HeapQuickSelectSketch sk1 = (HeapQuickSelectSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    ResizeFactor rf = sk1.getResizeFactor();
    int subMul = ThetaUtil.startingSubMultiple(11, rf.lg(), 5); //messy
    assertEquals(sk1.getLgArrLongs(), subMul);

    final UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_).setResizeFactor(ResizeFactor.X1).setNominalEntries(k).build();
    sk1 = (HeapQuickSelectSketch)usk2;

    for (int i=0; i<u; i++) {
      usk2.update(i);
    }

    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    rf = sk1.getResizeFactor();
    subMul = ThetaUtil.startingSubMultiple(11, rf.lg(), 5); //messy
    assertEquals(sk1.getLgArrLongs(), subMul);

    assertNull(sk1.getMemorySegment());
    assertFalse(sk1.isOrdered());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    final int k = 512;
    final UpdateSketch qs = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    qs.hashUpdate(-1L);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMinReqBytes() {
    final int k = 16;
    final UpdateSketch s1 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < (4 * k); i++) { s1.update(i); }
    final byte[] byteArray = s1.toByteArray();
    final byte[] badBytes = Arrays.copyOfRange(byteArray, 0, 24);
    final MemorySegment seg = MemorySegment.ofArray(badBytes);
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkThetaAndLgArrLongs() {
    final int k = 16;
    final UpdateSketch s1 = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < k; i++) { s1.update(i); }
    final byte[] badArray = s1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(badArray);
    PreambleUtil.insertLgArrLongs(seg, 4);
    PreambleUtil.insertThetaLong(seg, Long.MAX_VALUE / 2);
    Sketch.heapify(seg);
  }

  @Test
  public void checkFamily() {
    final UpdateSketch sketch = Sketches.updateSketchBuilder().build();
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
  }

  @Test
  public void checkSegSerDeExceptions() {
    final int k = 1024;
    final UpdateSketch sk1 = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    sk1.update(1L); //forces preLongs to 3
    final byte[] bytearray1 = sk1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(bytearray1);
    final long pre0 = seg.get(JAVA_LONG_UNALIGNED, 0);

    tryBadSeg(seg, PREAMBLE_LONGS_BYTE, 2); //Corrupt PreLongs
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, SER_VER_BYTE, 2); //Corrupt SerVer
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FAMILY_BYTE, 1); //Corrupt Family
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FLAGS_BYTE, 2); //Corrupt READ_ONLY to true
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FAMILY_BYTE, 4); //Corrupt, Family to Union
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    final long origThetaLong = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
    try {
      seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, Long.MAX_VALUE / 2); //Corrupt the theta value
      HeapQuickSelectSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
    seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, origThetaLong); //restore theta
    final byte[] byteArray2 = new byte[bytearray1.length -1];
    final MemorySegment seg2 = MemorySegment.ofArray(byteArray2);
    MemorySegment.copy(seg, 0, seg2, 0, seg2.byteSize());
    try {
      HeapQuickSelectSketch.heapifyInstance(seg2, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }

    // force ResizeFactor.X1, but allocated capacity too small
    insertLgResizeFactor(seg, ResizeFactor.X1.lg());
    final UpdateSketch hqss = HeapQuickSelectSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
    assertEquals(hqss.getResizeFactor(), ResizeFactor.X2); // force-promote to X2
  }

  private static void tryBadSeg(final MemorySegment seg, final int byteOffset, final int byteValue) {
    try {
      seg.set(JAVA_BYTE, byteOffset, (byte) byteValue); //Corrupt
      HeapQuickSelectSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
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

}
