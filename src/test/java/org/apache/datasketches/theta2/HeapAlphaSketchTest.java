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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Family.ALPHA;
import static org.apache.datasketches.common.ResizeFactor.X1;
import static org.apache.datasketches.common.ResizeFactor.X2;
import static org.apache.datasketches.common.ResizeFactor.X8;
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.theta2.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta2.PreambleUtil.insertLgResizeFactor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon2.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class HeapAlphaSketchTest {
  private Family fam_ = ALPHA;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    int u = k;
    long seed = Util.DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      sk1.update(i);
    }

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);

    byte[] byteArray = usk.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

    Sketch.heapify(seg, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    int k = 256;
    UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAlphaIncompatibleWithSeg() {
    MemorySegment seg = MemorySegment.ofArray(new byte[(512*16)+24]);
    UpdateSketch.builder().setFamily(Family.ALPHA).setNominalEntries(512).build(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    int k = 512;
    int u = k;
    long seed = Util.DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks
    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
    byte[] byteArray = usk.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corrupted seg
    Sketch.heapify(seg, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int k = 512;
    long seed1 = 1021;
    long seed2 = Util.DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed1)
        .setNominalEntries(k).build();
    byte[] byteArray = usk.toByteArray();
    MemorySegment srcSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    Sketch.heapify(srcSeg, seed2);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    int u = k;
    long seed = Util.DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes();
    byte[] byteArray = usk.toByteArray();
    assertEquals(bytes, byteArray.length);

    MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcSeg, seed);
    assertEquals(usk2.getEstimate(), u, 0.0);
    assertEquals(usk2.getLowerBound(2), u, 0.0);
    assertEquals(usk2.getUpperBound(2), u, 0.0);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), false);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
    usk2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    int k = 4096;
    int u = 2*k;
    long seed = Util.DEFAULT_UPDATE_SEED;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), true);
    byte[] byteArray = usk.toByteArray();

    MemorySegment srcSeg = MemorySegment.ofArray(byteArray).asReadOnly();
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcSeg, seed);
    assertEquals(usk2.getEstimate(), uskEst);
    assertEquals(usk2.getLowerBound(2), uskLB);
    assertEquals(usk2.getUpperBound(2), uskUB);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), true);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
  }

  @Test
  public void checkHeapifyMemorySegmentEstimating() {
    int k = 512;
    int u = 2*k; //thus estimating
    long seed = Util.DEFAULT_UPDATE_SEED;
    //int maxBytes = (k << 4) + (Family.ALPHA.getLowPreLongs());

    UpdateSketch sk1 = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      sk1.update(i);
    }

    double sk1est = sk1.getEstimate();
    double sk1lb  = sk1.getLowerBound(2);
    double sk1ub  = sk1.getUpperBound(2);
    assertTrue(sk1.isEstimationMode());

    byte[] byteArray = sk1.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArray).asReadOnly();

    UpdateSketch sk2 = (UpdateSketch)Sketch.heapify(seg, Util.DEFAULT_UPDATE_SEED);

    assertEquals(sk2.getEstimate(), sk1est);
    assertEquals(sk2.getLowerBound(2), sk1lb);
    assertEquals(sk2.getUpperBound(2), sk1ub);
    assertEquals(sk2.isEmpty(), false);
    assertTrue(sk2.isEstimationMode());
    assertEquals(sk2.getClass().getSimpleName(), sk1.getClass().getSimpleName());
  }

  @Test
  public void checkAlphaToCompactForms() {
    int k = 512;
    int u = 4*k; //thus estimating

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertEquals(usk.getClass().getSimpleName(), "HeapAlphaSketch");
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    sk1.rebuild(); //removes any dirty values

    //Alpha is more accurate, and size is a statistical variable about k
    // so cannot be directly compared to the compact forms
    assertTrue(usk.isEstimationMode());

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = usk.compact(false, null);

    //But we can compare the compact forms to each other
    double comp1est = comp1.getEstimate();
    double comp1lb  = comp1.getLowerBound(2);
    double comp1ub  = comp1.getUpperBound(2);
    int comp1bytes = comp1.getCompactBytes();
    assertEquals(comp1bytes, comp1.getCurrentBytes());
    int comp1curCount = comp1.getRetainedEntries(true);
    assertEquals(comp1bytes, (comp1curCount << 3) + (Family.COMPACT.getMaxPreLongs() << 3));

    assertEquals(comp1.isEmpty(), false);
    assertTrue(comp1.isEstimationMode());
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactSketch");

    comp2 = usk.compact(true,  null);

    assertEquals(comp2.getEstimate(), comp1est);
    assertEquals(comp2.getLowerBound(2), comp1lb);
    assertEquals(comp2.getUpperBound(2), comp1ub);
    assertEquals(comp2.isEmpty(), false);
    assertTrue(comp2.isEstimationMode());
    assertEquals(comp1bytes, comp2.getCompactBytes());
    assertEquals(comp1curCount, comp2.getRetainedEntries(true));
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactSketch");

    int bytes = usk.getCompactBytes();
    int alphaBytes = sk1.getRetainedEntries(true) * 8;
    assertEquals(bytes, alphaBytes + (Family.COMPACT.getMaxPreLongs() << 3));
    byte[] segArr2 = new byte[bytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    comp3 = usk.compact(false, seg2);

    assertEquals(comp3.getEstimate(), comp1est);
    assertEquals(comp3.getLowerBound(2), comp1lb);
    assertEquals(comp3.getUpperBound(2), comp1ub);
    assertEquals(comp3.isEmpty(), false);
    assertTrue(comp3.isEstimationMode());
    assertEquals(comp1bytes, comp3.getCompactBytes());
    assertEquals(comp1curCount, comp3.getRetainedEntries(true));
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactSketch");

    clear(seg2);
    comp4 = usk.compact(true, seg2);

    assertEquals(comp4.getEstimate(), comp1est);
    assertEquals(comp4.getLowerBound(2), comp1lb);
    assertEquals(comp4.getUpperBound(2), comp1ub);
    assertEquals(comp4.isEmpty(), false);
    assertTrue(comp4.isEstimationMode());
    assertEquals(comp1bytes, comp4.getCompactBytes());
    assertEquals(comp1curCount, comp4.getRetainedEntries(true));
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactSketch");
  }

  @Test
  public void checkAlphaToCompactEmptyForms() {
    int k = 512;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();

    //empty
    usk.toString(false, true, 0, false);
    boolean estimating = false;
    assertTrue(usk instanceof HeapAlphaSketch);
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), estimating);

    int bytes = usk.getCompactBytes();
    assertEquals(bytes, 8); //compact, empty and theta = 1.0
    byte[] segArr2 = new byte[bytes];
    MemorySegment seg2 = MemorySegment.ofArray(segArr2);

    CompactSketch csk2 = usk.compact(false,  seg2);
    assertEquals(csk2.getEstimate(), uskEst);
    assertEquals(csk2.getLowerBound(2), uskLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertTrue(csk2.isOrdered());

    CompactSketch csk3 = usk.compact(true, seg2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), uskEst);
    assertEquals(csk3.getLowerBound(2), uskLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
    assertEquals(csk3.isEmpty(), true);
    assertEquals(csk3.isEstimationMode(), estimating);
    assertTrue(csk3.isOrdered());
  }

  @Test
  public void checkExactMode() {
    int k = 4096;
    int u = 4096;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(sk1.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstMode() {
    int k = 4096;
    int u = 2*k;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(ResizeFactor.X4)
        .setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertTrue(sk1.getRetainedEntries(false) > k);
  }

  @Test
  public void checkSamplingMode() {
    int k = 4096;
    int u = k;
    float p = (float)0.5;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setP(p)
        .setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    for (int i = 0; i < u; i++ ) {
      usk.update(i);
    }

    double p2 = sk1.getP();
    double theta = sk1.getTheta();
    assertTrue(theta <= p2);

    double est = usk.getEstimate();
    double kdbl = k;
    assertEquals(kdbl, est, kdbl*.05);
    double ub = usk.getUpperBound(1);
    assertTrue(ub > est);
    double lb = usk.getLowerBound(1);
    assertTrue(lb < est);
  }

  @Test
  public void checkErrorBounds() {
    int k = 512;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X1)
        .setNominalEntries(k).build();

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
    int u = 10*k;
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
    int k = 1024;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());
    usk.update(1);
    assertEquals(sk1.getRetainedEntries(true), 1);
    assertFalse(usk.isEmpty());

    //virgin, p = .001
    UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_).setP((float)0.001)
        .setNominalEntries(k).build();
    sk1 = (HeapAlphaSketch)usk2;
    assertTrue(usk2.isEmpty());
    usk2.update(1); //will be rejected
    assertEquals(sk1.getRetainedEntries(true), 0);
    assertFalse(usk2.isEmpty());
    double est = usk2.getEstimate();
    //println("Est: "+est);
    assertEquals(est, 0.0, 0.0); //because curCount = 0
    double ub = usk2.getUpperBound(2); //huge because theta is tiny!
    //println("UB: "+ub);
    assertTrue(ub > 0.0);
    double lb = usk2.getLowerBound(2);
    assertTrue(lb <= est);
    //println("LB: "+lb);
  }

  @Test
  public void checkUpperAndLowerBounds() {
    int k = 512;
    int u = 2*k;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X2)
        .setNominalEntries(k).build();

    for (int i = 0; i < u; i++ ) {
      usk.update(i);
    }

    double est = usk.getEstimate();
    double ub = usk.getUpperBound(1);
    double lb = usk.getLowerBound(1);
    assertTrue(ub > est);
    assertTrue(lb < est);
  }

  @Test
  public void checkRebuild() {
    int k = 512;
    int u = 4*k;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }

    assertFalse(usk.isEmpty());
    assertTrue(usk.getEstimate() > 0.0);
    assertNotEquals(sk1.getRetainedEntries(false), sk1.getRetainedEntries(true));

    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), sk1.getRetainedEntries(true));
    sk1.rebuild();
    assertEquals(sk1.getRetainedEntries(false), sk1.getRetainedEntries(true));
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int k = 1024;
    int u = 4*k;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setResizeFactor(X8)
        .setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    ResizeFactor rf = sk1.getResizeFactor();
    int subMul = ThetaUtil.startingSubMultiple(11, rf.lg(), 5);
    assertEquals(sk1.getLgArrLongs(), subMul);

    UpdateSketch usk2 = UpdateSketch.builder().setFamily(fam_)
        .setResizeFactor(ResizeFactor.X1).setNominalEntries(k).build();
    sk1 = (HeapAlphaSketch)usk2;

    for (int i=0; i<u; i++) {
      usk2.update(i);
    }

    assertEquals(1 << sk1.getLgArrLongs(), 2*k);
    sk1.reset();
    rf = sk1.getResizeFactor();
    subMul = ThetaUtil.startingSubMultiple(11, rf.lg(), 5);
    assertEquals(sk1.getLgArrLongs(), subMul);

    assertNull(sk1.getMemorySegment());
    assertFalse(sk1.isOrdered());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkLBlimits0() {
    int k = 512;
    Sketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.getLowerBound(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUBlimits0() {
    int k = 512;
    Sketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.getUpperBound(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkLBlimits4() {
    int k = 512;
    Sketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.getLowerBound(4);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUBlimits4() {
    int k = 512;
    Sketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.getUpperBound(4);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreambleLongs() {
    int k = 512;
    Sketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    byte[] byteArray = alpha.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(byteArray);
    //corrupt:
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 4);
    Sketch.heapify(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    int k = 512;
    UpdateSketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.hashUpdate(-1L);
  }

  @Test
  public void checkSegDeSerExceptions() {
    int k = 1024;
    UpdateSketch sk1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    sk1.update(1L); //forces preLongs to 3
    byte[] bytearray1 = sk1.toByteArray();
    MemorySegment seg = MemorySegment.ofArray(bytearray1);
    long pre0 = seg.get(JAVA_LONG_UNALIGNED, 0);

    tryBadSeg(seg, PREAMBLE_LONGS_BYTE, 2); //Corrupt PreLongs
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, SER_VER_BYTE, 2); //Corrupt SerVer
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FAMILY_BYTE, 2); //Corrupt Family
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FLAGS_BYTE, 2); //Corrupt READ_ONLY to true
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    final long origThetaLong = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
    try {
      seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, Long.MAX_VALUE / 2); //Corrupt the theta value
      HeapAlphaSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
    seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, origThetaLong); //restore theta
    byte[] byteArray2 = new byte[bytearray1.length -1];
    MemorySegment seg2 = MemorySegment.ofArray(byteArray2);
    MemorySegment.copy(seg, 0, seg2, 0, seg2.byteSize());
    try {
      HeapAlphaSketch.heapifyInstance(seg2, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }

    // force ResizeFactor.X1, and allocated capacity too small
    insertLgResizeFactor(seg, ResizeFactor.X1.lg());
    UpdateSketch usk = HeapAlphaSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
    ResizeFactor rf = usk.getResizeFactor();
    assertEquals(rf, ResizeFactor.X2);//ResizeFactor recovered to X2, which always works.
  }

  private static void tryBadSeg(MemorySegment seg, int byteOffset, int byteValue) {
    try {
      seg.set(JAVA_BYTE, byteOffset, (byte) byteValue); //Corrupt
      HeapAlphaSketch.heapifyInstance(seg, Util.DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkEnhancedHashInsertOnFullHashTable() {
    final HeapAlphaSketch alpha = (HeapAlphaSketch) UpdateSketch.builder()
        .setFamily(ALPHA).build();
    final int n = 1 << alpha.getLgArrLongs();

    final long[] hashTable = new long[n];
    for (int i = 1; i <= n; ++i) {
      alpha.enhancedHashInsert(hashTable, i);
    }

    try {
      alpha.enhancedHashInsert(hashTable, n + 1);
      fail();
    } catch (SketchesArgumentException e) {
      // expected
    }
  }

  @Test
  public void checkFamily() {
    UpdateSketch sketch = Sketches.updateSketchBuilder().setFamily(ALPHA).build();
    assertEquals(sketch.getFamily(), Family.ALPHA);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void corruptionLgNomLongs() {
    final int k = 512;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k)
        .setFamily(ALPHA).build();
    for (int i = 0; i < k; i++) { sketch.update(i); }
    byte[] byteArr = sketch.toByteArray();
    MemorySegment wseg = MemorySegment.ofArray(byteArr);
    wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte) 8); //corrupt LgNomLongs
    UpdateSketch sk = Sketches.heapifyUpdateSketch(wseg);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }

}
