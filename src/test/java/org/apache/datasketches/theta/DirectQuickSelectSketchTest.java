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
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.HashOperations;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectQuickSelectSketchTest {

  @Test//(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final int k = 512;
    try (Arena arena = Arena.ofConfined()) {
        final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< k; i++) { usk.update(i); }

      assertFalse(usk.isEmpty());
      assertEquals(usk.getEstimate(), k, 0.0);
      assertEquals(sk1.getRetainedEntries(false), k);

      wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

      ThetaSketch.wrap(wseg);
    } catch (final Exception e) {
      if (e instanceof SketchesArgumentException) {}
      else { throw new RuntimeException(e); }
    }
  }

  @Test
  public void checkConstructorKtooSmall() {
    final int k = 8;
    try (Arena arena = Arena.ofConfined()) {
        final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
    } catch (final Exception e) {
      if (e instanceof SketchesArgumentException) {}
      else { throw new RuntimeException(e); }
    }
  }

  @Test
  public void checkConstructorSegTooSmall() {
    final int k = 16;
    try (Arena arena = Arena.ofConfined()) {
        final MemorySegment wseg = makeNativeMemorySegment(k/2, arena);
      UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
    } catch (final Exception e) {
      if (e instanceof SketchesArgumentException) {}
      else { throw new RuntimeException(e); }
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    final int k = 512;
    final int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    final MemorySegment seg = MemorySegment.ofArray(new byte[bytes]);
    UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);

    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte

    //try to heapify the corrupted seg
    ThetaSketch.heapify(seg); //catch in ThetaSketch.constructHeapSketch
  }

  @Test
  public void checkHeapifySegmentEstimating() {
    final int k = 512;
    final int u = 2*k; //thus estimating
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      for (int i=0; i<u; i++) { sk1.update(i); }

      final double sk1est = sk1.getEstimate();
      final double sk1lb  = sk1.getLowerBound(2);
      final double sk1ub  = sk1.getUpperBound(2);
      assertTrue(sk1.isEstimationMode());
      assertEquals(sk1.getClass().getSimpleName(), "DirectQuickSelectSketch");
      final int curCount1 = sk1.getRetainedEntries(true);
      assertTrue(sk1.isOffHeap());
      assertTrue(sk1.hasMemorySegment());
      assertFalse(sk1.isDirty());
      assertTrue(sk1.hasMemorySegment());
      assertEquals(sk1.getCurrentPreambleLongs(), 3);

      final UpdatableThetaSketch sk2 = UpdatableThetaSketch.heapify(wseg);
      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertTrue(sk2.isEstimationMode());
      assertEquals(sk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
      final int curCount2 = sk2.getRetainedEntries(true);
      final long[] cache = sk2.getCache();
      assertEquals(curCount1, curCount2);
      final long thetaLong = sk2.getThetaLong();
      final int cacheCount = HashOperations.count(cache, thetaLong);
      assertEquals(curCount1, cacheCount);
      assertFalse(sk2.isOffHeap());
      assertFalse(sk2.hasMemorySegment());
      assertFalse(sk2.isDirty());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    final int k = 512;
    final int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    final MemorySegment seg = MemorySegment.ofArray(new byte[maxBytes]);

    UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);

    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the FamilyID byte

    //try to wrap the corrupted seg
    ThetaSketch.wrap(seg); //catch in ThetaSketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    final int k = 512;
    final int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    final MemorySegment seg = MemorySegment.ofArray(new byte[maxBytes]);

    UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);

    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); //corrupt the FamilyID byte

    //try to wrap the corrupted seg
    DirectQuickSelectSketch.writableWrap(seg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkHeapifySeedConflict() {
    final int k = 512;
    final long seed1 = 1021;
    final long seed2 = Util.DEFAULT_UPDATE_SEED;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setSeed(seed1).setNominalEntries(k).build(wseg);
      final byte[] byteArray = usk.toByteArray();
      final MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
      ThetaSketch.heapify(srcSeg, seed2);
    } catch (final Exception e) {
      if (e instanceof SketchesArgumentException) {}
      else { throw new RuntimeException(e); }
    }
  }

  @Test
  public void checkCorruptLgNomLongs() {
    final int k = 16;

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      wseg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte)2); //corrupt
      ThetaSketch.heapify(wseg, Util.DEFAULT_UPDATE_SEED);
    } catch (final Exception e) {
      if (e instanceof SketchesArgumentException) {}
      else { throw new RuntimeException(e); }
    }
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    final int k = 512;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);

      for (int i=0; i< k; i++) { usk.update(i); }

      final int bytes = usk.getCurrentBytes();
      final byte[] byteArray = usk.toByteArray();
      assertEquals(bytes, byteArray.length);

      final MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
      final ThetaSketch usk2 = ThetaSketch.heapify(srcSeg);
      assertEquals(usk2.getEstimate(), k, 0.0);
      assertEquals(usk2.getLowerBound(2), k, 0.0);
      assertEquals(usk2.getUpperBound(2), k, 0.0);
      assertEquals(usk2.isEmpty(), false);
      assertEquals(usk2.isEstimationMode(), false);
      assertEquals(usk2.getClass().getSimpleName(), "HeapQuickSelectSketch");

      // Run toString just to make sure that we can pull out all of the relevant information.
      // That is, this is being run for its side-effect of accessing things.
      // If something is wonky, it will generate an exception and fail the test.
      usk2.toString(true, true, 8, true);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    final int k = 4096;
    final int u = 2*k;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);

      for (int i=0; i<u; i++) { usk.update(i); }

      final double uskEst = usk.getEstimate();
      final double uskLB  = usk.getLowerBound(2);
      final double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), true);
      final byte[] byteArray = usk.toByteArray();

      final MemorySegment srcSeg = MemorySegment.ofArray(byteArray);
      final ThetaSketch usk2 = ThetaSketch.heapify(srcSeg);
      assertEquals(usk2.getEstimate(), uskEst);
      assertEquals(usk2.getLowerBound(2), uskLB);
      assertEquals(usk2.getUpperBound(2), uskUB);
      assertEquals(usk2.isEmpty(), false);
      assertEquals(usk2.isEstimationMode(), true);
      assertEquals(usk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkWrapMemorySegmentEst() {
    final int k = 512;
    final int u = 2*k; //thus estimating
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);
      final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      for (int i=0; i<u; i++) { sk1.update(i); }

      final double sk1est = sk1.getEstimate();
      final double sk1lb  = sk1.getLowerBound(2);
      final double sk1ub  = sk1.getUpperBound(2);
      assertTrue(sk1.isEstimationMode());

      final ThetaSketch sk2 = ThetaSketch.wrap(wseg);

      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertTrue(sk2.isEstimationMode());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkDQStoCompactForms() {
    final int k = 512;
    final int u = 4*k; //thus estimating
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      assertTrue(usk.isOffHeap());
      assertTrue(usk.hasMemorySegment());
      assertFalse(usk.isCompact());
      assertFalse(usk.isOrdered());

      for (int i=0; i<u; i++) { usk.update(i); }

      sk1.rebuild(); //forces size back to k

      //get baseline values
      final double uskEst = usk.getEstimate();
      final double uskLB  = usk.getLowerBound(2);
      final double uskUB  = usk.getUpperBound(2);
      assertTrue(usk.isEstimationMode());

      CompactThetaSketch csk;

      csk = usk.compact(false,  null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

      csk = usk.compact(true, null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactSketch");

      final int bytes = usk.getCompactBytes();
      assertEquals(bytes, k*8 + (Family.COMPACT.getMaxPreLongs() << 3));
      final byte[] segArr2 = new byte[bytes];
      final MemorySegment seg2 = MemorySegment.ofArray(segArr2);

      csk = usk.compact(false,  seg2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");

      clear(seg2);
      csk = usk.compact(true, seg2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
      csk.toString(false, true, 0, false);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    final int k = 512;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);

      //empty
      usk.toString(false, true, 0, false); //exercise toString
      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      final double uskEst = usk.getEstimate();
      final double uskLB  = usk.getLowerBound(2);
      final double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), false);

      final int bytes = usk.getCompactBytes(); //compact form
      assertEquals(bytes, 8);
      final byte[] segArr2 = new byte[bytes];
      final MemorySegment seg2 = MemorySegment.ofArray(segArr2);

      final CompactThetaSketch csk2 = usk.compact(false,  seg2);
      assertEquals(csk2.getEstimate(), uskEst);
      assertEquals(csk2.getLowerBound(2), uskLB);
      assertEquals(csk2.getUpperBound(2), uskUB);
      assertEquals(csk2.isEmpty(), true);
      assertEquals(csk2.isEstimationMode(), false);
      assertEquals(csk2.getClass().getSimpleName(), "DirectCompactSketch");

      final CompactThetaSketch csk3 = usk.compact(true, seg2);
      csk3.toString(false, true, 0, false);
      csk3.toString();
      assertEquals(csk3.getEstimate(), uskEst);
      assertEquals(csk3.getLowerBound(2), uskLB);
      assertEquals(csk3.getUpperBound(2), uskUB);
      assertEquals(csk3.isEmpty(), true);
      assertEquals(csk3.isEstimationMode(), false);
      assertEquals(csk3.getClass().getSimpleName(), "DirectCompactSketch");
    } catch (final Exception e) {
      //if (e instanceof SketchesArgumentException) {}
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkEstMode() {
    final int k = 4096;
    final int u = 2*k;

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertTrue(sk1.getRetainedEntries(false) > k);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkSamplingMode() {
    final int k = 4096;
    final float p = (float)0.5;

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setP(p).setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      for (int i = 0; i < k; i++ ) { usk.update(i); }

      final double p2 = sk1.getP();
      final double theta = sk1.getTheta();
      assertTrue(theta <= p2);

      final double est = usk.getEstimate();
      assertEquals(k, est, k *.05);
      final double ub = usk.getUpperBound(1);
      assertTrue(ub > est);
      final double lb = usk.getLowerBound(1);
      assertTrue(lb < est);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkErrorBounds() {
    final int k = 512;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);

      //Exact mode
      for (int i = 0; i < k; i++ ) { usk.update(i); }

      double est = usk.getEstimate();
      double lb = usk.getLowerBound(2);
      double ub = usk.getUpperBound(2);
      assertEquals(est, ub, 0.0);
      assertEquals(est, lb, 0.0);

      //Est mode
      final int u = 100*k;
      for (int i = k; i < u; i++ ) {
        usk.update(i);
        usk.update(i); //test duplicate rejection
      }
      est = usk.getEstimate();
      lb = usk.getLowerBound(2);
      ub = usk.getUpperBound(2);
      assertTrue(est <= ub);
      assertTrue(est >= lb);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  //Empty Tests
  @Test
  public void checkEmptyAndP() {
    //virgin, p = 1.0
    final int k = 1024;
    float p = (float)1.0;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setP(p).setNominalEntries(k).build(wseg);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());
      usk.update(1);
      assertEquals(sk1.getRetainedEntries(true), 1);
      assertFalse(usk.isEmpty());

      //virgin, p = .001
      p = (float)0.001;
      final byte[] segArr2 = new byte[(int) wseg.byteSize()];
      final MemorySegment seg2 = MemorySegment.ofArray(segArr2);
      final UpdatableThetaSketch usk2 = UpdatableThetaSketch.builder().setP(p).setNominalEntries(k).build(seg2);
      sk1 = (DirectQuickSelectSketch)usk2;

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
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkUpperAndLowerBounds() {
    final int k = 512;
    final int u = 2*k;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);

      for (int i = 0; i < u; i++ ) { usk.update(i); }

      final double est = usk.getEstimate();
      final double ub = usk.getUpperBound(1);
      final double lb = usk.getLowerBound(1);
      assertTrue(ub > est);
      assertTrue(lb < est);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkRebuild() {
    final int k = 512;
    final int u = 4*k;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertFalse(usk.isEmpty());
      assertTrue(usk.getEstimate() > 0.0);
      assertTrue(sk1.getRetainedEntries(false) > k);

      sk1.rebuild();
      assertEquals(sk1.getRetainedEntries(false), k);
      assertEquals(sk1.getRetainedEntries(true), k);
      sk1.rebuild();
      assertEquals(sk1.getRetainedEntries(false), k);
      assertEquals(sk1.getRetainedEntries(true), k);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    final int k = 512;
    final int u = 4*k;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertFalse(usk.isEmpty());
      assertTrue(sk1.getRetainedEntries(false) > k);
      assertTrue(sk1.getThetaLong() < Long.MAX_VALUE);

      sk1.reset();
      assertTrue(usk.isEmpty());
      assertEquals(sk1.getRetainedEntries(false), 0);
      assertEquals(usk.getEstimate(), 0.0, 0.0);
      assertEquals(sk1.getThetaLong(), Long.MAX_VALUE);

      assertNotNull(sk1.getMemorySegment());
      assertFalse(sk1.isOrdered());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkExactModeMemorySegmentArr() {
    final int k = 4096;
    final int u = 4096;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertEquals(usk.getEstimate(), u, 0.0);
      assertEquals(sk1.getRetainedEntries(false), u);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkEstModeMemorySegmentArr() {
    final int k = 4096;
    final int u = 2*k;

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(sk1.getRetainedEntries(false) > k);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkEstModeNativeMemorySegment() {
    final int k = 4096;
    final int u = 2*k;
    final int segCapacity = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(segCapacity, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      final DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }
      final double est = usk.getEstimate();
      println(""+est);
      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(sk1.getRetainedEntries(false) > k);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkConstructReconstructFromMemorySegment() {
    final int k = 4096;
    final int u = 2*k;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment wseg = makeNativeMemorySegment(k, arena);

      final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); } //force estimation

      final double est1 = usk.getEstimate();
      final int count1 = usk.getRetainedEntries(false);
      assertEquals(est1, u, u*.05);
      assertTrue(count1 >= k);

      byte[] serArr;
      double est2;
      int count2;

      serArr = usk.toByteArray();

      final MemorySegment seg2 = MemorySegment.ofArray(serArr);

      //reconstruct to Native/Direct
      final UpdatableThetaSketch usk2 = UpdatableThetaSketch.wrap(seg2);

      est2 = usk2.getEstimate();
      count2 = usk2.getRetainedEntries(false);

      assertEquals(count2, count1);
      assertEquals(est2, est1, 0.0);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expectedExceptions = SketchesReadOnlyException.class)
  public void updateAfterReadOnlyWrap() {
    final UpdatableThetaSketch usk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch usk2 = (UpdatableThetaSketch) ThetaSketch.wrap(MemorySegment.ofArray(usk1.toByteArray()));
    usk2.update(0);
  }

  public void updateAfterWritableWrap() {
    final UpdatableThetaSketch usk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch usk2 = UpdatableThetaSketch.wrap(MemorySegment.ofArray(usk1.toByteArray()));
    usk2.update(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    final int k = 512;
    final UpdatableThetaSketch qs = UpdatableThetaSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    qs.hashUpdate(-1L);
  }

  @Test
  public void checkConstructorSrcSegCorruptions() {
    final int k = 1024; //lgNomLongs = 10
    final int u = k; //exact mode, lgArrLongs = 11

    final int bytes = ThetaSketch.getMaxUpdateSketchBytes(k);
    final byte[] arr1 = new byte[bytes];
    final MemorySegment seg1 = MemorySegment.ofArray(arr1);
    final ResizeFactor rf = ResizeFactor.X1; //0
    final UpdatableThetaSketch usk1 = UpdatableThetaSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(seg1);
    for (int i=0; i<u; i++) { usk1.update(i); }
    //println(PreambleUtil.toString(seg1));
    @SuppressWarnings("unused")
    UpdatableThetaSketch usk2;
    seg1.set(JAVA_BYTE, FAMILY_BYTE, (byte) 3); //corrupt Family by setting to Compact
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg1, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //Pass
    }
    seg1.set(JAVA_BYTE, FAMILY_BYTE, (byte) 2); //fix Family
    seg1.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1); //corrupt preLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg1, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //pass
    }
    seg1.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 3); //fix preLongs
    seg1.set(JAVA_BYTE, SER_VER_BYTE, (byte) 2); //corrupt serVer
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg1, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //pass
    }
    seg1.set(JAVA_BYTE, SER_VER_BYTE, (byte) 3); //fix serVer

    seg1.set(JAVA_LONG_UNALIGNED, THETA_LONG, Long.MAX_VALUE >>> 1); //corrupt theta and
    seg1.set(JAVA_BYTE, LG_ARR_LONGS_BYTE, (byte) 10); //corrupt lgArrLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg1, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //pass
    }
    seg1.set(JAVA_LONG_UNALIGNED, THETA_LONG, Long.MAX_VALUE); //fix theta and
    seg1.set(JAVA_BYTE, LG_ARR_LONGS_BYTE, (byte) 11); //fix lgArrLongs
    final byte badFlags = (byte) (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | ORDERED_FLAG_MASK);
    seg1.set(JAVA_BYTE, FLAGS_BYTE, badFlags);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg1, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //pass
    }

    final byte[] arr2 = Arrays.copyOfRange(arr1, 0, bytes-1); //corrupt length
    final MemorySegment seg2 = MemorySegment.ofArray(arr2);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(seg2, null, Util.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //pass
    }
  }

  @Test
  public void checkCorruptRFWithInsufficientArray() {
    final int k = 1024; //lgNomLongs = 10

    final int bytes = ThetaSketch.getMaxUpdateSketchBytes(k);
    final byte[] arr = new byte[bytes];
    final MemorySegment seg = MemorySegment.ofArray(arr);
    final ResizeFactor rf = ResizeFactor.X8; // 3
    final UpdatableThetaSketch usk = UpdatableThetaSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(seg);
    usk.update(0);

    insertLgResizeFactor(seg, 0); // corrupt RF: X1
    final UpdatableThetaSketch dqss = DirectQuickSelectSketch.writableWrap(seg, null, Util.DEFAULT_UPDATE_SEED);
    assertEquals(dqss.getResizeFactor(), ResizeFactor.X2); // force-promote to X2
  }

  @Test
  public void checkFamilyAndRF() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 + 24]);
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
    assertEquals(sketch.getResizeFactor(), ResizeFactor.X8);
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigSeg() {
    final int k = 1 << 14;
    final int u = 1 << 20;
    final MemorySegment seg = MemorySegment.ofArray(new byte[8*k*16 +24]);
    final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);
    for (int i=0; i<u; i++) { sketch.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*16 +24]);
    UpdatableThetaSketch.builder().setNominalEntries(k).build(seg);
    seg.set(JAVA_BYTE, LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(seg, null, Util.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkMoveAndResize() {
    final int k = 1 << 12;
    final int u = 2 * k;
    final int bytes = ThetaSketch.getMaxUpdateSketchBytes(k);
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(bytes / 2);
      final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      assertTrue(sketch.isSameResource(wseg));
      for (int i = 0; i < u; i++) { sketch.update(i); }
      assertFalse(sketch.isSameResource(wseg));
    }
    assertFalse(wseg.scope().isAlive());
  }

  @Test
  public void checkReadOnlyRebuildResize() {
    final int k = 1 << 12;
    final int u = 2 * k;
    final int bytes = ThetaSketch.getMaxUpdateSketchBytes(k);
    MemorySegment wseg;
    try (Arena arena = Arena.ofConfined()) {
      wseg = arena.allocate(bytes / 2);
      final UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().setNominalEntries(k).build(wseg);
      for (int i = 0; i < u; i++) { sketch.update(i); }
      final double est1 = sketch.getEstimate();
      final byte[] serBytes = sketch.toByteArray();
      final MemorySegment seg = MemorySegment.ofArray(serBytes).asReadOnly();
      final UpdatableThetaSketch roSketch = (UpdatableThetaSketch) ThetaSketch.wrap(seg);
      final double est2 = roSketch.getEstimate();
      assertEquals(est2, est1);
      try {
        roSketch.rebuild();
        fail();
      } catch (final SketchesReadOnlyException e) {
        //expected
      }
      try {
        roSketch.reset();
        fail();
      } catch (final SketchesReadOnlyException e) {
        //expected
      }
    }
    assertFalse(wseg.scope().isAlive());
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

  private static final int getMaxBytes(final int k) {
    return (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
  }

  private static MemorySegment makeNativeMemorySegment(final int k, final Arena arena) {
    return arena.allocate(getMaxBytes(k));
  }

}
