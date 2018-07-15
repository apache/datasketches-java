/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.QUICKSELECT;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesReadOnlyException;

/**
 * @author Lee Rhodes
 */
public class DirectQuickSelectSketchTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< k; i++) { usk.update(i); }

      assertFalse(usk.isEmpty());
      assertEquals(usk.getEstimate(), k, 0.0);
      assertEquals(sk1.getRetainedEntries(false), k);

      mem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

      Sketch.wrap(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    int k = 8;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch.builder().setNominalEntries(k).build(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorMemTooSmall() {
    int k = 16;
    try (WritableDirectHandle h = makeNativeMemory(k/2)) {
      WritableMemory mem = h.get();
      UpdateSketch.builder().setNominalEntries(k).build(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    int k = 512;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[bytes]);
    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte

    //try to heapify the corrupted mem
    Sketch.heapify(mem); //catch in Sketch.constructHeapSketch
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    int u = 2*k;
    boolean estimating = (u > k);

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
      for (int i=0; i<u; i++) { sk1.update(i); }

      double sk1est = sk1.getEstimate();
      double sk1lb  = sk1.getLowerBound(2);
      double sk1ub  = sk1.getUpperBound(2);
      assertEquals(sk1.isEstimationMode(), estimating);
      assertEquals(sk1.getClass().getSimpleName(), "DirectQuickSelectSketch");
      int curCount1 = sk1.getRetainedEntries(true);
      assertTrue(sk1.isDirect());
      assertFalse(sk1.isDirty());
      assertTrue(sk1.hasMemory());
      assertEquals(sk1.getCurrentPreambleLongs(false), 3);

      UpdateSketch sk2 = Sketches.heapifyUpdateSketch(mem);
      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertEquals(sk2.isEstimationMode(), estimating);
      assertEquals(sk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
      int curCount2 = sk2.getRetainedEntries(true);
      long[] cache = sk2.getCache();
      assertEquals(curCount1, curCount2);
      long thetaLong = sk2.getThetaLong();
      int cacheCount = HashOperations.count(cache, thetaLong);
      assertEquals(curCount1, cacheCount);
      assertFalse(sk2.isDirect());
      assertFalse(sk2.isDirty());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    int k = 512;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[maxBytes]);

    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    Sketch.wrap(mem); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    int k = 512;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[maxBytes]);

    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int k = 512;
    long seed1 = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setSeed(seed1).setNominalEntries(k).build(mem);
      byte[] byteArray = usk.toByteArray();
      Memory srcMem = Memory.wrap(byteArray);
      Sketch.heapify(srcMem, seed2);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    int k = 16;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch.builder().setNominalEntries(k).build(mem);
      mem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
      Sketch.heapify(mem, DEFAULT_UPDATE_SEED);
    }
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      for (int i=0; i< k; i++) { usk.update(i); }

      int bytes = usk.getCurrentBytes(false);
      byte[] byteArray = usk.toByteArray();
      assertEquals(bytes, byteArray.length);

      Memory srcMem = Memory.wrap(byteArray);
      Sketch usk2 = Sketch.heapify(srcMem);
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
    }
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    int k = 4096;
    int u = 2*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      for (int i=0; i<u; i++) { usk.update(i); }

      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), true);
      byte[] byteArray = usk.toByteArray();

      Memory srcMem = Memory.wrap(byteArray);
      Sketch usk2 = Sketch.heapify(srcMem);
      assertEquals(usk2.getEstimate(), uskEst);
      assertEquals(usk2.getLowerBound(2), uskLB);
      assertEquals(usk2.getUpperBound(2), uskUB);
      assertEquals(usk2.isEmpty(), false);
      assertEquals(usk2.isEstimationMode(), true);
      assertEquals(usk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
    }
  }

  @Test
  public void checkWrapMemoryEst() {
    int k = 512;
    int u = 2*k;
    boolean estimating = (u > k);

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
      for (int i=0; i<u; i++) { sk1.update(i); }

      double sk1est = sk1.getEstimate();
      double sk1lb  = sk1.getLowerBound(2);
      double sk1ub  = sk1.getUpperBound(2);
      assertEquals(sk1.isEstimationMode(), estimating);

      Sketch sk2 = Sketch.wrap(mem);

      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertEquals(sk2.isEstimationMode(), estimating);
    }
  }

  @Test
  public void checkDQStoCompactForms() {
    int k = 512;
    int u = 4*k;
    boolean estimating = (u > k);
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      assertTrue(usk.isDirect());
      assertFalse(usk.isCompact());
      assertFalse(usk.isOrdered());

      for (int i=0; i<u; i++) { usk.update(i); }

      sk1.rebuild(); //forces size back to k

      //get baseline values
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), estimating);

      CompactSketch csk;

      csk = usk.compact(false,  null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

      csk = usk.compact(true, null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactOrderedSketch");

      int bytes = usk.getCurrentBytes(true);
      assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      csk = usk.compact(false,  mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      mem2.clear();
      csk = usk.compact(true, mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
      csk.toString(false, true, 0, false);
    }
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    int k = 512;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      //empty
      usk.toString(false, true, 0, false); //exercise toString
      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), false);

      int bytes = usk.getCurrentBytes(true); //compact form
      assertEquals(bytes, 8);
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      CompactSketch csk2 = usk.compact(false,  mem2);
      assertEquals(csk2.getEstimate(), uskEst);
      assertEquals(csk2.getLowerBound(2), uskLB);
      assertEquals(csk2.getUpperBound(2), uskUB);
      assertEquals(csk2.isEmpty(), true);
      assertEquals(csk2.isEstimationMode(), false);
      assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      CompactSketch csk3 = usk.compact(true, mem2);
      csk3.toString(false, true, 0, false);
      csk3.toString();
      assertEquals(csk3.getEstimate(), uskEst);
      assertEquals(csk3.getLowerBound(2), uskLB);
      assertEquals(csk3.getUpperBound(2), uskUB);
      assertEquals(csk3.isEmpty(), true);
      assertEquals(csk3.isEstimationMode(), false);
      assertEquals(csk3.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    }
  }

  @Test
  public void checkEstMode() {
    int k = 4096;
    int u = 2*k;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertTrue(sk1.getRetainedEntries(false) > k);
    }
  }

  @Test
  public void checkSamplingMode() {
    int k = 4096;
    float p = (float)0.5;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setP(p).setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      for (int i = 0; i < k; i++ ) { usk.update(i); }

      double p2 = sk1.getP();
      double theta = sk1.getTheta();
      assertTrue(theta <= p2);

      double est = usk.getEstimate();
      assertEquals(k, est, k *.05);
      double ub = usk.getUpperBound(1);
      assertTrue(ub > est);
      double lb = usk.getLowerBound(1);
      assertTrue(lb < est);
    }
  }

  @Test
  public void checkErrorBounds() {
    int k = 512;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      //Exact mode
      for (int i = 0; i < k; i++ ) { usk.update(i); }

      double est = usk.getEstimate();
      double lb = usk.getLowerBound(2);
      double ub = usk.getUpperBound(2);
      assertEquals(est, ub, 0.0);
      assertEquals(est, lb, 0.0);

      //Est mode
      int u = 100*k;
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
  }

  //Empty Tests
  @Test
  public void checkEmptyAndP() {
    //virgin, p = 1.0
    int k = 1024;
    float p = (float)1.0;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setP(p).setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());
      usk.update(1);
      assertEquals(sk1.getRetainedEntries(true), 1);
      assertFalse(usk.isEmpty());

      //virgin, p = .001
      p = (float)0.001;
      byte[] memArr2 = new byte[(int) mem.getCapacity()];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);
      UpdateSketch usk2 = UpdateSketch.builder().setP(p).setNominalEntries(k).build(mem2);
      sk1 = (DirectQuickSelectSketch)usk2;

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
  }

  @Test
  public void checkUpperAndLowerBounds() {
    int k = 512;
    int u = 2*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      for (int i = 0; i < u; i++ ) { usk.update(i); }

      double est = usk.getEstimate();
      double ub = usk.getUpperBound(1);
      double lb = usk.getLowerBound(1);
      assertTrue(ub > est);
      assertTrue(lb < est);
    }
  }

  @Test
  public void checkRebuild() {
    int k = 512;
    int u = 4*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

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
    }
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int k = 512;
    int u = 4*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

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

      assertNotNull(sk1.getMemory());
      assertFalse(sk1.isOrdered());
    }
  }

  @Test
  public void checkExactModeMemoryArr() {
    int k = 4096;
    int u = 4096;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertEquals(usk.getEstimate(), u, 0.0);
      assertEquals(sk1.getRetainedEntries(false), u);
    }
  }

  @Test
  public void checkEstModeMemoryArr() {
    int k = 4096;
    int u = 2*k;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }

      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(sk1.getRetainedEntries(false) > k);
    }
  }

  @Test
  public void checkEstModeNativeMemory() {
    int k = 4096;
    int u = 2*k;
    int memCapacity = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    try(WritableDirectHandle memHandler = WritableMemory.allocateDirect(memCapacity)) {

      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(memHandler.get());
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }
      double est = usk.getEstimate();
      println(""+est);
      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(sk1.getRetainedEntries(false) > k);
    }
  }

  @Test
  public void checkConstructReconstructFromMemory() {
    int k = 4096;
    int u = 2*k;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(h.get());
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); } //force estimation

      double est1 = usk.getEstimate();
      int count1 = usk.getRetainedEntries(false);
      assertEquals(est1, u, u*.05);
      assertTrue(count1 >= k);

      byte[] serArr;
      double est2;
      int count2;

      serArr = usk.toByteArray();

      WritableMemory mem2 = WritableMemory.wrap(serArr);

      //reconstruct to Native/Direct
      UpdateSketch usk2 = Sketches.wrapUpdateSketch(mem2);

      est2 = usk2.getEstimate();
      count2 = usk2.getRetainedEntries(false);

      assertEquals(count2, count1);
      assertEquals(est2, est1, 0.0);
    }
  }

  @Test(expectedExceptions = SketchesReadOnlyException.class)
  public void updateAfterReadOnlyWrap() {
    UpdateSketch usk1 = UpdateSketch.builder().build();
    UpdateSketch usk2 = (UpdateSketch) Sketch.wrap(Memory.wrap(usk1.toByteArray()));
    usk2.update(0);
  }

  public void updateAfterWritableWrap() {
    UpdateSketch usk1 = UpdateSketch.builder().build();
    UpdateSketch usk2 = UpdateSketch.wrap(WritableMemory.wrap(usk1.toByteArray()));
    usk2.update(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    int k = 512;
    UpdateSketch qs = UpdateSketch.builder().setFamily(QUICKSELECT).setNominalEntries(k).build();
    qs.hashUpdate(-1L);
  }

  @Test
  public void checkConstructorSrcMemCorruptions() {
    int k = 1024; //lgNomLongs = 10
    int u = k; //exact mode, lgArrLongs = 11

    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    byte[] arr1 = new byte[bytes];
    WritableMemory mem1 = WritableMemory.wrap(arr1);
    ResizeFactor rf = ResizeFactor.X1; //0
    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(mem1);
    for (int i=0; i<u; i++) { usk1.update(i); }
    //println(PreambleUtil.toString(mem1));
    @SuppressWarnings("unused")
    UpdateSketch usk2;
    mem1.putByte(FAMILY_BYTE, (byte) 3); //corrupt Family by setting to Compact
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //Pass
    }
    mem1.putByte(FAMILY_BYTE, (byte) 2); //fix Family
    mem1.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); //corrupt preLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putByte(PREAMBLE_LONGS_BYTE, (byte) 3); //fix preLongs
    mem1.putByte(SER_VER_BYTE, (byte) 2); //corrupt serVer
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putByte(SER_VER_BYTE, (byte) 3); //fix serVer

    mem1.putLong(THETA_LONG, Long.MAX_VALUE >>> 1); //corrupt theta and
    mem1.putByte(LG_ARR_LONGS_BYTE, (byte) 10); //corrupt lgArrLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putLong(THETA_LONG, Long.MAX_VALUE); //fix theta and
    mem1.putByte(LG_ARR_LONGS_BYTE, (byte) 11); //fix lgArrLongs
    byte badFlags = (byte) (BIG_ENDIAN_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | ORDERED_FLAG_MASK);
    mem1.putByte(FLAGS_BYTE, badFlags);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }

    byte[] arr2 = Arrays.copyOfRange(arr1, 0, bytes-1); //corrupt length
    WritableMemory mem2 = WritableMemory.wrap(arr2);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem2, DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
  }

  @Test
  public void checkCorruptRFWithInsufficientArray() {
    int k = 1024; //lgNomLongs = 10

    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    byte[] arr = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(arr);
    ResizeFactor rf = ResizeFactor.X8; // 3
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(mem);
    usk.update(0);

    insertLgResizeFactor(mem, 0); // corrupt RF: X1
    UpdateSketch dqss = DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
    assertEquals(dqss.getResizeFactor(), ResizeFactor.X2); // force-promote to X2
  }

  @Test
  public void checkFamilyAndRF() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
    assertEquals(sketch.getResizeFactor(), ResizeFactor.X8);
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigMem() {
    int k = 1 << 14;
    int u = 1 << 20;
    WritableMemory mem = WritableMemory.wrap(new byte[(8*k*16) +24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    for (int i=0; i<u; i++) { sketch.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +24]);
    Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    mem.putByte(LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkMoveAndResize() {
    int k = 1 << 12;
    int u = 2 * k;
    int bytes = Sketches.getMaxUpdateSketchBytes(k);
      try (WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes/2)) { //will request
      WritableMemory wmem = wdh.get();
      UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
      assertTrue(sketch.isSameResource(wmem));
      for (int i = 0; i < u; i++) { sketch.update(i); }
      assertFalse(sketch.isSameResource(wmem));
    }
  }

  @Test
  public void checkReadOnlyRebuildResize() {
    int k = 1 << 12;
    int u = 2 * k;
    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    try (WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes/2)) { //will request
      WritableMemory wmem = wdh.get();
      UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
      for (int i = 0; i < u; i++) { sketch.update(i); }
      double est1 = sketch.getEstimate();
      byte[] ser = sketch.toByteArray();
      Memory mem = Memory.wrap(ser);
      UpdateSketch roSketch = (UpdateSketch) Sketches.wrapSketch(mem);
      double est2 = roSketch.getEstimate();
      assertEquals(est2, est1);
      try {
        roSketch.rebuild();
        fail();
      } catch (SketchesReadOnlyException e) {
        //expected
      }
      try {
        roSketch.reset();
        fail();
      } catch (SketchesReadOnlyException e) {
        //expected
      }
    }

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

  private static final int getMaxBytes(int k) {
    return (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
  }

  private static WritableDirectHandle makeNativeMemory(int k) {
    return WritableMemory.allocateDirect(getMaxBytes(k));
  }

}
