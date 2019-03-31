/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author eshcar
 */
public class ConcurrentDirectQuickSelectSketchTest {

  private int lgK;
  private volatile UpdateSketch shared;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      assertTrue(local.isEmpty());

      for (int i = 0; i< k; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      assertFalse(local.isEmpty());
      assertEquals(local.getEstimate(), k, 0.0);
      assertEquals(shared.getRetainedEntries(false), k);

      mem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

      Sketch.wrap(mem);
    }
  }

  @Test
  public void checkDirectCompactConversion() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      buildSharedReturnLocalSketch(mem);
      assertTrue(shared instanceof ConcurrentDirectQuickSelectSketch);
      assertTrue(shared.compact().isCompact());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    lgK = 3;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      buildSharedReturnLocalSketch(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorMemTooSmall() {
    lgK = 4;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k/2)) {
      WritableMemory mem = h.get();
      buildSharedReturnLocalSketch(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    lgK = 9;
    int k = 1 << lgK;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[bytes]);
    buildSharedReturnLocalSketch(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte

    //try to heapify the corrupted mem
    Sketch.heapify(mem); //catch in Sketch.constructHeapSketch
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean estimating = (u > k);

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);
      for (int i=0; i<u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      double sk1est = local.getEstimate();
      double sk1lb  = local.getLowerBound(2);
      double sk1ub  = local.getUpperBound(2);
      assertEquals(local.isEstimationMode(), estimating);
      assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      int curCount1 = shared.getRetainedEntries(true);
      assertTrue(local.isDirect());
      assertTrue(local.hasMemory());
      assertEquals(local.getCurrentPreambleLongs(false), 3);

      UpdateSketch sharedHeap = Sketches.heapifyUpdateSketch(mem);
      assertEquals(sharedHeap.getEstimate(), sk1est);
      assertEquals(sharedHeap.getLowerBound(2), sk1lb);
      assertEquals(sharedHeap.getUpperBound(2), sk1ub);
      assertFalse(sharedHeap.isEmpty());
      assertEquals(sharedHeap.isEstimationMode(), estimating);
      assertEquals(sharedHeap.getClass().getSimpleName(), "HeapQuickSelectSketch");
      int curCount2 = sharedHeap.getRetainedEntries(true);
      long[] cache = sharedHeap.getCache();
      assertEquals(curCount1, curCount2);
      long thetaLong = sharedHeap.getThetaLong();
      int cacheCount = HashOperations.count(cache, thetaLong);
      assertEquals(curCount1, cacheCount);
      assertFalse(sharedHeap.isDirect());
      assertFalse(sharedHeap.hasMemory());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    lgK = 9;
    int k = 1 << lgK;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[maxBytes]);

    buildSharedReturnLocalSketch(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    Sketch.wrap(mem); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    lgK = 9;
    int k = 1 << lgK;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[maxBytes]);

    buildSharedReturnLocalSketch(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    lgK = 9;
    int k = 1 << lgK;
    long seed1 = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder().setSeed(seed1);
      //must build shared first
      shared = bldr.buildShared(mem);
      byte[]  serArr = shared.toByteArray();
      Memory srcMem = Memory.wrap(serArr);
      Sketch.heapify(srcMem, seed2);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    lgK = 4;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      buildSharedReturnLocalSketch(mem);
      mem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
      Sketch.heapify(mem, DEFAULT_UPDATE_SEED);
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    lgK = 4;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      buildSharedReturnLocalSketch(h.get());
      shared.hashUpdate(1);
    }
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      UpdateSketch local = buildSharedReturnLocalSketch(mem);

      for (int i=0; i< k; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      byte[]  serArr = shared.toByteArray();
      Memory srcMem = Memory.wrap(serArr);
      Sketch recoveredShared = Sketch.heapify(srcMem);

      //reconstruct to Native/Direct
      final int bytes = Sketch.getMaxUpdateSketchBytes(k);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      shared = bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wmem);
      UpdateSketch local2 = bldr.buildLocal(shared);

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
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    lgK = 12;
    int k = 1 << lgK;
    int u = 2*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      final UpdateSketchBuilder bldr = configureBuilder();
      UpdateSketch local = buildSharedReturnLocalSketch(mem);

      for (int i=0; i<u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

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
      shared = bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wmem);
      UpdateSketch local2 = bldr.buildLocal(shared);


      assertEquals(local2.getEstimate(), uskEst);
      assertEquals(local2.getLowerBound(2), uskLB);
      assertEquals(local2.getUpperBound(2), uskUB);
      assertEquals(local2.isEmpty(), false);
      assertEquals(local2.isEstimationMode(), true);
      assertEquals(recoveredShared.getClass().getSimpleName(), "HeapQuickSelectSketch");
    }
  }

  @Test
  public void checkWrapMemoryEst() {
    lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean estimating = (u > k);

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch local = buildSharedReturnLocalSketch(mem);
      for (int i=0; i<u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      double sk1est = local.getEstimate();
      double sk1lb  = local.getLowerBound(2);
      double sk1ub  = local.getUpperBound(2);
      assertEquals(local.isEstimationMode(), estimating);

      Sketch local2 = Sketch.wrap(mem);

      assertEquals(local2.getEstimate(), sk1est);
      assertEquals(local2.getLowerBound(2), sk1lb);
      assertEquals(local2.getUpperBound(2), sk1ub);
      assertEquals(local2.isEmpty(), false);
      assertEquals(local2.isEstimationMode(), estimating);
    }
  }

  @Test
  public void checkDQStoCompactForms() {
    lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    boolean estimating = (u > k);
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      assertTrue(local.isDirect());
      assertTrue(local.hasMemory());

      for (int i=0; i<u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

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
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

      csk = shared.compact(true, null);
      assertEquals(csk.getEstimate(), localEst);
      assertEquals(csk.getLowerBound(2), localLB);
      assertEquals(csk.getUpperBound(2), localUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactOrderedSketch");

      int bytes = local.getCurrentBytes(true);
      assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      csk = shared.compact(false,  mem2);
      assertEquals(csk.getEstimate(), localEst);
      assertEquals(csk.getLowerBound(2), localLB);
      assertEquals(csk.getUpperBound(2), localUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      mem2.clear();
      csk = shared.compact(true, mem2);
      assertEquals(csk.getEstimate(), localEst);
      assertEquals(csk.getLowerBound(2), localLB);
      assertEquals(csk.getUpperBound(2), localUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
      csk.toString(false, true, 0, false);
    }
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      //empty
      local.toString(false, true, 0, false); //exercise toString
      assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      double localEst = local.getEstimate();
      double localLB  = local.getLowerBound(2);
      double localUB  = local.getUpperBound(2);
      assertFalse(local.isEstimationMode());

      int bytes = local.getCurrentBytes(true); //compact form
      assertEquals(bytes, 8);
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      CompactSketch csk2 = shared.compact(false,  mem2);
      assertEquals(csk2.getEstimate(), localEst);
      assertEquals(csk2.getLowerBound(2), localLB);
      assertEquals(csk2.getUpperBound(2), localUB);
      assertTrue(csk2.isEmpty());
      assertFalse(csk2.isEstimationMode());
      assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      CompactSketch csk3 = shared.compact(true, mem2);
      csk3.toString(false, true, 0, false);
      csk3.toString();
      assertEquals(csk3.getEstimate(), localEst);
      assertEquals(csk3.getLowerBound(2), localLB);
      assertEquals(csk3.getUpperBound(2), localUB);
      assertTrue(csk3.isEmpty());
      assertFalse(csk3.isEstimationMode());
      assertEquals(csk3.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    }
  }

  @Test
  public void checkEstMode() {
    lgK = 12;
    int k = 1 << lgK;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      assertTrue(local.isEmpty());
      int u = 3*k;

      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      assertTrue(shared.getRetainedEntries(false) > k);
    }
  }

  @Test
  public void checkErrorBounds() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch local = buildSharedReturnLocalSketch(mem);

      //Exact mode
      for (int i = 0; i < k; i++ ) { local.update(i); }
      waitForBgPropagationToComplete();

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
      waitForBgPropagationToComplete();
      est = local.getEstimate();
      lb = local.getLowerBound(2);
      ub = local.getUpperBound(2);
      assertTrue(est <= ub);
      assertTrue(est >= lb);
    }
  }


  @Test
  public void checkUpperAndLowerBounds() {
    lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch local = buildSharedReturnLocalSketch(mem);

      for (int i = 0; i < u; i++ ) { local.update(i); }
      waitForBgPropagationToComplete();

      double est = local.getEstimate();
      double ub = local.getUpperBound(1);
      double lb = local.getLowerBound(1);
      assertTrue(ub > est);
      assertTrue(lb < est);
    }
  }

  @Test
  public void checkRebuild() {
    lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      assertTrue(local.isEmpty());

      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

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
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);

      assertTrue(local.isEmpty());

      int u = 4*k;
      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

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
  }

  @Test
  public void checkExactModeMemoryArr() {
    lgK = 12;
    int k = 1 << lgK;
    int u = k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);
      assertTrue(local.isEmpty());

      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      assertEquals(local.getEstimate(), u, 0.0);
      assertEquals(shared.getRetainedEntries(false), u);
    }
  }

  @Test
  public void checkEstModeMemoryArr() {
    lgK = 12;
    int k = 1 << lgK;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);
      assertTrue(local.isEmpty());

      int u = 3*k;
      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();

      double est = local.getEstimate();
      assertTrue((est < (u * 1.05)) && (est > (u * 0.95)));
      assertTrue(shared.getRetainedEntries(false) >= k);
    }
  }

  @Test
  public void checkEstModeNativeMemory() {
    lgK = 12;
    int k = 1 << lgK;
    int memCapacity = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    try(WritableDirectHandle memHandler = WritableMemory.allocateDirect(memCapacity)) {

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(memHandler.get());
      UpdateSketch local = bldr.buildLocal(shared);
      assertTrue(local.isEmpty());
      int u = 3*k;

      for (int i = 0; i< u; i++) { local.update(i); }
      waitForBgPropagationToComplete();
      double est = local.getEstimate();
      assertTrue((est < (u * 1.05)) && (est > (u * 0.95)));
      assertTrue(shared.getRetainedEntries(false) >= k);
    }
  }

  @Test
  public void checkConstructReconstructFromMemory() {
    lgK = 12;
    int k = 1 << lgK;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(h.get());
      UpdateSketch local = bldr.buildLocal(shared);
      assertTrue(local.isEmpty());
      int u = 3*k;

      for (int i = 0; i< u; i++) { local.update(i); } //force estimation
      waitForBgPropagationToComplete();

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
      shared = bldr.buildSharedFromSketch(recoveredShared, wmem);
      UpdateSketch local2 = bldr.buildLocal(shared);
      est2 = local2.getEstimate();

      assertEquals(est2, est1, 0.0);
    }
  }


  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigMem() {
    lgK = 14;
    int k = 1 << lgK;
    int u = 1 << 20;
    WritableMemory mem = WritableMemory.wrap(new byte[(8*k*16) +24]);
    UpdateSketch local = buildSharedReturnLocalSketch(mem);
    for (int i=0; i<u; i++) { local.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int k = 16;
    lgK = 4;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +24]);
    buildSharedReturnLocalSketch(mem);
    mem.putByte(LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkBackgroundPropagation() {
    lgK = 4;
    int k = 1 << lgK;
    int u = 10*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildShared(mem);
      UpdateSketch local = bldr.buildLocal(shared);
      ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)local; //for internal checks

      assertTrue(local.isEmpty());

      int i = 0;
      for (; i< k; i++) {
        local.update(i);
      }
//      waitForBgPropagationToComplete();
      assertFalse(local.isEmpty());
      assertTrue(local.getEstimate() > 0.0);
      long theta1 = ((ConcurrentSharedThetaSketch)shared).getVolatileTheta();

      for (; i< u; i++) {
        local.update(i);
      }
      waitForBgPropagationToComplete();

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

  private UpdateSketch buildSharedReturnLocalSketch(WritableMemory mem) {
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(mem);
    return bldr.buildLocal(shared);
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(lgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
    return bldr;
  }

  private void waitForBgPropagationToComplete() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ((ConcurrentSharedThetaSketch)shared).awaitBgPropagationTermination();
    ConcurrentPropagationService.resetExecutorService(Thread.currentThread().getId());
    ((ConcurrentSharedThetaSketch)shared).initBgPropagationService();
  }

}
