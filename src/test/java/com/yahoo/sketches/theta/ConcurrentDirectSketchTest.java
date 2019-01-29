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
public class ConcurrentDirectSketchTest {

  private int lgK;
  private volatile ConcurrentSharedThetaSketch shared;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);

      ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

      assertTrue(usk.isEmpty());

      for (int i = 0; i< k; i++) { sk1.update(i); }
      waitForPropagationToComplete();

      assertFalse(usk.isEmpty());
      assertEquals(usk.getEstimate(), k, 0.0);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);

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
      buildConcSketch(mem);
      assertTrue(shared instanceof ConcurrentDirectQuickSelectSketch);
      assertTrue(((UpdateSketch)shared).compact().isCompact());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    lgK = 3;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      buildConcSketch(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorMemTooSmall() {
    lgK = 4;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k/2)) {
      WritableMemory mem = h.get();
      buildConcSketch(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    lgK = 9;
    int k = 1 << lgK;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[bytes]);
    buildConcSketch(mem);

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
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch sk1 = bldr.buildLocalInternal(shared);
      for (int i=0; i<u; i++) { sk1.update(i); }
      waitForPropagationToComplete();

      double sk1est = sk1.getEstimate();
      double sk1lb  = sk1.getLowerBound(2);
      double sk1ub  = sk1.getUpperBound(2);
      assertEquals(sk1.isEstimationMode(), estimating);
      assertEquals(sk1.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      int curCount1 = ((UpdateSketch)shared).getRetainedEntries(true);
      assertTrue(sk1.isDirect());
      assertEquals(sk1.getCurrentPreambleLongs(false), 3);

      UpdateSketch sk2 = Sketches.heapifyUpdateSketch(mem);
      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertFalse(sk2.isEmpty());
      assertEquals(sk2.isEstimationMode(), estimating);
      assertEquals(sk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
      int curCount2 = sk2.getRetainedEntries(true);
      long[] cache = sk2.getCache();
      assertEquals(curCount1, curCount2);
      long thetaLong = sk2.getThetaLong();
      int cacheCount = HashOperations.count(cache, thetaLong);
      assertEquals(curCount1, cacheCount);
      assertFalse(sk2.isDirect());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    lgK = 9;
    int k = 1 << lgK;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.wrap(new byte[maxBytes]);

    buildConcSketch(mem);

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

    buildConcSketch(mem);

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
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);
      byte[] byteArray = usk.toByteArray();
      Memory srcMem = Memory.wrap(byteArray);
      Sketch.heapify(srcMem, seed2);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    lgK = 4;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      buildConcSketch(mem);
      mem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
      Sketch.heapify(mem, DEFAULT_UPDATE_SEED);
    }
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = buildConcSketch(mem);

      for (int i=0; i< k; i++) { usk.update(i); }
      waitForPropagationToComplete();

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
    lgK = 12;
    int k = 1 << lgK;
    int u = 2*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch usk = buildConcSketch(mem);

      for (int i=0; i<u; i++) { usk.update(i); }
      waitForPropagationToComplete();

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
    lgK = 9;
    int k = 1 << lgK;
    int u = 2*k;
    boolean estimating = (u > k);

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();
      UpdateSketch sk1 = buildConcSketch(mem);
      for (int i=0; i<u; i++) { sk1.update(i); }
      waitForPropagationToComplete();

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
    lgK = 9;
    int k = 1 << lgK;
    int u = 4*k;
    boolean estimating = (u > k);
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);

      assertEquals(usk.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      assertTrue(usk.isDirect());

      for (int i=0; i<u; i++) { usk.update(i); }
      waitForPropagationToComplete();

      ((UpdateSketch)shared).rebuild(); //forces size back to k

      //get baseline values
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), estimating);

      CompactSketch csk;

      csk = ((UpdateSketch)shared).compact(false,  null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

      csk = ((UpdateSketch)shared).compact(true, null);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "HeapCompactOrderedSketch");

      int bytes = usk.getCurrentBytes(true);
      assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      csk = ((UpdateSketch)shared).compact(false,  mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertFalse(csk.isEmpty());
      assertEquals(csk.isEstimationMode(), estimating);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      mem2.clear();
      csk = ((UpdateSketch)shared).compact(true, mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
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
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);

      //empty
      usk.toString(false, true, 0, false); //exercise toString
      assertEquals(usk.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertFalse(usk.isEstimationMode());

      int bytes = usk.getCurrentBytes(true); //compact form
      assertEquals(bytes, 8);
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.wrap(memArr2);

      CompactSketch csk2 = ((UpdateSketch)shared).compact(false,  mem2);
      assertEquals(csk2.getEstimate(), uskEst);
      assertEquals(csk2.getLowerBound(2), uskLB);
      assertEquals(csk2.getUpperBound(2), uskUB);
      assertTrue(csk2.isEmpty());
      assertFalse(csk2.isEstimationMode());
      assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

      CompactSketch csk3 = ((UpdateSketch)shared).compact(true, mem2);
      csk3.toString(false, true, 0, false);
      csk3.toString();
      assertEquals(csk3.getEstimate(), uskEst);
      assertEquals(csk3.getLowerBound(2), uskLB);
      assertEquals(csk3.getUpperBound(2), uskUB);
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
      shared = bldr.buildSharedInternal(mem);
      ConcurrentHeapThetaBuffer usk = bldr.buildLocalInternal(shared);

      assertTrue(usk.isEmpty());
      int u = usk.getHashTableThreshold();

      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();

      assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);
    }
  }

  @Test
  public void checkErrorBounds() {
    lgK = 9;
    int k = 1 << lgK;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      UpdateSketch usk = buildConcSketch(mem);

      //Exact mode
      for (int i = 0; i < k; i++ ) { usk.update(i); }
      waitForPropagationToComplete();

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
      waitForPropagationToComplete();
      est = usk.getEstimate();
      lb = usk.getLowerBound(2);
      ub = usk.getUpperBound(2);
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

      UpdateSketch usk = buildConcSketch(mem);

      for (int i = 0; i < u; i++ ) { usk.update(i); }
      waitForPropagationToComplete();

      double est = usk.getEstimate();
      double ub = usk.getUpperBound(1);
      double lb = usk.getLowerBound(1);
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
      shared = bldr.buildSharedInternal(mem);
      ConcurrentHeapThetaBuffer usk = bldr.buildLocalInternal(shared);

      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();

      assertFalse(usk.isEmpty());
      assertTrue(usk.getEstimate() > 0.0);
<<<<<<< HEAD
      assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);

      ((UpdateSketch)shared).rebuild();
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
      sk1.rebuild();
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
=======
      assertTrue(shared.getSharedRetainedEntries(false) >= k);

      shared.rebuildShared();
      assertEquals(shared.getSharedRetainedEntries(false), k);
      assertEquals(shared.getSharedRetainedEntries(true), k);
      usk.rebuild();
      assertEquals(shared.getSharedRetainedEntries(false), k);
      assertEquals(shared.getSharedRetainedEntries(true), k);
>>>>>>> remove cache limit, add concurrency error factor
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
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);
      ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

      assertTrue(usk.isEmpty());

      int u = 4*sk1.getHashTableThreshold();
      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();

      assertFalse(usk.isEmpty());
<<<<<<< HEAD
      assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);
=======
      assertTrue(shared.getSharedRetainedEntries(false) >= k);
>>>>>>> remove cache limit, add concurrency error factor
      assertTrue(sk1.getThetaLong() < Long.MAX_VALUE);

      shared.resetShared();
      sk1.reset();
      assertTrue(usk.isEmpty());
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), 0);
      assertEquals(usk.getEstimate(), 0.0, 0.0);
      assertEquals(sk1.getThetaLong(), Long.MAX_VALUE);
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
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();

      assertEquals(usk.getEstimate(), u, 0.0);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), u);
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
      shared = bldr.buildSharedInternal(mem);
      ConcurrentHeapThetaBuffer usk = bldr.buildLocalInternal(shared);
      assertTrue(usk.isEmpty());

      int u = 3*usk.getHashTableThreshold();
      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();

<<<<<<< HEAD
      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);
=======
      double est = usk.getEstimate();
      assertTrue(est<u*1.05 && est > u*0.95);
      assertTrue(shared.getSharedRetainedEntries(false) >= k);
>>>>>>> remove cache limit, add concurrency error factor
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
      shared = bldr.buildSharedInternal(memHandler.get());
      ConcurrentHeapThetaBuffer usk = bldr.buildLocalInternal(shared);
      assertTrue(usk.isEmpty());
      int u = 3*usk.getHashTableThreshold();

      for (int i = 0; i< u; i++) { usk.update(i); }
      waitForPropagationToComplete();
      double est = usk.getEstimate();
<<<<<<< HEAD
      println(""+est);
      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);
=======
      assertTrue(est<u*1.05 && est > u*0.95);
      assertTrue(shared.getSharedRetainedEntries(false) >= k);
>>>>>>> remove cache limit, add concurrency error factor
    }
  }

  @Test
  public void checkConstructReconstructFromMemory() {
    lgK = 12;
    int k = 1 << lgK;

    try (WritableDirectHandle h = makeNativeMemory(k)) {
      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildSharedInternal(h.get());
      ConcurrentHeapThetaBuffer usk = bldr.buildLocalInternal(shared);
      assertTrue(usk.isEmpty());
      int u = 3*usk.getHashTableThreshold();

      for (int i = 0; i< u; i++) { usk.update(i); } //force estimation
      waitForPropagationToComplete();

      double est1 = usk.getEstimate();
<<<<<<< HEAD
      int count1 = ((UpdateSketch)shared).getRetainedEntries(false);
      assertEquals(est1, u, u*.05);
=======
      int count1 = shared.getSharedRetainedEntries(false);
      assertTrue(est1<u*1.05 && est1 > u*0.95);
>>>>>>> remove cache limit, add concurrency error factor
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


  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigMem() {
    lgK = 14;
    int k = 1 << lgK;
    int u = 1 << 20;
    WritableMemory mem = WritableMemory.wrap(new byte[(8*k*16) +24]);
    UpdateSketch sketch = buildConcSketch(mem);
    for (int i=0; i<u; i++) { sketch.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int k = 16;
    lgK = 4;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*16) +24]);
    buildConcSketch(mem);
    mem.putByte(LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(mem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkBackgroundPropagation() {
    lgK = 4;
    int k = 1 << lgK;
    int u = 5*k;
    try (WritableDirectHandle h = makeNativeMemory(k)) {
      WritableMemory mem = h.get();

      final UpdateSketchBuilder bldr = configureBuilder();
      //must build shared first
      shared = bldr.buildSharedInternal(mem);
      UpdateSketch usk = bldr.buildLocalInternal(shared);
      ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

      assertTrue(usk.isEmpty());

      int i = 0;
      for (; i< k; i++) {
        usk.update(i);
      }
      waitForPropagationToComplete();
      assertFalse(usk.isEmpty());
      assertTrue(usk.getEstimate() > 0.0);
      long theta1 = shared.getVolatileTheta();

      for (; i< u; i++) {
        usk.update(i);
      }
      waitForPropagationToComplete();

      long theta2 = shared.getVolatileTheta();
<<<<<<< HEAD
      int entries = ((UpdateSketch)shared).getRetainedEntries(false);
      assertTrue((entries > k) || (theta2 < theta1),"entries="+entries+" k="+k+" theta1="+theta1+" theta2="+theta2);
=======
      int entries = shared.getSharedRetainedEntries(false);
      assertTrue((entries > k) || (theta2 < theta1),
          "entries="+entries+" k="+k+" theta1="+theta1+" theta2="+theta2);
>>>>>>> remove cache limit, add concurrency error factor

      ((UpdateSketch)shared).rebuild();
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
      sk1.rebuild();
      assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
      assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
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

  private UpdateSketch buildConcSketch(WritableMemory mem) {
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(mem);
    assertFalse(shared.isPropagationInProgress());
    return bldr.buildLocalInternal(shared);
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setSharedLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(lgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
<<<<<<< HEAD
    return bldr;
  }
  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilderWithCache() {
    final UpdateSketchBuilder bldr = configureBuilder();
    int k = 1 << lgK;
    bldr.setCacheLimit(k);
=======
    bldr.setSharedIsDirect(true);
    bldr.setMaxConcurrencyError(0.0);
>>>>>>> remove cache limit, add concurrency error factor
    return bldr;
  }

  private void waitForPropagationToComplete() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    while (shared.isPropagationInProgress()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
