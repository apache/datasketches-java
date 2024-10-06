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

import static org.apache.datasketches.common.Family.QUICKSELECT;
import static org.apache.datasketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
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

import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DirectQuickSelectSketchTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch.builder().setNominalEntries(k).build(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorMemTooSmall() {
    int k = 16;
    try (WritableMemory mem = makeNativeMemory(k/2)) {
      UpdateSketch.builder().setNominalEntries(k).build(mem);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyIllegalFamilyID_heapify() {
    int k = 512;
    int bytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.writableWrap(new byte[bytes]);
    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Family ID byte

    //try to heapify the corrupted mem
    Sketch.heapify(mem); //catch in Sketch.constructHeapSketch
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    int u = 2*k; //thus estimating

    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
      for (int i=0; i<u; i++) { sk1.update(i); }

      double sk1est = sk1.getEstimate();
      double sk1lb  = sk1.getLowerBound(2);
      double sk1ub  = sk1.getUpperBound(2);
      assertTrue(sk1.isEstimationMode());
      assertEquals(sk1.getClass().getSimpleName(), "DirectQuickSelectSketch");
      int curCount1 = sk1.getRetainedEntries(true);
      assertTrue(sk1.isDirect());
      assertTrue(sk1.hasMemory());
      assertFalse(sk1.isDirty());
      assertTrue(sk1.hasMemory());
      assertEquals(sk1.getCurrentPreambleLongs(), 3);

      UpdateSketch sk2 = Sketches.heapifyUpdateSketch(mem);
      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertTrue(sk2.isEstimationMode());
      assertEquals(sk2.getClass().getSimpleName(), "HeapQuickSelectSketch");
      int curCount2 = sk2.getRetainedEntries(true);
      long[] cache = sk2.getCache();
      assertEquals(curCount1, curCount2);
      long thetaLong = sk2.getThetaLong();
      int cacheCount = HashOperations.count(cache, thetaLong);
      assertEquals(curCount1, cacheCount);
      assertFalse(sk2.isDirect());
      assertFalse(sk2.hasMemory());
      assertFalse(sk2.isDirty());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_wrap() {
    int k = 512;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.writableWrap(new byte[maxBytes]);
    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    Sketch.wrap(mem); //catch in Sketch.constructDirectSketch
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWrapIllegalFamilyID_direct() {
    int k = 512;
    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);
    WritableMemory mem = WritableMemory.writableWrap(new byte[maxBytes]);
    UpdateSketch.builder().setNominalEntries(k).build(mem);

    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to wrap the corrupted mem
    DirectQuickSelectSketch.writableWrap(mem, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  @Test (expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int k = 512;
    long seed1 = 1021;
    long seed2 = ThetaUtil.DEFAULT_UPDATE_SEED;
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setSeed(seed1).setNominalEntries(k).build(mem);
      byte[] byteArray = usk.toByteArray();
      Memory srcMem = Memory.wrap(byteArray);
      Sketch.heapify(srcMem, seed2);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptLgNomLongs() {
    int k = 16;
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch.builder().setNominalEntries(k).build(mem);
      mem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
      Sketch.heapify(mem, ThetaUtil.DEFAULT_UPDATE_SEED);
    }
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      for (int i=0; i< k; i++) { usk.update(i); }

      int bytes = usk.getCurrentBytes();
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    int u = 2*k; //thus estimating
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build(mem);
      for (int i=0; i<u; i++) { sk1.update(i); }

      double sk1est = sk1.getEstimate();
      double sk1lb  = sk1.getLowerBound(2);
      double sk1ub  = sk1.getUpperBound(2);
      assertTrue(sk1.isEstimationMode());

      Sketch sk2 = Sketch.wrap(mem);

      assertEquals(sk2.getEstimate(), sk1est);
      assertEquals(sk2.getLowerBound(2), sk1lb);
      assertEquals(sk2.getUpperBound(2), sk1ub);
      assertEquals(sk2.isEmpty(), false);
      assertTrue(sk2.isEstimationMode());
    }
  }

  @Test
  public void checkDQStoCompactForms() {
    int k = 512;
    int u = 4*k; //thus estimating
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      assertTrue(usk.isDirect());
      assertTrue(usk.hasMemory());
      assertFalse(usk.isCompact());
      assertFalse(usk.isOrdered());

      for (int i=0; i<u; i++) { usk.update(i); }

      sk1.rebuild(); //forces size back to k

      //get baseline values
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertTrue(usk.isEstimationMode());

      CompactSketch csk;

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

      int bytes = usk.getCompactBytes();
      assertEquals(bytes, (k*8) + (Family.COMPACT.getMaxPreLongs() << 3));
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.writableWrap(memArr2);

      csk = usk.compact(false,  mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");

      mem2.clear();
      csk = usk.compact(true, mem2);
      assertEquals(csk.getEstimate(), uskEst);
      assertEquals(csk.getLowerBound(2), uskLB);
      assertEquals(csk.getUpperBound(2), uskUB);
      assertEquals(csk.isEmpty(), false);
      assertTrue(csk.isEstimationMode());
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactSketch");
      csk.toString(false, true, 0, false);
    }
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    int k = 512;
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);

      //empty
      usk.toString(false, true, 0, false); //exercise toString
      assertEquals(usk.getClass().getSimpleName(), "DirectQuickSelectSketch");
      double uskEst = usk.getEstimate();
      double uskLB  = usk.getLowerBound(2);
      double uskUB  = usk.getUpperBound(2);
      assertEquals(usk.isEstimationMode(), false);

      int bytes = usk.getCompactBytes(); //compact form
      assertEquals(bytes, 8);
      byte[] memArr2 = new byte[bytes];
      WritableMemory mem2 = WritableMemory.writableWrap(memArr2);

      CompactSketch csk2 = usk.compact(false,  mem2);
      assertEquals(csk2.getEstimate(), uskEst);
      assertEquals(csk2.getLowerBound(2), uskLB);
      assertEquals(csk2.getUpperBound(2), uskUB);
      assertEquals(csk2.isEmpty(), true);
      assertEquals(csk2.isEstimationMode(), false);
      assertEquals(csk2.getClass().getSimpleName(), "DirectCompactSketch");

      CompactSketch csk3 = usk.compact(true, mem2);
      csk3.toString(false, true, 0, false);
      csk3.toString();
      assertEquals(csk3.getEstimate(), uskEst);
      assertEquals(csk3.getLowerBound(2), uskLB);
      assertEquals(csk3.getUpperBound(2), uskUB);
      assertEquals(csk3.isEmpty(), true);
      assertEquals(csk3.isEstimationMode(), false);
      assertEquals(csk3.getClass().getSimpleName(), "DirectCompactSketch");
    }
  }

  @Test
  public void checkEstMode() {
    int k = 4096;
    int u = 2*k;

    try (WritableMemory mem = makeNativeMemory(k)) {
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

    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setP(p).setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks

      assertTrue(usk.isEmpty());
      usk.update(1);
      assertEquals(sk1.getRetainedEntries(true), 1);
      assertFalse(usk.isEmpty());

      //virgin, p = .001
      p = (float)0.001;
      byte[] memArr2 = new byte[(int) mem.getCapacity()];
      WritableMemory mem2 = WritableMemory.writableWrap(memArr2);
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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
    try (WritableMemory mem = makeNativeMemory(k)) {
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

    try (WritableMemory mem = makeNativeMemory(k)) {
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

    try(WritableMemory mem = WritableMemory.allocateDirect(memCapacity)) { //will not request more memory
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
      DirectQuickSelectSketch sk1 = (DirectQuickSelectSketch)usk; //for internal checks
      assertTrue(usk.isEmpty());

      for (int i = 0; i< u; i++) { usk.update(i); }
      double est = usk.getEstimate();
      println(""+est);
      assertEquals(usk.getEstimate(), u, u*.05);
      assertTrue(sk1.getRetainedEntries(false) > k);
      Memory mem2 = usk.getMemory();
      assertTrue(mem2.isAlive());
      assertTrue(mem2.isDirect()); //still off heap.
      assertTrue(mem2.isSameResource(mem));
    }
  }

  @Test
  public void checkConstructReconstructFromMemory() {
    int k = 4096;
    int u = 2*k;

    try (WritableMemory mem = makeNativeMemory(k)) {
      UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build(mem);
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

      WritableMemory mem2 = WritableMemory.writableWrap(serArr);

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
    UpdateSketch usk2 = UpdateSketch.wrap(WritableMemory.writableWrap(usk1.toByteArray()));
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
    WritableMemory mem1 = WritableMemory.writableWrap(arr1);
    ResizeFactor rf = ResizeFactor.X1; //0
    UpdateSketch usk1 = UpdateSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(mem1);
    for (int i=0; i<u; i++) { usk1.update(i); }
    //println(PreambleUtil.toString(mem1));
    @SuppressWarnings("unused")
    UpdateSketch usk2;
    mem1.putByte(FAMILY_BYTE, (byte) 3); //corrupt Family by setting to Compact
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, ThetaUtil.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //Pass
    }
    mem1.putByte(FAMILY_BYTE, (byte) 2); //fix Family
    mem1.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); //corrupt preLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, ThetaUtil.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putByte(PREAMBLE_LONGS_BYTE, (byte) 3); //fix preLongs
    mem1.putByte(SER_VER_BYTE, (byte) 2); //corrupt serVer
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, ThetaUtil.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putByte(SER_VER_BYTE, (byte) 3); //fix serVer

    mem1.putLong(THETA_LONG, Long.MAX_VALUE >>> 1); //corrupt theta and
    mem1.putByte(LG_ARR_LONGS_BYTE, (byte) 10); //corrupt lgArrLongs
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, ThetaUtil.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
    mem1.putLong(THETA_LONG, Long.MAX_VALUE); //fix theta and
    mem1.putByte(LG_ARR_LONGS_BYTE, (byte) 11); //fix lgArrLongs
    byte badFlags = (byte) (BIG_ENDIAN_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | ORDERED_FLAG_MASK);
    mem1.putByte(FLAGS_BYTE, badFlags);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem1, ThetaUtil.DEFAULT_UPDATE_SEED);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }

    byte[] arr2 = Arrays.copyOfRange(arr1, 0, bytes-1); //corrupt length
    WritableMemory mem2 = WritableMemory.writableWrap(arr2);
    try {
      usk2 = DirectQuickSelectSketch.writableWrap(mem2, ThetaUtil.DEFAULT_UPDATE_SEED);
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
    WritableMemory mem = WritableMemory.writableWrap(arr);
    ResizeFactor rf = ResizeFactor.X8; // 3
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).setResizeFactor(rf).build(mem);
    usk.update(0);

    insertLgResizeFactor(mem, 0); // corrupt RF: X1
    UpdateSketch dqss = DirectQuickSelectSketch.writableWrap(mem, ThetaUtil.DEFAULT_UPDATE_SEED);
    assertEquals(dqss.getResizeFactor(), ResizeFactor.X2); // force-promote to X2
  }

  @Test
  public void checkFamilyAndRF() {
    int k = 16;
    WritableMemory mem = WritableMemory.writableWrap(new byte[(k*16) +24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
    assertEquals(sketch.getResizeFactor(), ResizeFactor.X8);
  }

  //checks Alex's bug where lgArrLongs > lgNomLongs +1.
  @Test
  public void checkResizeInBigMem() {
    int k = 1 << 14;
    int u = 1 << 20;
    WritableMemory mem = WritableMemory.writableWrap(new byte[(8*k*16) +24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    for (int i=0; i<u; i++) { sketch.update(i); }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadLgNomLongs() {
    int k = 16;
    WritableMemory mem = WritableMemory.writableWrap(new byte[(k*16) +24]);
    Sketches.updateSketchBuilder().setNominalEntries(k).build(mem);
    mem.putByte(LG_NOM_LONGS_BYTE, (byte) 3); //Corrupt LgNomLongs byte
    DirectQuickSelectSketch.writableWrap(mem, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkMoveAndResize() {
    int k = 1 << 12;
    int u = 2 * k;
    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    WritableMemory wmem = WritableMemory.allocateDirect(bytes/2); //will request more memory
    WritableMemory wmemCopy = wmem;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
    assertTrue(sketch.isSameResource(wmem));
    for (int i = 0; i < u; i++) { sketch.update(i); }
    Memory mem = sketch.getMemory();
    assertTrue(mem.isAlive());
    assertFalse(mem.isDirect()); //now on heap.
    assertTrue(wmemCopy.isDirect()); //original copy
    assertFalse(wmem.isAlive()); //wmem closed by MemoryRequestServer
    assertFalse(wmemCopy.isAlive()); //original copy closed
  }

  @Test
  public void checkReadOnlyRebuildResize() {
    int k = 1 << 12;
    int u = 2 * k;
    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    WritableMemory wmem = WritableMemory.allocateDirect(bytes/2); //will request more memory
    WritableMemory wmemCopy = wmem;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
    for (int i = 0; i < u; i++) { sketch.update(i); }
    double est1 = sketch.getEstimate();
    byte[] ser = sketch.toByteArray();
    Memory mem = Memory.wrap(ser);
    UpdateSketch roSketch = (UpdateSketch) Sketches.wrapSketch(mem);
    double est2 = roSketch.getEstimate();
    assertEquals(est2, est1);
    Memory mem2 = sketch.getMemory();
    assertTrue(mem2.isAlive());
    assertFalse(mem2.isDirect()); //now on heap
    assertFalse(wmem.isAlive());  //wmem closed by MemoryRequestServer
    assertTrue(wmemCopy.isDirect());
    assertFalse(wmemCopy.isAlive());
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

  private static WritableMemory makeNativeMemory(int k) {
    return WritableMemory.allocateDirect(getMaxBytes(k));
  }

}
