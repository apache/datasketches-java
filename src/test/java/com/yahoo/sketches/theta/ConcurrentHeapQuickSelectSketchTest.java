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
import static org.testng.Assert.fail;

import java.util.Arrays;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * @author eshcar
 */
public class ConcurrentHeapQuickSelectSketchTest {
  private int lgK;
  private long seed = DEFAULT_UPDATE_SEED;
  //private volatile ConcurrentSharedThetaSketch shared;
  private volatile UpdateSketch shared;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    assertFalse(local.isEmpty());
    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);

    byte[]  serArr = shared.toByteArray();
    WritableMemory mem = WritableMemory.wrap(serArr);
    Sketch sk = Sketch.heapify(mem, seed);
    assertTrue(sk instanceof HeapQuickSelectSketch); //Intentional promotion to Parent

    mem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte
    Sketch.heapify(mem, seed);
  }

  @Test
  public void checkPropagationNotOrdered() {
    int k = 256;
    lgK = 8;
    int u = 200*k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilderNotOrdered();
    assertFalse((1 << bldr.getLocalLgNominalEntries()) == 0);
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertTrue(local.isEmpty());

    for (int i = 0; i < u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    assertFalse(local.isEmpty());
    assertTrue(shared.getRetainedEntries(false) <= u);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);
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
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corruped mem
    Sketch.heapify(mem, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    lgK = 9;
    seed = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    buildSharedReturnLocalSketch();
    byte[] byteArray = shared.toByteArray();
    Memory srcMem = Memory.wrap(byteArray);
    Sketch.heapify(srcMem, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    lgK = 4;
    buildSharedReturnLocalSketch();
    byte[]  serArr = shared.toByteArray();
    WritableMemory srcMem = WritableMemory.wrap(serArr);
    srcMem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void checkIllegalHashUpdate() {
    lgK = 4;
    buildSharedReturnLocalSketch();
    shared.hashUpdate(1);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    UpdateSketch local = buildSharedReturnLocalSketch();

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    byte[]  serArr = shared.toByteArray();
    Memory srcMem = Memory.wrap(serArr);
    Sketch recoveredShared = Sketches.heapifyUpdateSketch(srcMem);

    //reconstruct to Native/Direct
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = bldr.buildSharedFromSketch((UpdateSketch)recoveredShared, wmem);
    UpdateSketch local2 = bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), u, 0.0);
    assertEquals(local2.getLowerBound(2), u, 0.0);
    assertEquals(local2.getUpperBound(2), u, 0.0);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), false);
    assertEquals(local2.getResizeFactor(), local.getResizeFactor());
    local2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    int k = 4096;
    lgK = 12;
    int u = 2*k;
    seed = DEFAULT_UPDATE_SEED;

    final UpdateSketchBuilder bldr = configureBuilder();
    UpdateSketch local = buildSharedReturnLocalSketch();

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), true);
    byte[]  serArr = shared.toByteArray();

    Memory srcMem = Memory.wrap(serArr);
    UpdateSketch recoveredShared = UpdateSketch.heapify(srcMem, seed);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = bldr.buildSharedFromSketch(recoveredShared, wmem);
    UpdateSketch local2 = bldr.buildLocal(shared);
    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), true);
    assertEquals(local2.getResizeFactor(), local.getResizeFactor());
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    lgK = 9;
    int u = 2*k;
    seed = DEFAULT_UPDATE_SEED;
    boolean estimating = (u > k);
    final UpdateSketchBuilder bldr = configureBuilder();
    UpdateSketch local = buildSharedReturnLocalSketch();

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    assertEquals(local.isEstimationMode(), estimating);
    assertFalse(local.isDirect());
    assertFalse(local.hasMemory());

    byte[]  serArr = shared.toByteArray();

    Memory srcMem = Memory.wrap(serArr);
    UpdateSketch recoveredShared = UpdateSketch.heapify(srcMem, DEFAULT_UPDATE_SEED);

    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final WritableMemory wmem = WritableMemory.allocate(bytes);
    shared = bldr.buildSharedFromSketch(recoveredShared, wmem);
    UpdateSketch local2 = bldr.buildLocal(shared);

    assertEquals(local2.getEstimate(), localEst);
    assertEquals(local2.getLowerBound(2), localLB);
    assertEquals(local2.getUpperBound(2), localUB);
    assertEquals(local2.isEmpty(), false);
    assertEquals(local2.isEstimationMode(), estimating);
  }

  @Test
  public void checkHQStoCompactForms() {
    int k = 512;
    lgK = 9;
    int u = 4*k;
    boolean estimating = (u > k);

    int maxBytes = (k << 4) + (Family.QUICKSELECT.getMinPreLongs() << 3);

    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(local.isDirect());
    assertFalse(local.hasMemory());

    for (int i=0; i<u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    shared.rebuild(); //forces size back to k

    //get baseline values
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    int localBytes = local.getCurrentBytes(false);    //size stored as UpdateSketch
    int localCompBytes = local.getCurrentBytes(true); //size stored as CompactSketch
    assertEquals(localBytes, maxBytes);
    assertEquals(local.isEstimationMode(), estimating);

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = shared.compact(false,  null);

    assertEquals(comp1.getEstimate(), localEst);
    assertEquals(comp1.getLowerBound(2), localLB);
    assertEquals(comp1.getUpperBound(2), localUB);
    assertEquals(comp1.isEmpty(), false);
    assertEquals(comp1.isEstimationMode(), estimating);
    assertEquals(comp1.getCurrentBytes(true), localCompBytes);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

    comp2 = shared.compact(true, null);

    assertEquals(comp2.getEstimate(), localEst);
    assertEquals(comp2.getLowerBound(2), localLB);
    assertEquals(comp2.getUpperBound(2), localUB);
    assertEquals(comp2.isEmpty(), false);
    assertEquals(comp2.isEstimationMode(), estimating);
    assertEquals(comp2.getCurrentBytes(true), localCompBytes);
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactOrderedSketch");

    byte[] memArr2 = new byte[localCompBytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);  //allocate mem for compact form

    comp3 = shared.compact(false,  mem2);  //load the mem2

    assertEquals(comp3.getEstimate(), localEst);
    assertEquals(comp3.getLowerBound(2), localLB);
    assertEquals(comp3.getUpperBound(2), localUB);
    assertEquals(comp3.isEmpty(), false);
    assertEquals(comp3.isEstimationMode(), estimating);
    assertEquals(comp3.getCurrentBytes(true), localCompBytes);
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    mem2.clear();
    comp4 = shared.compact(true, mem2);

    assertEquals(comp4.getEstimate(), localEst);
    assertEquals(comp4.getLowerBound(2), localLB);
    assertEquals(comp4.getUpperBound(2), localUB);
    assertEquals(comp4.isEmpty(), false);
    assertEquals(comp4.isEstimationMode(), estimating);
    assertEquals(comp4.getCurrentBytes(true), localCompBytes);
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    comp4.toString(false, true, 0, false);
  }

  @Test
  public void checkHQStoCompactEmptyForms() {
    lgK = 9;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);
    println("lgArr: "+ local.getLgArrLongs());

    //empty
    local.toString(false, true, 0, false);
    boolean estimating = false;
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double localUB  = local.getUpperBound(2);
    //int currentUSBytes = local.getCurrentBytes(false);
    //assertEquals(currentUSBytes, (32*8) + 24);  // clumsy, but a function of RF and TCF
    int compBytes = local.getCurrentBytes(true); //compact form
    assertEquals(compBytes, 8);
    assertEquals(local.isEstimationMode(), estimating);

    byte[] arr2 = new byte[compBytes];
    WritableMemory mem2 = WritableMemory.wrap(arr2);

    CompactSketch csk2 = shared.compact(false,  mem2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), localUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    CompactSketch csk3 = shared.compact(true, mem2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), localEst);
    assertEquals(csk3.getLowerBound(2), localLB);
    assertEquals(csk3.getUpperBound(2), localUB);
    assertEquals(csk3.isEmpty(), true);
    assertEquals(csk3.isEstimationMode(), estimating);
    assertEquals(csk3.getClass().getSimpleName(), "DirectCompactOrderedSketch");
  }

  @Test
  public void checkExactMode() {
    lgK = 12;
    int u = 4096;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertTrue(local.isEmpty());

    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    assertEquals(local.getEstimate(), u, 0.0);
    assertEquals(shared.getRetainedEntries(false), u);
  }

  @Test
  public void checkEstMode() {
    int k = 4096;
    lgK = 12;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertTrue(local.isEmpty());

    int u = 3*k;
    for (int i = 0; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();
    final int retained = shared.getRetainedEntries(false);
    //final int retained = ((UpdateSketch) shared).getRetainedEntries(false);
    assertTrue(retained > k);
    // in general it might be exactly k, but in this case must be greater
  }

  @Test
  public void checkErrorBounds() {
    int k = 512;
    lgK = 12;

    seed = DEFAULT_UPDATE_SEED;
    UpdateSketch local = buildSharedReturnLocalSketch();

    //Exact mode
    int limit = (int)ConcurrentSharedThetaSketch.computeExactLimit(k, 0);
    for (int i = 0; i < limit; i++ ) {
      local.update(i);
    }

    double est = local.getEstimate();
    double lb = local.getLowerBound(2);
    double ub = local.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    int u = 10*k;
    for (int i = limit; i < u; i++ ) {
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

  @Test
  public void checkRebuild() {
    int k = 16;
    lgK = 4;

    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    assertTrue(local.isEmpty());
    int t = ((ConcurrentHeapThetaBuffer)local).getHashTableThreshold();

    for (int i = 0; i< t; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

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

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkBuilder() {
    lgK = 4;

    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilderWithNominal();
    assertEquals(bldr.getLocalLgNominalEntries(), lgK);
    assertEquals(bldr.getLgNominalEntries(), lgK);
    println(bldr.toString());
    bldr.buildLocal(shared);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderSmallLgNominal() {
    lgK = 1;
    seed = DEFAULT_UPDATE_SEED;
    configureBuilder();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBuilderSmallNominal() {
    lgK = 2;
    seed = DEFAULT_UPDATE_SEED;
    configureBuilderWithNominal();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    lgK = 9;
    UpdateSketch qs = buildSharedReturnLocalSketch();
    qs.hashUpdate(-1L);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int k = 512;
    lgK = 9;

    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared();
    UpdateSketch local = bldr.buildLocal(shared);
    //ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

    assertTrue(local.isEmpty());
    int u = 3*k;

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

  @Test
  public void checkDQStoCompactEmptyForms() {
    lgK = 9;

    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);

    //empty
    local.toString(false, true, 0, false); //exercise toString
    assertEquals(local.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    double localEst = local.getEstimate();
    double localLB  = local.getLowerBound(2);
    double uskUB  = local.getUpperBound(2);
    assertFalse(local.isEstimationMode());

    int bytes = local.getCurrentBytes(true); //compact form
    assertEquals(bytes, 8);
    byte[] memArr2 = new byte[bytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);

    CompactSketch csk2 = shared.compact(false,  mem2);
    assertEquals(csk2.getEstimate(), localEst);
    assertEquals(csk2.getLowerBound(2), localLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertTrue(csk2.isEmpty());
    assertFalse(csk2.isEstimationMode());
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    CompactSketch csk3 = shared.compact(true, mem2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), localEst);
    assertEquals(csk3.getLowerBound(2), localLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
    assertTrue(csk3.isEmpty());
    assertFalse(csk3.isEstimationMode());
    assertEquals(csk3.getClass().getSimpleName(), "DirectCompactOrderedSketch");
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMinReqBytes() {
    int k = 16;
    lgK = 4;
    UpdateSketch local = buildSharedReturnLocalSketch();
    for (int i = 0; i < (4 * k); i++) { local.update(i); }
    waitForBgPropagationToComplete();
    byte[] byteArray = shared.toByteArray();
    byte[] badBytes = Arrays.copyOfRange(byteArray, 0, 24);
    Memory mem = Memory.wrap(badBytes);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkThetaAndLgArrLongs() {
    int k = 16;
    lgK = 4;
    UpdateSketch local = buildSharedReturnLocalSketch();
    for (int i = 0; i < k; i++) { local.update(i); }
    waitForBgPropagationToComplete();
    byte[] badArray = shared.toByteArray();
    WritableMemory mem = WritableMemory.wrap(badArray);
    PreambleUtil.insertLgArrLongs(mem, 4);
    PreambleUtil.insertThetaLong(mem, Long.MAX_VALUE / 2);
    Sketch.heapify(mem);
  }

  @Test
  public void checkFamily() {
    UpdateSketch local = buildSharedReturnLocalSketch();
    assertEquals(local.getFamily(), Family.QUICKSELECT);
  }

  @Test
  public void checkBackgroundPropagation() {
    int k = 16;
    lgK = 4;
    int u = 5*k;
    final UpdateSketchBuilder bldr = configureBuilderWithCache();
    //must build shared first
    shared = bldr.buildShared(null);
    UpdateSketch local = bldr.buildLocal(shared);


    assertTrue(local.isEmpty());

    int i = 0;
    for (; i< k; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();
    assertFalse(local.isEmpty());
    assertTrue(local.getEstimate() > 0.0);
    long theta1 = ((ConcurrentHeapQuickSelectSketch)shared).getVolatileTheta();

    for (; i< u; i++) {
      local.update(i);
    }
    waitForBgPropagationToComplete();

    long theta2 = ((ConcurrentHeapQuickSelectSketch)shared).getVolatileTheta();
    int entries = shared.getRetainedEntries(false);
    assertTrue((entries > k) || (theta2 < theta1),
        "entries= " + entries + " k= " + k + " theta1= " + theta1 + " theta2= " + theta2);

    shared.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
    local.rebuild();
    assertEquals(shared.getRetainedEntries(false), k);
    assertEquals(shared.getRetainedEntries(true), k);
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
    UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    UpdateSketch shared = bldr.buildShared();
    UpdateSketch local = bldr.buildLocal(shared);
    local.toByteArray();
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  @AfterMethod
  public void clearShared() {
    shared = null;
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

  private UpdateSketch buildSharedReturnLocalSketch() {
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildShared(null);
    return bldr.buildLocal(shared);
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(lgK);
    bldr.setSeed(seed);
    return bldr;
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilderNotOrdered() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(4);
    bldr.setSeed(seed);
    bldr.setPropagateOrderedCompact(false);
    return bldr;
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilderWithNominal() {
    final UpdateSketchBuilder bldr = configureBuilder();
    int k = 1 << lgK;
    bldr.setLocalNominalEntries(k);
    bldr.setNominalEntries(k);
    assertTrue(bldr.getPropagateOrderedCompact());
    assertEquals(bldr.getSeed(), DEFAULT_UPDATE_SEED);
    return bldr;
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilderWithCache() {
    final UpdateSketchBuilder bldr = configureBuilder();
    return bldr;
  }

  private void waitForBgPropagationToComplete() {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ((ConcurrentHeapQuickSelectSketch)shared).awaitBgPropagationTermination();
    ConcurrentPropagationService.resetExecutorService(Thread.currentThread().getId());
    ((ConcurrentHeapQuickSelectSketch)shared).initBgPropagationService();
  }

}
