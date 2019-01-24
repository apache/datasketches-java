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
  private volatile ConcurrentSharedThetaSketch shared;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      sk1.update(i);
    }
    waitForPropagationToComplete();

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), u);

    byte[] byteArray = usk.toByteArray();
    WritableMemory mem = WritableMemory.wrap(byteArray);
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
    assertFalse(bldr.getCacheLimit() == 0);
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer) usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i < u; i++) {
      sk1.update(i);
    }
    waitForPropagationToComplete();

    assertFalse(usk.isEmpty());
    assertTrue(((UpdateSketch)shared).getRetainedEntries(false) <= u);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);
    //ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks
    assertTrue(usk.isEmpty());
    assertTrue(shared instanceof ConcurrentHeapQuickSelectSketch);
    for (int i = 0; i< u; i++) {
      usk.update(i);
    }
    assertTrue(((UpdateSketch)shared).compact().isCompact());

    assertFalse(usk.isEmpty());
    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), u);
    byte[] byteArray = usk.toByteArray();
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
    UpdateSketch usk = buildConcSketch();
    byte[] byteArray = usk.toByteArray();
    Memory srcMem = Memory.wrap(byteArray);
    Sketch.heapify(srcMem, seed2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifyCorruptLgNomLongs() {
    lgK = 4;
    UpdateSketch usk = buildConcSketch();
    WritableMemory srcMem = WritableMemory.wrap(usk.toByteArray());
    srcMem.putByte(LG_NOM_LONGS_BYTE, (byte)2); //corrupt
    Sketch.heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    lgK = 9;
    int u = k;
    seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = buildConcSketch();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }
    waitForPropagationToComplete();

    int bytes = usk.getCurrentBytes(false);
    byte[] byteArray = usk.toByteArray();
    assertEquals(bytes, byteArray.length);

    Memory srcMem = Memory.wrap(byteArray);
    UpdateSketch usk2 = Sketches.heapifyUpdateSketch(srcMem, seed);
    assertEquals(usk2.getEstimate(), u, 0.0);
    assertEquals(usk2.getLowerBound(2), u, 0.0);
    assertEquals(usk2.getUpperBound(2), u, 0.0);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), false);
    assertEquals(usk2.getResizeFactor(), usk.getResizeFactor());
    usk2.toString(true, true, 8, true);
  }

  @Test
  public void checkHeapifyByteArrayEstimating() {
    int k = 4096;
    lgK = 12;
    int u = 2*k;
    seed = DEFAULT_UPDATE_SEED;

    UpdateSketch usk = buildConcSketch();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }
    waitForPropagationToComplete();

    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), true);
    byte[] byteArray = usk.toByteArray();

    Memory srcMem = Memory.wrap(byteArray);
    UpdateSketch usk2 = UpdateSketch.heapify(srcMem, seed);
    assertEquals(usk2.getEstimate(), uskEst);
    assertEquals(usk2.getLowerBound(2), uskLB);
    assertEquals(usk2.getUpperBound(2), uskUB);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), true);
    assertEquals(usk2.getResizeFactor(), usk.getResizeFactor());
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    lgK = 9;
    int u = 2*k;
    seed = DEFAULT_UPDATE_SEED;
    boolean estimating = (u > k);
    UpdateSketch sk1 = buildConcSketch();

    for (int i=0; i<u; i++) {
      sk1.update(i);
    }
    waitForPropagationToComplete();

    double sk1est = sk1.getEstimate();
    double sk1lb  = sk1.getLowerBound(2);
    double sk1ub  = sk1.getUpperBound(2);
    assertEquals(sk1.isEstimationMode(), estimating);
    assertFalse(sk1.isDirect());

    byte[] byteArray = sk1.toByteArray();
    Memory mem = Memory.wrap(byteArray);

    UpdateSketch sk2 = UpdateSketch.heapify(mem, DEFAULT_UPDATE_SEED);

    assertEquals(sk2.getEstimate(), sk1est);
    assertEquals(sk2.getLowerBound(2), sk1lb);
    assertEquals(sk2.getUpperBound(2), sk1ub);
    assertEquals(sk2.isEmpty(), false);
    assertEquals(sk2.isEstimationMode(), estimating);
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
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    assertEquals(usk.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    assertFalse(usk.isDirect());

    for (int i=0; i<u; i++) {
      usk.update(i);
    }
    if(shared.isPropagationInProgress()) {
      waitForPropagationToComplete();
    }

    ((UpdateSketch)shared).rebuild(); //forces size back to k

    //get baseline values
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    int uskBytes = usk.getCurrentBytes(false);    //size stored as UpdateSketch
    int uskCompBytes = usk.getCurrentBytes(true); //size stored as CompactSketch
    assertEquals(uskBytes, maxBytes);
    assertEquals(usk.isEstimationMode(), estimating);

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = ((UpdateSketch)shared).compact(false,  null);

    assertEquals(comp1.getEstimate(), uskEst);
    assertEquals(comp1.getLowerBound(2), uskLB);
    assertEquals(comp1.getUpperBound(2), uskUB);
    assertEquals(comp1.isEmpty(), false);
    assertEquals(comp1.isEstimationMode(), estimating);
    assertEquals(comp1.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

    comp2 = ((UpdateSketch)shared).compact(true, null);

    assertEquals(comp2.getEstimate(), uskEst);
    assertEquals(comp2.getLowerBound(2), uskLB);
    assertEquals(comp2.getUpperBound(2), uskUB);
    assertEquals(comp2.isEmpty(), false);
    assertEquals(comp2.isEstimationMode(), estimating);
    assertEquals(comp2.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactOrderedSketch");

    byte[] memArr2 = new byte[uskCompBytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);  //allocate mem for compact form

    comp3 = ((UpdateSketch)shared).compact(false,  mem2);  //load the mem2

    assertEquals(comp3.getEstimate(), uskEst);
    assertEquals(comp3.getLowerBound(2), uskLB);
    assertEquals(comp3.getUpperBound(2), uskUB);
    assertEquals(comp3.isEmpty(), false);
    assertEquals(comp3.isEstimationMode(), estimating);
    assertEquals(comp3.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    mem2.clear();
    comp4 = ((UpdateSketch)shared).compact(true, mem2);

    assertEquals(comp4.getEstimate(), uskEst);
    assertEquals(comp4.getLowerBound(2), uskLB);
    assertEquals(comp4.getUpperBound(2), uskUB);
    assertEquals(comp4.isEmpty(), false);
    assertEquals(comp4.isEstimationMode(), estimating);
    assertEquals(comp4.getCurrentBytes(true), uskCompBytes);
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactOrderedSketch");
    comp4.toString(false, true, 0, false);
  }

  @Test
  public void checkHQStoCompactEmptyForms() {
    lgK = 9;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);
    println("lgArr: "+ usk.getLgArrLongs());


    //empty
    usk.toString(false, true, 0, false);
    boolean estimating = false;
    assertEquals(usk.getClass().getSimpleName(), "ConcurrentHeapThetaBuffer");
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    //int currentUSBytes = usk.getCurrentBytes(false);
    //assertEquals(currentUSBytes, (32*8) + 24);  // clumsy, but a function of RF and TCF
    int compBytes = usk.getCurrentBytes(true); //compact form
    assertEquals(compBytes, 8);
    assertEquals(usk.isEstimationMode(), estimating);

    byte[] arr2 = new byte[compBytes];
    WritableMemory mem2 = WritableMemory.wrap(arr2);

    CompactSketch csk2 = ((UpdateSketch)shared).compact(false,  mem2);
    assertEquals(csk2.getEstimate(), uskEst);
    assertEquals(csk2.getLowerBound(2), uskLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    CompactSketch csk3 = ((UpdateSketch)shared).compact(true, mem2);
    csk3.toString(false, true, 0, false);
    csk3.toString();
    assertEquals(csk3.getEstimate(), uskEst);
    assertEquals(csk3.getLowerBound(2), uskLB);
    assertEquals(csk3.getUpperBound(2), uskUB);
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
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }
    waitForPropagationToComplete();

    assertEquals(usk.getEstimate(), u, 0.0);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), u);
  }

  @Test
  public void checkEstMode() {
    int k = 4096;
    lgK = 12;
    int u = 2*k;
    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }
    waitForPropagationToComplete();

    assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k); // in general it might be exactly k, but
    // in this
    // case must be greater
  }

  @Test
  public void checkErrorBounds() {
    int k = 512;
    lgK = 12;

    seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = buildConcSketch();

    //Exact mode
    int limit = (int)ConcurrentSharedThetaSketch.getLimit(k);
    for (int i = 0; i < limit; i++ ) {
      usk.update(i);
    }

    double est = usk.getEstimate();
    double lb = usk.getLowerBound(2);
    double ub = usk.getUpperBound(2);
    assertEquals(est, ub, 0.0);
    assertEquals(est, lb, 0.0);

    //Est mode
    int u = 10*k;
    for (int i = limit; i < u; i++ ) {
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

  @Test
  public void checkRebuild() {
    int k = 16;
    lgK = 4;
    int u = 4*k;

    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) {
      usk.update(i);
    }
    waitForPropagationToComplete();

    assertFalse(usk.isEmpty());
    assertTrue(usk.getEstimate() > 0.0);
    assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);

    ((UpdateSketch)shared).rebuild();
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
    ((UpdateSketch)shared).rebuild();
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
  }

  @Test(expectedExceptions = SketchesStateException.class)
  public void checkBuilder() {
    lgK = 4;

    seed = DEFAULT_UPDATE_SEED;
    final UpdateSketchBuilder bldr = configureBuilderWithNominal();
    assertEquals(bldr.getLocalLgNominalEntries(), lgK);
    assertEquals(bldr.getLgNominalEntries(), lgK);
    println(bldr.toString());
    bldr.buildLocalInternal(shared);
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
    UpdateSketch qs = buildConcSketch();
    qs.hashUpdate(-1L);
  }

  @Test
  public void checkResetAndStartingSubMultiple() {
    int k = 512;
    lgK = 9;
    int u = 4*k;

    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    UpdateSketch usk = bldr.buildLocalInternal(shared);
    ConcurrentHeapThetaBuffer sk1 = (ConcurrentHeapThetaBuffer)usk; //for internal checks

    assertTrue(usk.isEmpty());

    for (int i = 0; i< u; i++) { usk.update(i); }
    waitForPropagationToComplete();

    assertFalse(usk.isEmpty());
    assertTrue(((UpdateSketch)shared).getRetainedEntries(false) > k);
    assertTrue(sk1.getThetaLong() < Long.MAX_VALUE);

    shared.resetShared();
    sk1.reset();
    assertTrue(usk.isEmpty());
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), 0);
    assertEquals(usk.getEstimate(), 0.0, 0.0);
    assertEquals(sk1.getThetaLong(), Long.MAX_VALUE);
  }

  @Test
  public void checkDQStoCompactEmptyForms() {
    lgK = 9;

    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
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

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMinReqBytes() {
    int k = 16;
    lgK = 4;
    UpdateSketch s1 = buildConcSketch();
    for (int i = 0; i < (4 * k); i++) { s1.update(i); }
    waitForPropagationToComplete();
    byte[] byteArray = s1.toByteArray();
    byte[] badBytes = Arrays.copyOfRange(byteArray, 0, 24);
    Memory mem = Memory.wrap(badBytes);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkThetaAndLgArrLongs() {
    int k = 16;
    lgK = 4;
    UpdateSketch s1 = buildConcSketch();
    for (int i = 0; i < k; i++) { s1.update(i); }
    waitForPropagationToComplete();
    byte[] badArray = s1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(badArray);
    PreambleUtil.insertLgArrLongs(mem, 4);
    PreambleUtil.insertThetaLong(mem, Long.MAX_VALUE / 2);
    Sketch.heapify(mem);
  }

  @Test
  public void checkFamily() {
    UpdateSketch sketch = buildConcSketch();
    assertEquals(sketch.getFamily(), Family.QUICKSELECT);
  }

  @Test
  public void checkBackgroundPropagation() {
    int k = 16;
    lgK = 4;
    int u = 5*k;
    final UpdateSketchBuilder bldr = configureBuilderWithCache();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
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
    int entries = ((UpdateSketch)shared).getRetainedEntries(false);
    assertTrue((entries > k) || (theta2 < theta1),"entries="+entries+" k="+k+" theta1="+theta1+" theta2="+theta2);

    ((UpdateSketch)shared).rebuild();
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
    sk1.rebuild();
    assertEquals(((UpdateSketch)shared).getRetainedEntries(false), k);
    assertEquals(((UpdateSketch)shared).getRetainedEntries(true), k);
  }

  @SuppressWarnings("unused")
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

  private UpdateSketch buildConcSketch() {
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    shared = bldr.buildSharedInternal(null);
    assertFalse(shared.isPropagationInProgress());
    return bldr.buildLocalInternal(shared);
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setSharedLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(lgK);
    bldr.setSeed(seed);
    return bldr;
  }

  //configures builder for both local and shared
  private UpdateSketchBuilder configureBuilderNotOrdered() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setSharedLogNominalEntries(lgK);
    bldr.setLocalLogNominalEntries(4);
    bldr.setSeed(seed);
    bldr.setPropagateOrderedCompact(false);
    bldr.setCacheLimit(16);
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
    int k = 1 << lgK;
    bldr.setCacheLimit(k);
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
