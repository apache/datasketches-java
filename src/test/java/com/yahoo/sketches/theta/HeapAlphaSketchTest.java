/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.ALPHA;
import static com.yahoo.sketches.ResizeFactor.X1;
import static com.yahoo.sketches.ResizeFactor.X2;
import static com.yahoo.sketches.ResizeFactor.X8;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgResizeFactor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class HeapAlphaSketchTest {
  private Family fam_ = ALPHA;

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
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
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(SER_VER_BYTE, (byte) 0); //corrupt the SerVer byte

    Sketch.heapify(mem, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkConstructorKtooSmall() {
    int k = 256;
    UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkAlphaIncompatibleWithMem() {
    WritableMemory mem = WritableMemory.wrap(new byte[(512*16)+24]);
    UpdateSketch.builder().setFamily(Family.ALPHA).setNominalEntries(512).build(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkIllegalSketchID_UpdateSketch() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
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
    WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(FAMILY_BYTE, (byte) 0); //corrupt the Sketch ID byte

    //try to heapify the corruped mem
    Sketch.heapify(mem, seed);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkHeapifySeedConflict() {
    int k = 512;
    long seed1 = 1021;
    long seed2 = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed1)
        .setNominalEntries(k).build();
    byte[] byteArray = usk.toByteArray();
    Memory srcMem = Memory.wrap(byteArray);
    Sketch.heapify(srcMem, seed2);
  }

  @Test
  public void checkHeapifyByteArrayExact() {
    int k = 512;
    int u = k;
    long seed = DEFAULT_UPDATE_SEED;
    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes(false);
    byte[] byteArray = usk.toByteArray();
    assertEquals(bytes, byteArray.length);

    Memory srcMem = Memory.wrap(byteArray);
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcMem, seed);
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
    long seed = DEFAULT_UPDATE_SEED;

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

    Memory srcMem = Memory.wrap(byteArray);
    UpdateSketch usk2 = (UpdateSketch)Sketch.heapify(srcMem, seed);
    assertEquals(usk2.getEstimate(), uskEst);
    assertEquals(usk2.getLowerBound(2), uskLB);
    assertEquals(usk2.getUpperBound(2), uskUB);
    assertEquals(usk2.isEmpty(), false);
    assertEquals(usk2.isEstimationMode(), true);
    assertEquals(usk2.getClass().getSimpleName(), usk.getClass().getSimpleName());
  }

  @Test
  public void checkHeapifyMemoryEstimating() {
    int k = 512;
    int u = 2*k;
    long seed = DEFAULT_UPDATE_SEED;
    boolean estimating = (u > k);
    //int maxBytes = (k << 4) + (Family.ALPHA.getLowPreLongs());

    UpdateSketch sk1 = UpdateSketch.builder().setFamily(fam_).setSeed(seed)
        .setNominalEntries(k).build();

    for (int i=0; i<u; i++) {
      sk1.update(i);
    }

    double sk1est = sk1.getEstimate();
    double sk1lb  = sk1.getLowerBound(2);
    double sk1ub  = sk1.getUpperBound(2);
    assertEquals(sk1.isEstimationMode(), estimating);

    byte[] byteArray = sk1.toByteArray();
    Memory mem = Memory.wrap(byteArray);

    UpdateSketch sk2 = (UpdateSketch)Sketch.heapify(mem, DEFAULT_UPDATE_SEED);

    assertEquals(sk2.getEstimate(), sk1est);
    assertEquals(sk2.getLowerBound(2), sk1lb);
    assertEquals(sk2.getUpperBound(2), sk1ub);
    assertEquals(sk2.isEmpty(), false);
    assertEquals(sk2.isEstimationMode(), estimating);
    assertEquals(sk2.getClass().getSimpleName(), sk1.getClass().getSimpleName());
  }

  @Test
  public void checkAlphaToCompactForms() {
    int k = 512;
    int u = 4*k;
    boolean estimating = (u > k);

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();
    HeapAlphaSketch sk1 = (HeapAlphaSketch)usk; //for internal checks

    assertEquals(usk.getClass().getSimpleName(), "HeapAlphaSketch");
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    sk1.rebuild(); //removes any dirty values

    //Alpha is more accurate, and size is a statistical variable about k
    // so cannot be directly compared to the compact forms
    assertEquals(usk.isEstimationMode(), estimating);

    CompactSketch comp1, comp2, comp3, comp4;

    comp1 = usk.compact(false, null);

    //But we can compare the compact forms to each other
    double comp1est = comp1.getEstimate();
    double comp1lb  = comp1.getLowerBound(2);
    double comp1ub  = comp1.getUpperBound(2);
    int comp1bytes = comp1.getCurrentBytes(true);
    assertEquals(comp1bytes, comp1.getCurrentBytes(false));
    int comp1curCount = comp1.getRetainedEntries(true); //flag is not relevant
    assertEquals(comp1bytes, (comp1curCount << 3) + (Family.COMPACT.getMaxPreLongs() << 3));

    assertEquals(comp1.isEmpty(), false);
    assertEquals(comp1.isEstimationMode(), estimating);
    assertEquals(comp1.getClass().getSimpleName(), "HeapCompactUnorderedSketch");

    comp2 = usk.compact(true,  null);

    assertEquals(comp2.getEstimate(), comp1est);
    assertEquals(comp2.getLowerBound(2), comp1lb);
    assertEquals(comp2.getUpperBound(2), comp1ub);
    assertEquals(comp2.isEmpty(), false);
    assertEquals(comp2.isEstimationMode(), estimating);
    assertEquals(comp1bytes, comp2.getCurrentBytes(true)); //flag is not relevant
    assertEquals(comp1curCount, comp2.getRetainedEntries(true)); //flag is not relevant
    assertEquals(comp2.getClass().getSimpleName(), "HeapCompactOrderedSketch");

    int bytes = usk.getCurrentBytes(true);
    int alphaBytes = sk1.getRetainedEntries(true) * 8;
    assertEquals(bytes, alphaBytes + (Family.COMPACT.getMaxPreLongs() << 3));
    byte[] memArr2 = new byte[bytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);

    comp3 = usk.compact(false, mem2);

    assertEquals(comp3.getEstimate(), comp1est);
    assertEquals(comp3.getLowerBound(2), comp1lb);
    assertEquals(comp3.getUpperBound(2), comp1ub);
    assertEquals(comp3.isEmpty(), false);
    assertEquals(comp3.isEstimationMode(), estimating);
    assertEquals(comp1bytes, comp3.getCurrentBytes(true)); //flag is not relevant
    assertEquals(comp1curCount, comp3.getRetainedEntries(true)); //flag is not relevant
    assertEquals(comp3.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    mem2.clear();
    comp4 = usk.compact(true, mem2);

    assertEquals(comp4.getEstimate(), comp1est);
    assertEquals(comp4.getLowerBound(2), comp1lb);
    assertEquals(comp4.getUpperBound(2), comp1ub);
    assertEquals(comp4.isEmpty(), false);
    assertEquals(comp4.isEstimationMode(), estimating);
    assertEquals(comp1bytes, comp4.getCurrentBytes(true)); //flag is not relevant
    assertEquals(comp1curCount, comp4.getRetainedEntries(true)); //flag is not relevant
    assertEquals(comp4.getClass().getSimpleName(), "DirectCompactOrderedSketch");
  }

  @Test
  public void checkAlphaToCompactEmptyForms() {
    int k = 512;

    UpdateSketch usk = UpdateSketch.builder().setFamily(fam_).setNominalEntries(k).build();

    //empty
    usk.toString(false, true, 0, false);
    boolean estimating = false;
    assertEquals(usk.getClass().getSimpleName(), "HeapAlphaSketch");
    double uskEst = usk.getEstimate();
    double uskLB  = usk.getLowerBound(2);
    double uskUB  = usk.getUpperBound(2);
    assertEquals(usk.isEstimationMode(), estimating);

    int bytes = usk.getCurrentBytes(true);
    assertEquals(bytes, 8); //compact, empty and theta = 1.0
    byte[] memArr2 = new byte[bytes];
    WritableMemory mem2 = WritableMemory.wrap(memArr2);

    CompactSketch csk2 = usk.compact(false,  mem2);
    assertEquals(csk2.getEstimate(), uskEst);
    assertEquals(csk2.getLowerBound(2), uskLB);
    assertEquals(csk2.getUpperBound(2), uskUB);
    assertEquals(csk2.isEmpty(), true);
    assertEquals(csk2.isEstimationMode(), estimating);
    assertEquals(csk2.getClass().getSimpleName(), "DirectCompactUnorderedSketch");

    CompactSketch csk3 = usk.compact(true, mem2);
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
    int subMul = Util.startingSubMultiple(11, rf, 5); //messy
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
    subMul = Util.startingSubMultiple(11, rf, 5); //messy
    assertEquals(sk1.getLgArrLongs(), subMul);

    assertNull(sk1.getMemory());
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
    WritableMemory mem = WritableMemory.wrap(byteArray);
    //corrupt:
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 4);
    Sketch.heapify(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeHashes() {
    int k = 512;
    UpdateSketch alpha = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    alpha.hashUpdate(-1L);
  }

  @Test
  public void checkMemDeSerExceptions() {
    int k = 1024;
    UpdateSketch sk1 = UpdateSketch.builder().setFamily(ALPHA).setNominalEntries(k).build();
    sk1.update(1L); //forces preLongs to 3
    byte[] bytearray1 = sk1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(bytearray1);
    long pre0 = mem.getLong(0);

    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt PreLongs
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt SerVer
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, FAMILY_BYTE, 2); //Corrupt Family
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, FLAGS_BYTE, 2); //Corrupt READ_ONLY to true
    mem.putLong(0, pre0); //restore

    final long origThetaLong = mem.getLong(THETA_LONG);
    try {
      mem.putLong(THETA_LONG, Long.MAX_VALUE / 2); //Corrupt the theta value
      HeapAlphaSketch.heapifyInstance(mem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
    mem.putLong(THETA_LONG, origThetaLong); //restore theta
    byte[] byteArray2 = new byte[bytearray1.length -1];
    WritableMemory mem2 = WritableMemory.wrap(byteArray2);
    mem.copyTo(0, mem2, 0, mem2.getCapacity());
    try {
      HeapAlphaSketch.heapifyInstance(mem2, DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }

    // force ResizeFactor.X1, but allocated capacity too small
    insertLgResizeFactor(mem, ResizeFactor.X1.lg());
    try {
      HeapAlphaSketch.heapifyInstance(mem, DEFAULT_UPDATE_SEED);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  private static void tryBadMem(WritableMemory mem, int byteOffset, int byteValue) {
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      HeapAlphaSketch.heapifyInstance(mem, DEFAULT_UPDATE_SEED);
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
    WritableMemory wmem = WritableMemory.wrap(byteArr);
    wmem.putByte(LG_NOM_LONGS_BYTE, (byte) 8); //corrupt LgNomLongs
    UpdateSketch sk = Sketches.heapifyUpdateSketch(wmem);
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
