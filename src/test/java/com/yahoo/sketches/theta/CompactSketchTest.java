/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class CompactSketchTest {

  @Test
  public void checkHeapifyWrap() {
    int k = 4096;
    checkHeapifyWrap(k, 0); //empty
    checkHeapifyWrap(k, k); //exact
    checkHeapifyWrap(k, 4 * k); //estimating
  }

  //test combinations of compact ordered/not ordered and heap/direct
  public void checkHeapifyWrap(int k, int u) {
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }
    assertFalse(usk.isDirect());
    assertFalse(usk.hasMemory());
    double uskEst = usk.getEstimate();
    assertEquals(uskEst, u, 0.05 * u);
    int uskCount = usk.getRetainedEntries(true);
    short uskSeedHash = usk.getSeedHash();
    long uskThetaLong = usk.getThetaLong();

    CompactSketch csk, csk2, csk3;
    byte[] byteArray;
    boolean ordered = true;
    boolean compact = true;

    /****/
    csk = usk.compact( !ordered, null); //NOT ORDERED
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactUnorderedSketch");
    assertFalse(csk.isDirect());
    assertFalse(csk.hasMemory());
    assertTrue(csk.isCompact());
    assertFalse(csk.isOrdered());

    assertEquals(csk.getFamily(), Family.COMPACT);
    //println("Ord: "+(!ordered)+", Mem: "+"Null");
    //println(csk.toString(true, true, 8, true));
    //println(PreambleUtil.toString(byteArray));

    //put image in memory, check heapify
    Memory srcMem = Memory.wrap(csk.toByteArray());
    csk2 = (CompactSketch) Sketch.heapify(srcMem);
    //println(csk2.toString(true, true, 8, true));
    double csk2est = csk2.getEstimate();
    assertEquals(csk2est, uskEst, 0.0);
    assertEquals(csk2.getRetainedEntries(true), uskCount);
    assertEquals(csk2.getSeedHash(), uskSeedHash);
    assertEquals(csk2.getThetaLong(), uskThetaLong);
    assertNull(csk2.getMemory());
    assertFalse(csk2.isOrdered()); //CHECK NOT ORDERED
    assertNotNull(csk2.getCache());

    /****/
    csk = usk.compact(  ordered, null); //ORDERED
    assertEquals(csk.getClass().getSimpleName(), "HeapCompactOrderedSketch");
    assertFalse(csk.isDirect());
    assertFalse(csk.hasMemory());
    assertTrue(csk.isCompact());
    assertTrue(csk.isOrdered()); //CHECK ORDERED


    Memory srcMem2 = Memory.wrap(csk.toByteArray());
    csk3 = (CompactSketch)Sketch.heapify(srcMem2);
    double csk3est = csk3.getEstimate();
    assertEquals(csk3est, uskEst, 0.0);

    assertEquals(csk3.getRetainedEntries(true), uskCount);
    assertEquals(csk3.getSeedHash(), uskSeedHash);
    assertEquals(csk3.getThetaLong(), uskThetaLong);
    assertNull(csk3.getMemory());
    assertFalse(csk3.isDirect());
    assertFalse(csk3.hasMemory());
    assertTrue(csk3.isOrdered());
    assertNotNull(csk3.getCache());

    /****/
    //Prepare Memory for direct
    int bytes = usk.getCurrentBytes(compact);
    WritableMemory directMem;
    Memory heapROMem;

    try (WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes)) {
      directMem = wdh.get();
      //Memory heapMem;

      /**Via CompactSketch.compact**/
      csk = usk.compact(!ordered, directMem); //NOT ORDERED, DIRECT
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactUnorderedSketch");
      assertTrue(csk.hasMemory());
      assertTrue(csk.isDirect());
      assertTrue(csk.hasMemory());

      csk2 = (CompactSketch)Sketch.wrap(directMem);
      assertEquals(csk2.getEstimate(), uskEst, 0.0);

      assertEquals(csk2.getRetainedEntries(true), uskCount);
      assertEquals(csk2.getSeedHash(), uskSeedHash);
      assertEquals(csk2.getThetaLong(), uskThetaLong);
      assertNotNull(csk2.getMemory());
      assertTrue(csk2.isDirect());
      assertTrue(csk2.hasMemory());
      assertFalse(csk2.isOrdered());
      assertNotNull(csk2.getCache());

      csk = usk.compact( !ordered, directMem);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactUnorderedSketch");
      assertTrue(csk.isDirect());
      assertTrue(csk.hasMemory());
      assertTrue(csk.isCompact());
      assertFalse(csk.isOrdered());

      /**Via byte[]**/
      byteArray = csk.toByteArray();
      heapROMem = Memory.wrap(byteArray);
      csk2 = (CompactSketch)Sketch.wrap(heapROMem);
      assertEquals(csk2.getEstimate(), uskEst, 0.0);

      assertEquals(csk2.getRetainedEntries(true), uskCount);
      assertEquals(csk2.getSeedHash(), uskSeedHash);
      assertEquals(csk2.getThetaLong(), uskThetaLong);
      assertNotNull(csk2.getMemory());
      assertFalse(csk2.isOrdered());
      assertNotNull(csk2.getCache());
      assertFalse(csk2.isDirect());
      assertTrue(csk2.hasMemory());

      /**Via CompactSketch.compact**/
      csk = usk.compact(  ordered, directMem);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
      assertTrue(csk.isDirect());
      assertTrue(csk.hasMemory());
      assertTrue(csk.isCompact());
      assertTrue(csk.isOrdered());

      csk2 = (CompactSketch)Sketch.wrap(directMem);
      assertEquals(csk2.getEstimate(), uskEst, 0.0);

      assertEquals(csk2.getRetainedEntries(true), uskCount);
      assertEquals(csk2.getSeedHash(), uskSeedHash);
      assertEquals(csk2.getThetaLong(), uskThetaLong);
      assertNotNull(csk2.getMemory());
      assertTrue(csk2.isDirect());
      assertTrue(csk2.hasMemory());
      assertTrue(csk2.isOrdered());
      assertNotNull(csk2.getCache());

      /**Via byte[]**/
      csk = usk.compact(  ordered, directMem);
      assertEquals(csk.getClass().getSimpleName(), "DirectCompactOrderedSketch");
      assertTrue(csk2.isDirect());
      assertTrue(csk2.hasMemory());

      byteArray = csk.toByteArray();
      heapROMem = Memory.wrap(byteArray);
      csk2 = (CompactSketch)Sketch.wrap(heapROMem);
      assertEquals(csk2.getEstimate(), uskEst, 0.0);

      assertEquals(csk2.getRetainedEntries(true), uskCount);
      assertEquals(csk2.getSeedHash(), uskSeedHash);
      assertEquals(csk2.getThetaLong(), uskThetaLong);
      assertNotNull(csk2.getMemory());
      assertTrue(csk2.isOrdered());
      assertNotNull(csk2.getCache());
      assertFalse(csk2.isDirect());
      assertTrue(csk2.hasMemory());
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmall() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = false;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    WritableMemory mem = WritableMemory.wrap(byteArray);
    usk.compact(ordered, mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkMemTooSmallOrdered() {
    int k = 512;
    int u = k;
    boolean compact = true;
    boolean ordered = true;
    UpdateSketch usk = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=0; i<u; i++) {
      usk.update(i);
    }

    int bytes = usk.getCurrentBytes(compact);
    byte[] byteArray = new byte[bytes -8]; //too small
    WritableMemory mem = WritableMemory.wrap(byteArray);
    usk.compact(ordered, mem);
  }

  @Test
  public void checkCompactCachePart() {
    //phony values except for curCount = 0.
    long[] result = CompactSketch.compactCachePart(null, 4, 0, 0L, false);
    assertEquals(result.length, 0);
  }

  @Test
  public void checkDirectCompactSingleItemSketch() {
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    CompactSketch csk = sk.compact(true, WritableMemory.allocate(16));
    assertEquals(csk.getCurrentBytes(true), 8);
    sk.update(1);
    csk = sk.compact(true, WritableMemory.allocate(16));
    assertEquals(csk.getCurrentBytes(true), 16);
    assertTrue(csk == csk.compact());
    assertTrue(csk == csk.compact(true, null));
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

}
