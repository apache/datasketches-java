/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

public class UnionImplTest {

  @Test
  public void checkUpdateWithSketch() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    WritableMemory mem2 = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { sketch.update(i); }
    CompactSketch sketchInDirectOrd = sketch.compact(true, mem);
    CompactSketch sketchInDirectUnord = sketch.compact(false, mem2);
    CompactSketch sketchInHeap = sketch.compact(true, null);

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(sketchInDirectOrd);
    union.update(sketchInHeap);
    union.update(sketchInDirectUnord);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptedCompactFlag() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { sketch.update(i); }
    CompactSketch sketchInDirectOrd = sketch.compact(true, mem);
    sketch.compact(false, mem); //corrupt memory
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(sketchInDirectOrd);
  }

  @Test
  public void checkUpdateWithMem() {
    int k = 16;
    WritableMemory skMem = WritableMemory.wrap(new byte[(2*k*8) + 24]);
    WritableMemory dirOrdCskMem = WritableMemory.wrap(new byte[(k*8) + 24]);
    WritableMemory dirUnordCskMem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch udSketch = UpdateSketch.builder().setNominalEntries(k).build(skMem);
    for (int i = 0; i < k; i++) { udSketch.update(i); } //exact
    udSketch.compact(true, dirOrdCskMem);
    udSketch.compact(false, dirUnordCskMem);

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(skMem);
    union.update(dirOrdCskMem);
    union.update(dirUnordCskMem);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkFastWrap() {
    int k = 16;
    long seed = DEFAULT_UPDATE_SEED;
    int unionSize = Sketches.getMaxUnionBytes(k);
    WritableMemory srcMem = WritableMemory.wrap(new byte[unionSize]);
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(srcMem);
    for (int i = 0; i < k; i++) { union.update(i); } //exact
    assertEquals(union.getResult().getEstimate(), k, 0.0);
    Union union2 = UnionImpl.fastWrap(srcMem, seed);
    assertEquals(union2.getResult().getEstimate(), k, 0.0);
    Memory srcMemR = srcMem;
    Union union3 = UnionImpl.fastWrap(srcMemR, seed);
    assertEquals(union3.getResult().getEstimate(), k, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptFamilyException() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    sketch.compact(true, mem);

    mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer1FamilyException() {
    int k = 16;
    WritableMemory v3mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    sketch.compact(true, v3mem);
    WritableMemory v1mem = ForwardCompatibilityTest.convertSerV3toSerV1(v3mem);

    v1mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)2); //corrupt family

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(v1mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer2FamilyException() {
    int k = 16;
    WritableMemory v3mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    sketch.compact(true, v3mem);
    WritableMemory v2mem = ForwardCompatibilityTest.convertSerV3toSerV2(v3mem);

    v2mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)2); //corrupt family

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(v2mem);
  }

  @Test
  public void checkMoveAndResize() {
    int k = 1 << 12;
    int u = 2 * k;
    int bytes = Sketches.getMaxUpdateSketchBytes(k);
    try (WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes/2);
         WritableDirectHandle wdh2 = WritableMemory.allocateDirect(bytes/2) ) {
      WritableMemory wmem = wdh.get();
      UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
      assertTrue(sketch.isSameResource(wmem));

      WritableMemory wmem2 = wdh2.get();
      Union union = SetOperation.builder().buildUnion(wmem2);
      assertTrue(union.isSameResource(wmem2));

      for (int i = 0; i < u; i++) { union.update(i); }
      assertFalse(union.isSameResource(wmem));

      Union union2 = SetOperation.builder().buildUnion(); //on-heap union
      assertFalse(union2.isSameResource(wmem2));  //obviously not
    }
  }

  @Test
  public void checkRestricted() {
    Union union = Sketches.setOperationBuilder().buildUnion();
    assertTrue(union.isEmpty());
    assertEquals(union.getThetaLong(), Long.MAX_VALUE);
    assertEquals(union.getSeedHash(), Util.computeSeedHash(DEFAULT_UPDATE_SEED));
    assertEquals(union.getRetainedEntries(true), 0);
    assertEquals(union.getCache().length, 128);
  }

  @Test
  public void checkUnionCompactOrderedSource() {
    int k = 1 << 12;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < k; i++) { sk.update(i); }
    double est1 = sk.getEstimate();

    int bytes = Sketches.getMaxCompactSketchBytes(sk.getRetainedEntries());
    try (WritableDirectHandle h = WritableMemory.allocateDirect(bytes)) {
      WritableMemory wmem = h.get();
      CompactSketch csk = sk.compact(true, wmem); //ordered, direct
      Union union = Sketches.setOperationBuilder().buildUnion();
      union.update(csk);
      double est2 = union.getResult().getEstimate();
      assertEquals(est2, est1);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCompactFlagCorruption() {
    int k = 1 << 12;
    int bytes = Sketch.getMaxUpdateSketchBytes(k);
    WritableMemory wmem1 = WritableMemory.allocate(bytes);
    UpdateSketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem1);
    for (int i = 0; i < k; i++) { sk.update(i); }
    sk.compact(true, wmem1); //corrupt the wmem1 to be a compact sketch

    Union union = SetOperation.builder().buildUnion();
    union.update(sk); //update the union with the UpdateSketch object
    CompactSketch csk1 = union.getResult();
    println(""+csk1.getEstimate());
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
