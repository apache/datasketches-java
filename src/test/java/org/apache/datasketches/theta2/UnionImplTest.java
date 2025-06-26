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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.isSameResource;
import static org.apache.datasketches.theta2.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta2.BackwardConversions.convertSerVer3toSerVer2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.nio.ByteOrder;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon2.ThetaUtil;
import org.testng.annotations.Test;

public class UnionImplTest {

  @Test
  public void checkGetCurrentAndMaxBytes() {
    final int lgK = 10;
    final Union union = Sketches.setOperationBuilder().setLogNominalEntries(lgK).buildUnion();
    assertEquals(union.getCurrentBytes(), 288);
    assertEquals(union.getMaxUnionBytes(), 16416);
  }

  @Test
  public void checkUpdateWithSketch() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*8 + 24]);
    final MemorySegment seg2 = MemorySegment.ofArray(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { sketch.update(i); }
    final CompactSketch sketchInDirectOrd = sketch.compact(true, seg);
    final CompactSketch sketchInDirectUnord = sketch.compact(false, seg2);
    final CompactSketch sketchInHeap = sketch.compact(true, null);

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(sketchInDirectOrd);
    union.union(sketchInHeap);
    union.union(sketchInDirectUnord);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkUnorderedAndOrderedMemorySegment() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < k; i++) { sketch.update(i); }
    final CompactSketch sketchInDirectOrd = sketch.compact(false, seg);
    assertFalse(sketchInDirectOrd.isOrdered());
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(sketchInDirectOrd);
    final double est1 = union.getResult().getEstimate();
    sketch.compact(true, seg); //change the order as a side effect
    assertTrue(sketchInDirectOrd.isOrdered());
    union.union(sketchInDirectOrd);
    final double est2 = union.getResult().getEstimate();
    assertEquals(est1, est2);
    assertEquals((int)est1, k);
  }

  @Test
  public void checkUpdateWithSeg() {
    final int k = 16;
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[2*k*8 + 24]);
    final MemorySegment dirOrdCskSeg = MemorySegment.ofArray(new byte[k*8 + 24]);
    final MemorySegment dirUnordCskSeg = MemorySegment.ofArray(new byte[k*8 + 24]);
    final UpdateSketch udSketch = UpdateSketch.builder().setNominalEntries(k).build(skSeg);
    for (int i = 0; i < k; i++) { udSketch.update(i); } //exact
    udSketch.compact(true, dirOrdCskSeg);
    udSketch.compact(false, dirUnordCskSeg);

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(skSeg);
    union.union(dirOrdCskSeg);
    union.union(dirUnordCskSeg);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkUpdateWithSegV4Exact() {
    int n = 1000;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      sk.update(i);
    }
    CompactSketch cs = sk.compact();
    assertFalse(cs.isEstimationMode());

    byte[] bytes = cs.toByteArrayCompressed();

    final Union union = Sketches.setOperationBuilder().buildUnion();
    union.union(MemorySegment.ofArray(bytes));
    assertEquals(union.getResult().getEstimate(), n, 0.0);
  }

  @Test
  public void checkFastWrap() {
    final int k = 16;
    final long seed = ThetaUtil.DEFAULT_UPDATE_SEED;
    final int unionSize = Sketches.getMaxUnionBytes(k);
    final MemorySegment srcSeg = MemorySegment.ofArray(new byte[unionSize]);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(srcSeg);
    for (int i = 0; i < k; i++) { union.update(i); } //exact
    assertEquals(union.getResult().getEstimate(), k, 0.0);
    final Union union2 = UnionImpl.fastWrap(srcSeg, seed);
    assertEquals(union2.getResult().getEstimate(), k, 0.0);
    final MemorySegment srcSegR = srcSeg;
    final Union union3 = UnionImpl.fastWrap(srcSegR, seed);
    assertEquals(union3.getResult().getEstimate(), k, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptFamilyException() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    sketch.compact(true, seg);

    seg.set(JAVA_BYTE, PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer2FamilyException() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    final CompactSketch csk = sketch.compact(true, null);
    final MemorySegment v2seg = convertSerVer3toSerVer2(csk, ThetaUtil.DEFAULT_UPDATE_SEED);

    v2seg.set(JAVA_BYTE, PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(v2seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer1FamilyException() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    final CompactSketch csk = sketch.compact(true, null);
    final MemorySegment v1seg = convertSerVer3toSerVer1(csk);

    v1seg.set(JAVA_BYTE, PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(v1seg);
  }

  @Test
  public void checkVer2EmptyHandling() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    final MemorySegment seg = convertSerVer3toSerVer2(sketch.compact(), ThetaUtil.DEFAULT_UPDATE_SEED);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(seg);
  }

  @Test
  public void checkMoveAndResizeOffHeap() {
    final int k = 1 << 12;
    final int u = 2 * k;
    final int bytes = Sketches.getMaxUpdateSketchBytes(k);
    MemorySegment skWseg, uWseg;
    try (Arena arena = Arena.ofConfined()) {
      skWseg = arena.allocate(bytes / 2);
      final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(skWseg);
      assertTrue(sketch.isSameResource(skWseg)); //of course

      uWseg = arena.allocate(bytes / 2); //independent of sketch, way too small, forces overflow
      final Union union = SetOperation.builder().buildUnion(uWseg); //off-heap union
      assertTrue(union.isSameResource(uWseg)); //of course

      for (int i = 0; i < u; i++) { union.update(i); } //populate first union
      assertFalse(union.isSameResource(skWseg)); //of course
      assertFalse(union.isSameResource(uWseg)); //moved

      final MemorySegment uWsegHeap = MemorySegment.ofArray(new byte[bytes / 2]);
      final Union union2 = SetOperation.builder().buildUnion(); //on-heap union
      assertFalse(isSameResource(uWseg, uWsegHeap));  //obviously not
      assertFalse(isSameResource(uWsegHeap, union.getMemorySegment())); //tgt moved
    }
    assertFalse(skWseg.scope().isAlive()); //closed as part of the same Arena
    assertFalse(uWseg.scope().isAlive()); //closed as part of the same Arena


  }

  @Test
  public void checkRestricted() {
    final Union union = Sketches.setOperationBuilder().buildUnion();
    assertTrue(union.isEmpty());
    assertEquals(union.getThetaLong(), Long.MAX_VALUE);
    assertEquals(union.getSeedHash(), ThetaUtil.computeSeedHash(ThetaUtil.DEFAULT_UPDATE_SEED));
    assertEquals(union.getRetainedEntries(), 0);
    assertEquals(union.getCache().length, 128); //only applies to stateful
  }

  @Test
  public void checkUnionCompactOrderedSource() {
    final int lgK = 12;
    final int k = 1 << lgK;
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < k; i++) { sk.update(i); }
    final double est1 = sk.getEstimate();

    final int bytes = Sketches.getCompactSketchMaxBytes(lgK);
    try (Arena arena = Arena.ofConfined()) {
        MemorySegment wseg = arena.allocate(bytes);

      final CompactSketch csk = sk.compact(true, wseg); //ordered, direct
      final Union union = Sketches.setOperationBuilder().buildUnion();
      union.union(csk);
      final double est2 = union.getResult().getEstimate();
      assertEquals(est2, est1);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCompactFlagCorruption() {
    final int k = 1 << 12;
    final int bytes = Sketch.getMaxUpdateSketchBytes(k);
    final MemorySegment wseg1 = MemorySegment.ofArray(new byte[bytes]);
    final UpdateSketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build(wseg1);
    for (int i = 0; i < k; i++) { sk.update(i); }
    sk.compact(true, wseg1); //corrupt the wseg1 to be a compact sketch

    final Union union = SetOperation.builder().buildUnion();
    union.union(sk); //update the union with the UpdateSketch object
    final CompactSketch csk1 = union.getResult();
    println(""+csk1.getEstimate());
  }

  @Test //checks for bug introduced in 1.0.0-incubating.
  public void checkDirectUnionSingleItem() {
    final int num = 2;
    final UpdateSketch[] skArr = new UpdateSketch[num];
    for (int i = 0; i < num; i++) {
      skArr[i] = new UpdateSketchBuilder().build();
    }
    for (int i = 0; i < num/2; i++) {
      skArr[i].update(i);
      skArr[i + num/2].update(i);
      skArr[i].update(i + num);
    }

    Union union = new SetOperationBuilder().buildUnion();
    for (int i = 0; i < num; i++) {
      union.union(skArr[i]);
    }

    CompactSketch csk = union.getResult();
    assertEquals(csk.getEstimate(), 2.0);
    //println(csk.toString(true, true, 1, true));

    final MemorySegment[] segArr = new MemorySegment[num];
    for (int i = 0; i < num; i++) {
      segArr[i] = MemorySegment.ofArray(skArr[i].compact().toByteArray());
    }
    union = new SetOperationBuilder().buildUnion();
    for (int i = 0; i < num; i++) {
      union.union(segArr[i]);
    }

    csk = union.getResult();
    assertEquals(csk.getEstimate(), 2.0);
    //println(csk.toString(true, true, 1, true));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param o value to print
   */
  static void println(final Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
