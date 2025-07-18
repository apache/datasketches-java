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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.Arena;
import java.nio.ByteOrder;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.PreambleUtil;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UnionImpl;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
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
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*8) + 24]);
    final MemorySegment seg2 = MemorySegment.ofArray(new byte[(k*8) + 24]);
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
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*8) + 24]);
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
    final MemorySegment skSeg = MemorySegment.ofArray(new byte[(2*k*8) + 24]);
    final MemorySegment dirOrdCskSeg = MemorySegment.ofArray(new byte[(k*8) + 24]);
    final MemorySegment dirUnordCskSeg = MemorySegment.ofArray(new byte[(k*8) + 24]);
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
    final int n = 1000;
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < n; i++) {
      sk.update(i);
    }
    final CompactSketch cs = sk.compact();
    assertFalse(cs.isEstimationMode());

    final byte[] bytes = cs.toByteArrayCompressed();

    final Union union = Sketches.setOperationBuilder().buildUnion();
    union.union(MemorySegment.ofArray(bytes));
    assertEquals(union.getResult().getEstimate(), n, 0.0);
  }

  @Test
  public void checkFastWrap() {
    final int k = 16;
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final int unionSize = Sketches.getMaxUnionBytes(k);
    final MemorySegment srcSeg = MemorySegment.ofArray(new byte[unionSize]);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(srcSeg);
    for (int i = 0; i < k; i++) { union.update(i); } //exact
    assertEquals(union.getResult().getEstimate(), k, 0.0);
    final Union union2 = UnionImpl.fastWrapInstance(srcSeg, seed);
    assertEquals(union2.getResult().getEstimate(), k, 0.0);
    final MemorySegment srcSegR = srcSeg;
    final Union union3 = UnionImpl.fastWrapInstance(srcSegR, seed);
    assertEquals(union3.getResult().getEstimate(), k, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptFamilyException() {
    final int k = 16;
    final MemorySegment seg = MemorySegment.ofArray(new byte[(k*8) + 24]);
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
    final MemorySegment v2seg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);

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
    final MemorySegment seg = convertSerVer3toSerVer2(sketch.compact(), Util.DEFAULT_UPDATE_SEED);
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
      skWseg = arena.allocate(bytes / 2); //we never populate the sketch so this size is not relevant
      final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(skWseg);
      //The sketch has not changed, so the sketch's internal segment is what we gave it.
      assertTrue(sketch.isSameResource(skWseg));

      //We now do the same with an independent Union
      uWseg = arena.allocate(bytes / 2); // way too small, this will forces overflow
      final Union union = SetOperation.builder().buildUnion(uWseg); //off-heap union
      assertTrue(union.isSameResource(uWseg)); //Union has not changed

      for (int i = 0; i < u; i++) { union.update(i); } //populate first union
      assertFalse(union.isSameResource(skWseg)); //Sketch and Union segments are totally distinct
      //Here the union's segment overflowed and moved to the heap,
      // thus, its reference to its segment is not the one we gave it!
      assertFalse(union.isSameResource(uWseg)); //moved.

      //Here we create a 2nd Union with a heap segment
      final MemorySegment uWsegHeap = MemorySegment.ofArray(new byte[bytes / 2]);
      final Union union2 = SetOperation.builder().buildUnion(uWsegHeap); //union with on-heap segment
      //As before, this establishes that the empty union2 has the same segment that we gave it.
      assertTrue(union2.isSameResource(uWsegHeap)); //Copilot complained this was absent, so here it is!

      //These two asserts are calling the Interface static method directly and establish
      // what should be obvious, that these are all different segments.
      assertFalse(MemorySegmentStatus.isSameResource(uWseg, uWsegHeap));  //obviously not
      assertFalse(MemorySegmentStatus.isSameResource(uWsegHeap, union.getMemorySegment())); //tgt moved
    } //the off-heap segments will be closed here
    assertFalse(skWseg.scope().isAlive()); //closed as part of the same Arena
    assertFalse(uWseg.scope().isAlive()); //closed as part of the same Arena
    //we don't need to close the heap segment.
  }

  @Test
  public void checkRestricted() {
    final Union union = Sketches.setOperationBuilder().buildUnion();
    assertTrue(union.isEmpty());
    assertEquals(union.getThetaLong(), Long.MAX_VALUE);
    assertEquals(union.getSeedHash(), Util.computeSeedHash(Util.DEFAULT_UPDATE_SEED));
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
        final MemorySegment wseg = arena.allocate(bytes);

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
    for (int i = 0; i < (num/2); i++) {
      skArr[i].update(i);
      skArr[i + (num/2)].update(i);
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
