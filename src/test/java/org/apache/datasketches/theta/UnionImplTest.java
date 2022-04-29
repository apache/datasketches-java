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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
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
    final WritableMemory mem = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final WritableMemory mem2 = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) { sketch.update(i); }
    final CompactSketch sketchInDirectOrd = sketch.compact(true, mem);
    final CompactSketch sketchInDirectUnord = sketch.compact(false, mem2);
    final CompactSketch sketchInHeap = sketch.compact(true, null);

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(sketchInDirectOrd);
    union.union(sketchInHeap);
    union.union(sketchInDirectUnord);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkUnorderedAndOrderedMemory() {
    final int k = 16;
    final WritableMemory mem = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < k; i++) { sketch.update(i); }
    final CompactSketch sketchInDirectOrd = sketch.compact(false, mem);
    assertFalse(sketchInDirectOrd.isOrdered());
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(sketchInDirectOrd);
    final double est1 = union.getResult().getEstimate();
    sketch.compact(true, mem); //change the order as a side effect
    assertTrue(sketchInDirectOrd.isOrdered());
    union.union(sketchInDirectOrd);
    final double est2 = union.getResult().getEstimate();
    assertEquals(est1, est2);
    assertEquals((int)est1, k);
  }

  @Test
  public void checkUpdateWithMem() {
    final int k = 16;
    final WritableMemory skMem = WritableMemory.writableWrap(new byte[2*k*8 + 24]);
    final WritableMemory dirOrdCskMem = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final WritableMemory dirUnordCskMem = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final UpdateSketch udSketch = UpdateSketch.builder().setNominalEntries(k).build(skMem);
    for (int i = 0; i < k; i++) { udSketch.update(i); } //exact
    udSketch.compact(true, dirOrdCskMem);
    udSketch.compact(false, dirUnordCskMem);

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(skMem);
    union.union(dirOrdCskMem);
    union.union(dirUnordCskMem);
    assertEquals(union.getResult().getEstimate(), k, 0.0);
  }

  @Test
  public void checkFastWrap() {
    final int k = 16;
    final long seed = DEFAULT_UPDATE_SEED;
    final int unionSize = Sketches.getMaxUnionBytes(k);
    final WritableMemory srcMem = WritableMemory.writableWrap(new byte[unionSize]);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion(srcMem);
    for (int i = 0; i < k; i++) { union.update(i); } //exact
    assertEquals(union.getResult().getEstimate(), k, 0.0);
    final Union union2 = UnionImpl.fastWrap(srcMem, seed);
    assertEquals(union2.getResult().getEstimate(), k, 0.0);
    final Memory srcMemR = srcMem;
    final Union union3 = UnionImpl.fastWrap(srcMemR, seed);
    assertEquals(union3.getResult().getEstimate(), k, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkCorruptFamilyException() {
    final int k = 16;
    final WritableMemory mem = WritableMemory.writableWrap(new byte[k*8 + 24]);
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    sketch.compact(true, mem);

    mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer2FamilyException() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    final CompactSketch csk = sketch.compact(true, null);
    final WritableMemory v2mem = (WritableMemory) convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);

    v2mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(v2mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer1FamilyException() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    final CompactSketch csk = sketch.compact(true, null);
    final WritableMemory v1mem = (WritableMemory) convertSerVer3toSerVer1(csk);

    v1mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(v1mem);
  }

  @Test
  public void checkVer2EmptyHandling() {
    final int k = 16;
    final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    final Memory mem = convertSerVer3toSerVer2(sketch.compact(), Util.DEFAULT_UPDATE_SEED);
    final Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.union(mem);
  }

  @Test
  public void checkMoveAndResize() {
    final int k = 1 << 12;
    final int u = 2 * k;
    final int bytes = Sketches.getMaxUpdateSketchBytes(k);
    try (WritableHandle wh = WritableMemory.allocateDirect(bytes/2);
        WritableHandle wh2 = WritableMemory.allocateDirect(bytes/2) ) {
      final WritableMemory wmem = wh.getWritable();
      final UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
      assertTrue(sketch.isSameResource(wmem));

      final WritableMemory wmem2 = wh2.getWritable();
      final Union union = SetOperation.builder().buildUnion(wmem2);
      assertTrue(union.isSameResource(wmem2));

      for (int i = 0; i < u; i++) { union.update(i); }
      assertFalse(union.isSameResource(wmem));

      final Union union2 = SetOperation.builder().buildUnion(); //on-heap union
      assertFalse(union2.isSameResource(wmem2));  //obviously not
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkRestricted() {
    final Union union = Sketches.setOperationBuilder().buildUnion();
    assertTrue(union.isEmpty());
    assertEquals(union.getThetaLong(), Long.MAX_VALUE);
    assertEquals(union.getSeedHash(), Util.computeSeedHash(DEFAULT_UPDATE_SEED));
    assertEquals(union.getRetainedEntries(), 0);
    assertEquals(union.getCache().length, 128); //only applies to stateful
  }

  @Test
  public void checkUnionCompactOrderedSource() {
    final int k = 1 << 12;
    final UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < k; i++) { sk.update(i); }
    final double est1 = sk.getEstimate();

    final int bytes = Sketches.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
    try (WritableHandle h = WritableMemory.allocateDirect(bytes)) {
      final WritableMemory wmem = h.getWritable();
      final CompactSketch csk = sk.compact(true, wmem); //ordered, direct
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
    final WritableMemory wmem1 = WritableMemory.allocate(bytes);
    final UpdateSketch sk = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem1);
    for (int i = 0; i < k; i++) { sk.update(i); }
    sk.compact(true, wmem1); //corrupt the wmem1 to be a compact sketch

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

    final Memory[] memArr = new Memory[num];
    for (int i = 0; i < num; i++) {
      memArr[i] = Memory.wrap(skArr[i].compact().toByteArray());
    }
    union = new SetOperationBuilder().buildUnion();
    for (int i = 0; i < num; i++) {
      union.union(memArr[i]);
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
