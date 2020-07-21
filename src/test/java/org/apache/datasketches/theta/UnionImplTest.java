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
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
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

  @Test
  public void checkUnorderedAndOrderedMemory() {
    int k = 16;
    WritableMemory mem = WritableMemory.wrap(new byte[(k*8) + 24]);
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i = 0; i < k; i++) { sketch.update(i); }
    CompactSketch sketchInDirectOrd = sketch.compact(false, mem);
    assertFalse(sketchInDirectOrd.isOrdered());
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(sketchInDirectOrd);
    final double est1 = union.getResult().getEstimate();
    sketch.compact(true, mem); //change the order as a side effect
    assertTrue(sketchInDirectOrd.isOrdered());
    union.update(sketchInDirectOrd);
    final double est2 = union.getResult().getEstimate();
    assertEquals(est1, est2);
    assertEquals((int)est1, k);
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
  public void checkVer2FamilyException() {
    int k = 16;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    CompactSketch csk = sketch.compact(true, null);
    WritableMemory v2mem = (WritableMemory) convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);

    v2mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(v2mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkVer1FamilyException() {
    int k = 16;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    for (int i=0; i<k; i++) {
      sketch.update(i);
    }
    CompactSketch csk = sketch.compact(true, null);
    WritableMemory v1mem = (WritableMemory) convertSerVer3toSerVer1(csk);

    v1mem.putByte(PreambleUtil.FAMILY_BYTE, (byte)0); //corrupt family

    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(v1mem);
  }

  @Test
  public void checkVer2EmptyHandling() {
    int k = 16;
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(k).build();
    Memory mem = convertSerVer3toSerVer2(sketch.compact(), Util.DEFAULT_UPDATE_SEED);
    Union union = Sketches.setOperationBuilder().setNominalEntries(k).buildUnion();
    union.update(mem);
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
    assertEquals(union.getRetainedEntries(), 0);
    assertEquals(union.getCache().length, 128); //only applies to stateful
  }

  @Test
  public void checkUnionCompactOrderedSource() {
    int k = 1 << 12;
    UpdateSketch sk = Sketches.updateSketchBuilder().build();
    for (int i = 0; i < k; i++) { sk.update(i); }
    double est1 = sk.getEstimate();

    int bytes = Sketches.getMaxCompactSketchBytes(sk.getRetainedEntries(true));
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

  @Test //checks for bug introduced in 1.0.0-incubating.
  public void checkDirectUnionSingleItem() {
    int num = 2;
    UpdateSketch[] skArr = new UpdateSketch[num];
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
      union.update(skArr[i]);
    }

    CompactSketch csk = union.getResult();
    assertEquals(csk.getEstimate(), 2.0);
    //println(csk.toString(true, true, 1, true));

    Memory[] memArr = new Memory[num];
    for (int i = 0; i < num; i++) {
      memArr[i] = Memory.wrap(skArr[i].compact().toByteArray());
    }
    union = new SetOperationBuilder().buildUnion();
    for (int i = 0; i < num; i++) {
      union.update(memArr[i]);
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
  static void println(Object o) {
    //System.out.println(o.toString()); //disable here
  }

}
