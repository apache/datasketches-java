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

import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;


/**
 * Empty essentially means that the sketch has never seen data.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class EmptyTest {

  @Test
  public void checkEmpty() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().build();
    UpdateSketch sk2 = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();

    int u = 100;
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    CompactSketch csk = inter.getResult();
    //The intersection of two disjoint, exact-mode sketches is empty, T == 1.0.
    println(csk.toString());
    assertTrue(csk.isEmpty());

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    CompactSketch csk2 = aNotB.aNotB(csk, sk1);
    //The AnotB of an empty, T == 1.0 sketch with another exact-mode sketch is empty, T == 1.0
    assertTrue(csk2.isEmpty());
  }

  @Test
  public void checkNotEmpty() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().build();
    UpdateSketch sk2 = Sketches.updateSketchBuilder().build();
    Intersection inter = Sketches.setOperationBuilder().buildIntersection();

    int u = 10000; //estimating
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    CompactSketch csk = inter.getResult();
    println(csk.toString());
    //The intersection of two disjoint, est-mode sketches is not-empty, T < 1.0.
    assertFalse(csk.isEmpty());

    AnotB aNotB = Sketches.setOperationBuilder().buildANotB();
    CompactSketch csk2 = aNotB.aNotB(csk, sk1); //empty, T < 1.0; with est-mode sketch
    println(csk2.toString());
    //The AnotB of an empty, T < 1.0 sketch with another exact-mode sketch is not-empty.
    assertFalse(csk2.isEmpty());

    UpdateSketch sk3 = Sketches.updateSketchBuilder().build();
    aNotB = Sketches.setOperationBuilder().buildANotB();
    CompactSketch csk3 = aNotB.aNotB(sk3, sk1); //empty, T == 1.0; with est-mode sketch
    println(csk3.toString());
    //the AnotB of an empty, T == 1.0 sketch with another est-mode sketch is empty, T < 1.0
    assertTrue(csk3.isEmpty());
  }

  @Test
  public void checkPsampling() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setP(.5F).build();
    assertTrue(sk1.isEmpty());
    //An empty P-sampling sketch where T < 1.0 and has never seen data is also empty
    // and will have a full preamble of 24 bytes.  But when compacted, theta returns to 1.0, so
    // it will be stored as only 8 bytes.
    assertEquals(sk1.compact().toByteArray().length, 8);
  }

  //These 3 tests reproduce a failure mode where an "old" empty sketch of 8 bytes without
  // its empty-flag bit set is read.
  @Test
  public void checkBackwardCompatibility1() {
    final int k = 16;
    final int bytes = Sketches.getMaxUnionBytes(k); //288
    Union union = SetOperation.builder().buildUnion(WritableMemory.allocate(bytes));
    Memory mem = badEmptySk();
    Sketch wsk = Sketches.wrapSketch(mem);
    union.update(wsk); //union has memory
  }

  @Test
  public void checkBackwardCompatibility2() {
    Union union = SetOperation.builder().setNominalEntries(16).buildUnion();
    Memory mem = badEmptySk();
    Sketch wsk = Sketches.wrapSketch(mem);
    union.update(wsk); //heap union
  }

  @Test
  public void checkBackwardCompatibility3() {
    Memory mem = badEmptySk();
    Sketches.heapifySketch(mem);
  }

  @Test
  public void checkEmptyToCompact() {
    UpdateSketch sk1 = Sketches.updateSketchBuilder().build();
    CompactSketch csk = sk1.compact();
    assertTrue(csk instanceof EmptyCompactSketch);
    CompactSketch csk2 = csk.compact();
    assertTrue(csk2 instanceof EmptyCompactSketch);
    CompactSketch csk3 = csk.compact(true, WritableMemory.allocate(8));
    assertTrue(csk3 instanceof DirectCompactSketch);
    assertEquals(csk2.getCurrentPreambleLongs(), 1);
  }


  //SerVer 2 had an empty sketch where preLongs = 1, but empty bit was not set.
  private static Memory badEmptySk() {
    final long preLongs = 1;
    final long serVer = 2;
    final long family = 3; //compact
    final long flags = (ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK);
    final long seedHash = 0x93CC;
    final long badEmptySk = (seedHash << 48) | (flags << 40)
        | (family << 16) | (serVer << 8) | preLongs;
    WritableMemory wmem =  WritableMemory.allocate(8);
    wmem.putLong(0, badEmptySk);
    return wmem;
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
