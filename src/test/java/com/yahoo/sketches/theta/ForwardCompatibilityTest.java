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

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static com.yahoo.sketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
public class ForwardCompatibilityTest {

  @Test
  public void checkSerVer1_Empty() {
    CompactSketch csk = EmptyCompactSketch.getInstance();
    Memory srcMem = convertSerVer3toSerVer1(csk);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSerVer1_tooSmall() {
    UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    usk.update(2);
    CompactSketch csk = usk.compact(true, null);
    Memory srcMem = convertSerVer3toSerVer1(csk);
    Memory srcMem2 = srcMem.region(0, srcMem.getCapacity() - 8);
    Sketch.heapify(srcMem2); //throws because too small
  }


  @Test
  public void checkSerVer1_1Value() {
    UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    CompactSketch csk = usk.compact(true, null);
    Memory srcMem = convertSerVer3toSerVer1(csk);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(sketch instanceof SingleItemSketch);
  }

  @Test
  public void checkSerVer2_Empty() {
    CompactSketch csk = EmptyCompactSketch.getInstance();
    Memory srcMem = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);
    Sketch sketch = Sketch.heapify(srcMem);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test
  public void checkSerVer2_24Bytes_0Values() {
    UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    for (int i = 0; i < 32; i++) { usk.update(i); }
    CompactSketch csk = usk.compact(true, null);
    Memory srcMem = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED);

    WritableMemory srcMemW = WritableMemory.allocate(24);
    srcMem.copyTo(0, srcMemW, 0, 24);
    PreambleUtil.setEmpty(srcMemW); //Force
    assertTrue(PreambleUtil.isEmpty(srcMemW));
    srcMemW.putInt(8, 0); //corrupt curCount = 0
    srcMemW.putLong(16, Long.MAX_VALUE); //corrupt to make it look empty

    Sketch sketch = Sketch.heapify(srcMemW);
    assertEquals(sketch.isEmpty(), true); //was forced true
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isDirect(), false);
    assertEquals(sketch.hasMemory(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof EmptyCompactSketch);
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
