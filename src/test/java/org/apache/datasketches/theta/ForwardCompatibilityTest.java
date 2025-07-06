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
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer2;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.EmptyCompactSketch;
import org.apache.datasketches.theta.HeapCompactSketch;
import org.apache.datasketches.theta.PreambleUtil;
import org.apache.datasketches.theta.SingleItemSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ForwardCompatibilityTest {

  @Test
  public void checkSerVer1_Empty() {
    final CompactSketch csk = EmptyCompactSketch.getInstance();
    final MemorySegment srcSeg = convertSerVer3toSerVer1(csk).asReadOnly();
    final Sketch sketch = Sketch.heapify(srcSeg);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSerVer1_badPrelongs() {
    final CompactSketch csk = EmptyCompactSketch.getInstance();

    final MemorySegment srcWseg = convertSerVer3toSerVer1(csk);
    final MemorySegment srcseg = srcWseg.asReadOnly();
    srcWseg.set(JAVA_BYTE, 0, (byte) 1);
    Sketch.heapify(srcWseg); //throws because bad preLongs
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkSerVer1_tooSmall() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    usk.update(2);
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer1(csk).asReadOnly();
    final MemorySegment srcSeg2 = srcSeg.asSlice(0, srcSeg.byteSize() - 8);
    Sketch.heapify(srcSeg2); //throws because too small
  }


  @Test
  public void checkSerVer1_1Value() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().build();
    usk.update(1);
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer1(csk).asReadOnly();
    final Sketch sketch = Sketch.heapify(srcSeg);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertEquals(sketch.getEstimate(), 1.0);
    assertTrue(sketch instanceof SingleItemSketch);
  }

  @Test
  public void checkSerVer2_1PreLong_Empty() {
    final CompactSketch csk = EmptyCompactSketch.getInstance();
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();
    final Sketch sketch = Sketch.heapify(srcSeg);
    assertEquals(sketch.isEmpty(), true);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test
  public void checkSerVer2_2PreLongs_Empty() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    for (int i = 0; i < 2; i++) { usk.update(i); } //exact mode
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();

    final MemorySegment srcWseg = MemorySegment.ofArray(new byte[16]);
    MemorySegment.copy(srcSeg, 0, srcWseg, 0, 16);
    PreambleUtil.setEmpty(srcWseg); //Force
    assertTrue(PreambleUtil.isEmptyFlag(srcWseg));
    srcWseg.set(JAVA_INT_UNALIGNED, 8, 0); //corrupt curCount = 0

    final Sketch sketch = Sketch.heapify(srcWseg);
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test
  public void checkSerVer2_3PreLongs_Empty() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    for (int i = 0; i < 32; i++) { usk.update(i); } //est mode
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();

    final MemorySegment srcWseg = MemorySegment.ofArray(new byte[24]);
    MemorySegment.copy(srcSeg, 0, srcWseg, 0, 24);
    PreambleUtil.setEmpty(srcWseg); //Force
    assertTrue(PreambleUtil.isEmptyFlag(srcWseg));
    srcWseg.set(JAVA_INT_UNALIGNED, 8, 0); //corrupt curCount = 0
    srcWseg.set(JAVA_LONG_UNALIGNED, 16, Long.MAX_VALUE); //corrupt to make it look empty

    final Sketch sketch = Sketch.heapify(srcWseg); //now serVer=3, EmptyCompactSketch
    assertTrue(sketch instanceof EmptyCompactSketch);
  }

  @Test
  public void checkSerVer2_2PreLongs_1Value() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    usk.update(1); //exact mode
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();

    final Sketch sketch = Sketch.heapify(srcSeg);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof SingleItemSketch);
  }

  @Test
  public void checkSerVer2_3PreLongs_1Value() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    for (int i = 0; i < 32; i++) { usk.update(i); } //est mode
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();

    final MemorySegment srcWseg = MemorySegment.ofArray(new byte[32]);
    MemorySegment.copy(srcSeg, 0, srcWseg, 0, 32);
    srcWseg.set(JAVA_INT_UNALIGNED, 8, 1); //corrupt curCount = 1
    srcWseg.set(JAVA_LONG_UNALIGNED, 16, Long.MAX_VALUE); //corrupt theta to make it look exact
    final long[] cache = csk.getCache();
    srcWseg.set(JAVA_LONG_UNALIGNED, 24, cache[0]); //corrupt cache with only one value

    final Sketch sketch = Sketch.heapify(srcWseg);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), false);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof SingleItemSketch);
  }

  @Test
  public void checkSerVer2_3PreLongs_1Value_ThLessthan1() {
    final UpdateSketch usk = Sketches.updateSketchBuilder().setLogNominalEntries(4).build();
    for (int i = 0; i < 32; i++) { usk.update(i); } //est mode
    final CompactSketch csk = usk.compact(true, null);
    final MemorySegment srcSeg = convertSerVer3toSerVer2(csk, Util.DEFAULT_UPDATE_SEED).asReadOnly();

    final MemorySegment srcWseg = MemorySegment.ofArray(new byte[32]);
    MemorySegment.copy(srcSeg, 0, srcWseg, 0, 32);
    srcWseg.set(JAVA_INT_UNALIGNED, 8, 1); //corrupt curCount = 1
    final long[] cache = csk.getCache();
    srcWseg.set(JAVA_LONG_UNALIGNED, 24, cache[0]); //corrupt cache with only one value

    final Sketch sketch = Sketch.heapify(srcWseg);
    assertEquals(sketch.isEmpty(), false);
    assertEquals(sketch.isEstimationMode(), true);
    assertEquals(sketch.isOffHeap(), false);
    assertEquals(sketch.hasMemorySegment(), false);
    assertEquals(sketch.isCompact(), true);
    assertEquals(sketch.isOrdered(), true);
    assertTrue(sketch instanceof HeapCompactSketch);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
