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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SketchMiscTest {

  private static MemorySegment getCompactSketchMemorySegment(final int k, final int from, final int to) {
    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=from; i<to; i++) {
      sk1.update(i);
    }
    final CompactSketch csk = sk1.compact(true, null);
    final byte[] sk1bytes = csk.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sk1bytes).asReadOnly();
    return seg;
  }

  private static MemorySegment getMemorySegmentFromCompactSketch(final CompactSketch csk) {
    final byte[] sk1bytes = csk.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sk1bytes).asReadOnly();
    return seg;
  }

  private static CompactSketch getCompactSketch(final int k, final int from, final int to) {
    final UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build();
    for (int i=from; i<to; i++) {
      sk1.update(i);
    }
    return sk1.compact(true, null);
  }

  @Test
  public void checkSketchMethods() {
    final int k = 1024;
    final MemorySegment seg = getCompactSketchMemorySegment(k, 0, k);

    CompactSketch csk2 = (CompactSketch) Sketch.heapify(seg);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch) Sketch.heapify(seg, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch) Sketch.wrap(seg);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch) Sketch.wrap(seg, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);
  }

  @Test
  public void checkSetOpMethods() {
    final int k = 1024;
    final MemorySegment seg1 = getCompactSketchMemorySegment(k, 0, k);
    final MemorySegment seg2 = getCompactSketchMemorySegment(k, k/2, 3*k/2);

    final SetOperationBuilder bldr = SetOperation.builder();
    final Union union = bldr.setNominalEntries(2 * k).buildUnion();

    union.union(seg1);
    CompactSketch cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), k);
    union.union(seg2);
    cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    final byte[] ubytes = union.toByteArray();
    final MemorySegment uSeg = MemorySegment.ofArray(ubytes);

    Union union2 = (Union) SetOperation.heapify(uSeg);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union) SetOperation.heapify(uSeg, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union) SetOperation.wrap(uSeg);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union) SetOperation.wrap(uSeg, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    final int serVer = Sketch.getSerializationVersion(uSeg);
    assertEquals(serVer, 3);
  }

  @Test
  public void checkUtilMethods() {
    final int lgK = 10;
    final int k = 1 << lgK;

    final int maxUnionBytes = SetOperation.getMaxUnionBytes(k);
    assertEquals(2*k*8+32, maxUnionBytes);

    final int maxInterBytes = SetOperation.getMaxIntersectionBytes(k);
    assertEquals(2*k*8+24, maxInterBytes);

    final int maxCompSkBytes = Sketch.getMaxCompactSketchBytes(k+1);
    assertEquals(24+(k+1)*8, maxCompSkBytes);

    final int compSkMaxBytes = Sketch.getCompactSketchMaxBytes(lgK); {
        int bytes = (int)((2 << lgK) * ThetaUtil.REBUILD_THRESHOLD + Family.QUICKSELECT.getMaxPreLongs()) * Long.BYTES;
        assertEquals(compSkMaxBytes, bytes);
    }

    final int maxSkBytes = Sketch.getMaxUpdateSketchBytes(k);
    assertEquals(24+2*k*8, maxSkBytes);
  }

  @Test
  public void checkStaticEstimators() {
    final int k = 4096;
    final int u = 4*k;
    final CompactSketch csk = getCompactSketch(k, 0, u);
    final MemorySegment srcSeg = getMemorySegmentFromCompactSketch(csk);
    final double est = Sketch.getEstimate(srcSeg);
    assertEquals(est, u, 0.05*u);
    final double rse = 1.0/Math.sqrt(k);
    final double ub = Sketch.getUpperBound(1, srcSeg);
    assertEquals(ub, est+rse, 0.05*u);
    final double lb = Sketch.getLowerBound(1, srcSeg);
    assertEquals(lb, est-rse, 0.05*u);
    final MemorySegment segV1 = BackwardConversions.convertSerVer3toSerVer1(csk);
    boolean empty = Sketch.getEmpty(segV1);
    assertFalse(empty);

    final CompactSketch csk2 = getCompactSketch(k, 0, 0);
    final MemorySegment emptySegV3 = getMemorySegmentFromCompactSketch(csk2);
    assertEquals(Sketch.getRetainedEntries(emptySegV3), 0);
    assertEquals(Sketch.getThetaLong(emptySegV3), Long.MAX_VALUE);
    final MemorySegment emptySegV1 = BackwardConversions.convertSerVer3toSerVer1(csk2);
    empty = Sketch.getEmpty(emptySegV1);
    assertTrue(empty);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSketchFamily() {
    final Union union = SetOperation.builder().buildUnion();
    final byte[] byteArr = union.toByteArray();
    final MemorySegment srcSeg = MemorySegment.ofArray(byteArr);
    Sketch.getEstimate(srcSeg); //Union is not a Theta Sketch, it is an operation
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
