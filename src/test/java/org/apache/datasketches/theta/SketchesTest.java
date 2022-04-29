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

import static org.apache.datasketches.theta.BackwardConversions.convertSerVer3toSerVer1;
import static org.apache.datasketches.theta.Sketches.getMaxCompactSketchBytes;
import static org.apache.datasketches.theta.Sketches.getMaxIntersectionBytes;
import static org.apache.datasketches.theta.Sketches.getMaxUnionBytes;
import static org.apache.datasketches.theta.Sketches.getMaxUpdateSketchBytes;
import static org.apache.datasketches.theta.Sketches.getSerializationVersion;
import static org.apache.datasketches.theta.Sketches.heapifySetOperation;
import static org.apache.datasketches.theta.Sketches.heapifySketch;
import static org.apache.datasketches.theta.Sketches.setOperationBuilder;
import static org.apache.datasketches.theta.Sketches.updateSketchBuilder;
import static org.apache.datasketches.theta.Sketches.wrapSetOperation;
import static org.apache.datasketches.theta.Sketches.wrapSketch;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SketchesTest {

  private static Memory getCompactSketchMemory(final int k, final int from, final int to) {
    final UpdateSketch sk1 = updateSketchBuilder().setNominalEntries(k).build();
    for (int i=from; i<to; i++) {
      sk1.update(i);
    }
    final CompactSketch csk = sk1.compact(true, null);
    final byte[] sk1bytes = csk.toByteArray();
    final Memory mem = Memory.wrap(sk1bytes);
    return mem;
  }

  private static Memory getMemoryFromCompactSketch(final CompactSketch csk) {
    final byte[] sk1bytes = csk.toByteArray();
    final Memory mem = Memory.wrap(sk1bytes);
    return mem;
  }

  private static CompactSketch getCompactSketch(final int k, final int from, final int to) {
    final UpdateSketch sk1 = updateSketchBuilder().setNominalEntries(k).build();
    for (int i=from; i<to; i++) {
      sk1.update(i);
    }
    return sk1.compact(true, null);
  }

  @Test
  public void checkSketchMethods() {
    final int k = 1024;
    final Memory mem = getCompactSketchMemory(k, 0, k);

    CompactSketch csk2 = (CompactSketch)heapifySketch(mem);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch)heapifySketch(mem, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch)wrapSketch(mem);
    assertEquals((int)csk2.getEstimate(), k);

    csk2 = (CompactSketch)wrapSketch(mem, Util.DEFAULT_UPDATE_SEED);
    assertEquals((int)csk2.getEstimate(), k);
  }

  @Test
  public void checkSetOpMethods() {
    final int k = 1024;
    final Memory mem1 = getCompactSketchMemory(k, 0, k);
    final Memory mem2 = getCompactSketchMemory(k, k/2, 3*k/2);

    final SetOperationBuilder bldr = setOperationBuilder();
    final Union union = bldr.setNominalEntries(2 * k).buildUnion();

    union.union(mem1);
    CompactSketch cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), k);
    union.union(mem2);
    cSk = union.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    final byte[] ubytes = union.toByteArray();
    final WritableMemory uMem = WritableMemory.writableWrap(ubytes);

    Union union2 = (Union)heapifySetOperation(uMem);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union)heapifySetOperation(uMem, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union)wrapSetOperation(uMem);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    union2 = (Union)wrapSetOperation(uMem, Util.DEFAULT_UPDATE_SEED);
    cSk = union2.getResult(true, null);
    assertEquals((int)cSk.getEstimate(), 3*k/2);

    final int serVer = getSerializationVersion(uMem);
    assertEquals(serVer, 3);
  }

  @Test
  public void checkUtilMethods() {
    final int k = 1024;

    final int maxUnionBytes = getMaxUnionBytes(k);
    assertEquals(2*k*8+32, maxUnionBytes);

    final int maxInterBytes = getMaxIntersectionBytes(k);
    assertEquals(2*k*8+24, maxInterBytes);

    final int maxCompSkBytes = getMaxCompactSketchBytes(k+1);
    assertEquals(24+(k+1)*8, maxCompSkBytes);

    final int maxSkBytes = getMaxUpdateSketchBytes(k);
    assertEquals(24+2*k*8, maxSkBytes);
  }

  @Test
  public void checkStaticEstimators() {
    final int k = 4096;
    final int u = 4*k;
    final CompactSketch csk = getCompactSketch(k, 0, u);
    final Memory srcMem = getMemoryFromCompactSketch(csk);
    final double est = Sketches.getEstimate(srcMem);
    assertEquals(est, u, 0.05*u);
    final double rse = 1.0/Math.sqrt(k);
    final double ub = Sketches.getUpperBound(1, srcMem);
    assertEquals(ub, est+rse, 0.05*u);
    final double lb = Sketches.getLowerBound(1, srcMem);
    assertEquals(lb, est-rse, 0.05*u);
    final Memory memV1 = convertSerVer3toSerVer1(csk);
    boolean empty = Sketches.getEmpty(memV1);
    assertFalse(empty);

    final CompactSketch csk2 = getCompactSketch(k, 0, 0);
    final Memory emptyMemV3 = getMemoryFromCompactSketch(csk2);
    assertEquals(Sketches.getRetainedEntries(emptyMemV3), 0);
    assertEquals(Sketches.getThetaLong(emptyMemV3), Long.MAX_VALUE);
    final Memory emptyMemV1 = convertSerVer3toSerVer1(csk2);
    empty = Sketches.getEmpty(emptyMemV1);
    assertTrue(empty);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSketchFamily() {
    final Union union = setOperationBuilder().buildUnion();
    final byte[] byteArr = union.toByteArray();
    final Memory srcMem = Memory.wrap(byteArr);
    Sketches.getEstimate(srcMem); //Union is not a Theta Sketch, it is an operation
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
