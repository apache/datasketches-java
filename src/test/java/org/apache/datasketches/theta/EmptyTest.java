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

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

/**
 * Empty essentially means that the sketch has never seen data.
 *
 * @author Lee Rhodes
 */
public class EmptyTest {

  @Test
  public void checkEmpty() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch sk2 = UpdatableThetaSketch.builder().build();
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();

    final int u = 100;
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    final CompactThetaSketch csk = inter.getResult();
    //The intersection of two disjoint, exact-mode sketches is empty, T == 1.0.
    println(csk.toString());
    assertTrue(csk.isEmpty());

    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk2 = aNotB.aNotB(csk, sk1);
    //The ThetaAnotB of an empty, T == 1.0 sketch with another exact-mode sketch is empty, T == 1.0
    assertTrue(csk2.isEmpty());
  }

  @Test
  public void checkNotEmpty() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch sk2 = UpdatableThetaSketch.builder().build();
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();

    final int u = 10000; //estimating
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    final CompactThetaSketch csk = inter.getResult();
    println(csk.toString());
    //The intersection of two disjoint, est-mode sketches is not-empty, T < 1.0.
    assertFalse(csk.isEmpty());

    ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk2 = aNotB.aNotB(csk, sk1); //empty, T < 1.0; with est-mode sketch
    println(csk2.toString());
    //The ThetaAnotB of an empty, T < 1.0 sketch with another exact-mode sketch is not-empty.
    assertFalse(csk2.isEmpty());

    final UpdatableThetaSketch sk3 = UpdatableThetaSketch.builder().build();
    aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk3 = aNotB.aNotB(sk3, sk1); //empty, T == 1.0; with est-mode sketch
    println(csk3.toString());
    //the ThetaAnotB of an empty, T == 1.0 sketch with another est-mode sketch is empty, T < 1.0
    assertTrue(csk3.isEmpty());
  }

  @Test
  public void checkPsampling() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().setP(.5F).build();
    assertTrue(sk1.isEmpty());
    //An empty P-sampling sketch where T < 1.0 and has never seen data is also empty
    // and will have a full preamble of 24 bytes.  But when compacted, theta returns to 1.0, so
    // it will be stored as only 8 bytes.
    assertEquals(sk1.compact().toByteArray().length, 8);
  }

  @Test
  public void checkEmptyToCompact() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final CompactThetaSketch csk = sk1.compact();
    assertTrue(csk instanceof EmptyCompactSketch);
    final CompactThetaSketch csk2 = csk.compact();
    assertTrue(csk2 instanceof EmptyCompactSketch);
    final CompactThetaSketch csk3 = csk.compact(true, MemorySegment.ofArray(new byte[8]));
    assertTrue(csk3 instanceof DirectCompactSketch);
    assertEquals(csk2.getCurrentPreambleLongs(), 1);
  }

  @Test
  public void checkEmptyMaskTestArrInvariants() {
    final long mask = EmptyCompactSketch.EMPTY_SKETCH_MASK;
    final long test = EmptyCompactSketch.EMPTY_SKETCH_TEST;
    final long arr = MemorySegment.ofArray(EmptyCompactSketch.EMPTY_COMPACT_SKETCH_ARR)
        .get(JAVA_LONG_UNALIGNED, 0);
    assertEquals(test & ~mask, 0L); //the TEST must lie within the MASK
    assertEquals(arr & mask, test); //our own output must pass
    assertEquals(EmptyCompactSketch.EMPTY_SKETCH_TEST_LEGACY & ~mask, 0L);
  }

  @Test
  public void checkEmptyAcceptsHistoricalImages() {
    //Java image: ordered flag set, seed hash 0
    checkEmptyAccepted(new byte[] { 1, 3, 3, 0, 0, 0x1E, 0, 0 });
    //C++ image: ordered flag set, seed hash present
    checkEmptyAccepted(new byte[] { 1, 3, 3, 0, 0, 0x1E, (byte) 0xCC, (byte) 0x93 });
    //C++ before 3.3.0: ordered flag not set
    checkEmptyAccepted(new byte[] { 1, 3, 3, 0, 0, 0x0E, (byte) 0xCC, (byte) 0x93 });
    //sketches-core 0.9.0, p = 0.5: full 24-byte form, count = 0, p = 1.0f, theta = 0.5
    checkEmptyAccepted(legacyEmptyP05((byte) 0x1E, 0)); //ordered
    checkEmptyAccepted(legacyEmptyP05((byte) 0x0E, 0)); //unordered
  }

  private static byte[] legacyEmptyP05(final byte flags, final int count) {
    return new byte[] {
        3, 3, 3, 0, 0, flags, (byte) 0xCC, (byte) 0x93,
        (byte) count, 0, 0, 0, 0, 0, (byte) 0x80, 0x3F,
        0, 0, 0, 0, 0, 0, 0, 0x40 };
  }

  @Test
  public void checkEmptyRejectsMangledImages() {
    checkEmptyRejected(new byte[] { 1, 3, 3, 1, 0, 0x1E, 0, 0 });    //byte 3 not zero
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 1, 0x1E, 0, 0 });    //byte 4 not zero
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, 0x1F, 0, 0 });    //reserved bit 0 set
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, 0x5E, 0, 0 });    //reserved bit 6 set
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, (byte) 0x9E, 0, 0 }); //reserved bit 7 set
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, 0x3E, 0, 0 });    //single-item set
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, 0x1C, 0, 0 });    //read-only not set
    checkEmptyRejected(new byte[] { 1, 3, 3, 0, 0, 0x16, 0, 0 });    //compact not set
    checkEmptyRejected(new byte[] { 2, 3, 3, 0, 0, 0x1E, 0, 0 });    //preLongs 2
    checkEmptyRejected(legacyEmptyP05((byte) 0x1E, 1));              //preLongs 3, count not 0
    checkEmptyRejected(new byte[] { 3, 3, 3, 0, 0, 0x1E, 0, 0 });    //preLongs 3, too short
    final byte[] wrongSeed = legacyEmptyP05((byte) 0x1E, 0);
    wrongSeed[6] = 0; wrongSeed[7] = 0;
    checkEmptyRejected(wrongSeed);                                    //preLongs 3, seed hash mismatch
  }

  private static void checkEmptyAccepted(final byte[] image) {
    final long seed = Util.DEFAULT_UPDATE_SEED;
    assertTrue(CompactThetaSketch.heapify(MemorySegment.ofArray(image), seed) instanceof EmptyCompactSketch);
    assertTrue(CompactThetaSketch.wrap(MemorySegment.ofArray(image), seed) instanceof EmptyCompactSketch);
    assertTrue(CompactThetaSketch.wrap(image, seed) instanceof EmptyCompactSketch);
  }

  private static void checkEmptyRejected(final byte[] image) {
    final long seed = Util.DEFAULT_UPDATE_SEED;
    try { CompactThetaSketch.heapify(MemorySegment.ofArray(image), seed); fail("heapify"); }
    catch (final SketchesArgumentException e) { /* expected */ }
    try { CompactThetaSketch.wrap(MemorySegment.ofArray(image), seed); fail("wrap segment"); }
    catch (final SketchesArgumentException e) { /* expected */ }
    try { CompactThetaSketch.wrap(image, seed); fail("wrap bytes"); }
    catch (final SketchesArgumentException e) { /* expected */ }
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
