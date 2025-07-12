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

package org.apache.datasketches.sampling;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.sampling.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.sampling.PreambleUtil.RESERVOIR_SIZE_INT;
import static org.apache.datasketches.sampling.PreambleUtil.RESERVOIR_SIZE_SHORT;
import static org.apache.datasketches.sampling.PreambleUtil.SER_VER_BYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.sampling.PreambleUtil;
import org.apache.datasketches.sampling.ReservoirLongsSketch;
import org.apache.datasketches.sampling.ReservoirLongsUnion;
import org.apache.datasketches.sampling.ReservoirSize;
import org.testng.annotations.Test;

public class ReservoirLongsUnionTest {
  @Test
  public void checkEmptyUnion() {
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(1024);
    final byte[] unionBytes = rlu.toByteArray();

    // will intentionally break if changing empty union serialization
    assertEquals(unionBytes.length, 8);

    println(rlu.toString());
  }

  @Test
  public void checkInstantiation() {
    final int n = 100;
    final int k = 25;

    // create empty unions
    ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    assertNull(rlu.getResult());
    rlu.update(5);
    assertNotNull(rlu.getResult());

    // pass in a sketch, as both an object and MemorySegment
    final ReservoirLongsSketch rls = ReservoirLongsSketch.newInstance(k);
    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }

    rlu.reset();
    assertEquals(rlu.getResult().getN(), 0);
    rlu.update(rls);
    assertEquals(rlu.getResult().getN(), rls.getN());

    final byte[] sketchBytes = rls.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sketchBytes);
    rlu = ReservoirLongsUnion.newInstance(rls.getK());
    rlu.update(seg);
    assertNotNull(rlu.getResult());

    println(rlu.toString());
  }

  /*
  @Test
  public void checkReadOnlyInstantiation() {
    final int k = 100;
    final ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(k);
    for (long i = 0; i < 2 * k; ++i) {
      union.update(i);
    }

    final byte[] unionBytes = union.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(unionBytes);

    final ReservoirLongsUnion rlu;
    rlu = ReservoirLongsUnion.heapify(seg);

    assertNotNull(rlu);
    assertEquals(rlu.getMaxK(), k);
    ReservoirLongsSketchTest.validateReservoirEquality(rlu.getResult(), union.getResult());
  }
  */

  @Test
  public void checkNullUpdate() {
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(1024);
    assertNull(rlu.getResult());

    // null sketch
    rlu.update((ReservoirLongsSketch) null);
    assertNull(rlu.getResult());

    // null MemorySegment
    rlu.update((MemorySegment) null);
    assertNull(rlu.getResult());

    // valid input
    rlu.update(5L);
    assertNotNull(rlu.getResult());
  }

  @Test
  public void checkSerialization() {
    final int n = 100;
    final int k = 25;

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    for (int i = 0; i < n; ++i) {
      rlu.update(i);
    }

    final byte[] unionBytes = rlu.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(unionBytes);

    final ReservoirLongsUnion rebuiltUnion = ReservoirLongsUnion.heapify(seg);
    validateUnionEquality(rlu, rebuiltUnion);
  }

  @Test
  public void checkVersionConversionWithEmptyGadget() {
    final int k = 32768;
    final short encK = ReservoirSize.computeSize(k);

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    final byte[] unionBytesOrig = rlu.toByteArray();

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] unionBytes = rlu.toByteArray();
    final MemorySegment unionSeg = MemorySegment.ofArray(unionBytes);

    unionSeg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 1);
    unionSeg.set(JAVA_INT_UNALIGNED, RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionSeg.set(JAVA_SHORT_UNALIGNED, RESERVOIR_SIZE_SHORT, encK);
    println(PreambleUtil.preambleToString(unionSeg));

    final ReservoirLongsUnion rebuilt = ReservoirLongsUnion.heapify(unionSeg);
    final byte[] rebuiltBytes = rebuilt.toByteArray();

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  @Test
  public void checkVersionConversionWithGadget() {
    final long n = 32;
    final int k = 256;
    final short encK = ReservoirSize.computeSize(k);

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    for (long i = 0; i < n; ++i) {
      rlu.update(i);
    }
    final byte[] unionBytesOrig = rlu.toByteArray();

    // get a new byte[], manually revert to v1, then reconstruct
    final byte[] unionBytes = rlu.toByteArray();
    final MemorySegment unionSeg = MemorySegment.ofArray(unionBytes);

    unionSeg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 1);
    unionSeg.set(JAVA_INT_UNALIGNED, RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionSeg.set(JAVA_SHORT_UNALIGNED, RESERVOIR_SIZE_SHORT, encK);

    // force gadget header to v1, too
    final int offset = Family.RESERVOIR_UNION.getMaxPreLongs() << 3;
    unionSeg.set(JAVA_BYTE, offset + SER_VER_BYTE, (byte) 1);
    unionSeg.set(JAVA_INT_UNALIGNED, offset + RESERVOIR_SIZE_INT, 0); // zero out all 4 bytes
    unionSeg.set(JAVA_SHORT_UNALIGNED, offset + RESERVOIR_SIZE_SHORT, encK);

    final ReservoirLongsUnion rebuilt = ReservoirLongsUnion.heapify(unionSeg);
    final byte[] rebuiltBytes = rebuilt.toByteArray();

    assertEquals(unionBytesOrig.length, rebuiltBytes.length);
    for (int i = 0; i < unionBytesOrig.length; ++i) {
      assertEquals(unionBytesOrig[i], rebuiltBytes[i]);
    }
  }

  //@SuppressWarnings("null") // this is the point of the test
  @Test(expectedExceptions = java.lang.NullPointerException.class)
  public void checkNullMemorySegmentInstantiation() {
    ReservoirLongsUnion.heapify(null);
  }

  @Test
  public void checkDownsampledUpdate() {
    final int bigK = 1024;
    final int bigN = 131072;
    final int smallK = 256;
    final int smallN = 2048;
    final ReservoirLongsSketch sketch1 = getBasicSketch(smallN, smallK);
    final ReservoirLongsSketch sketch2 = getBasicSketch(bigN, bigK);

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(smallK);
    assertEquals(rlu.getMaxK(), smallK);

    rlu.update(sketch1);
    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), smallK);

    rlu.update(sketch2);
    assertEquals(rlu.getResult().getK(), smallK);
    assertEquals(rlu.getResult().getNumSamples(), smallK);
  }

  @Test
  public void checkUnionResetWithInitialSmallK() {
    final int maxK = 25;
    final int sketchK = 10;
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(maxK);

    ReservoirLongsSketch rls = getBasicSketch(2 * sketchK, sketchK); // in sampling mode
    rlu.update(rls);
    assertEquals(rlu.getMaxK(), maxK);
    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), sketchK);

    rlu.reset();
    assertNotNull(rlu.getResult());

    // feed in sketch in sampling mode, with larger k than old gadget
    rls = getBasicSketch(2 * maxK, maxK + 1);
    rlu.update(rls);
    assertEquals(rlu.getMaxK(), maxK);
    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), maxK);
  }

  @Test
  public void checkNewGadget() {
    final int maxK = 1024;
    final int bigK = 1536;
    final int smallK = 128;

    // downsample input sketch, use as gadget (exact mode, but irrelevant here)
    final ReservoirLongsSketch bigKSketch = getBasicSketch(maxK / 2, bigK);
    final byte[] bigKBytes = bigKSketch.toByteArray();
    final MemorySegment bigKSeg = MemorySegment.ofArray(bigKBytes);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(maxK);
    rlu.update(bigKSeg);
    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), maxK);
    assertEquals(rlu.getResult().getN(), maxK / 2);

    // sketch k < maxK but in sampling mode
    final ReservoirLongsSketch smallKSketch = getBasicSketch(maxK, smallK);
    final byte[] smallKBytes = smallKSketch.toByteArray();
    final MemorySegment smallKSeg = MemorySegment.ofArray(smallKBytes);

    rlu = ReservoirLongsUnion.newInstance(maxK);
    rlu.update(smallKSeg);
    assertNotNull(rlu.getResult());
    assertTrue(rlu.getResult().getK() < maxK);
    assertEquals(rlu.getResult().getK(), smallK);
    assertEquals(rlu.getResult().getN(), maxK);

    // sketch k < maxK and in exact mode
    final ReservoirLongsSketch smallKExactSketch = getBasicSketch(smallK, smallK);
    final byte[] smallKExactBytes = smallKExactSketch.toByteArray();
    final MemorySegment smallKExactSeg = MemorySegment.ofArray(smallKExactBytes);

    rlu = ReservoirLongsUnion.newInstance(maxK);
    rlu.update(smallKExactSeg);
    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), maxK);
    assertEquals(rlu.getResult().getN(), smallK);
  }

  @Test
  public void checkStandardMergeNoCopy() {
    final int k = 1024;
    final int n1 = 256;
    final int n2 = 256;
    final ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    final ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);

    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), n1 + n2);

    // creating from MemorySegment should avoid a copy
    final int n3 = 2048;
    final ReservoirLongsSketch sketch3 = getBasicSketch(n3, k);
    final byte[] sketch3Bytes = sketch3.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(sketch3Bytes);
    rlu.update(seg);

    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2 + n3);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkStandardMergeWithCopy() {
    // this will check the other code route to a standard merge,
    // but will copy sketch2 to be non-destructive.
    final int k = 1024;
    final int n1 = 768;
    final int n2 = 2048;
    final ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    final ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);
    rlu.update(10);

    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2 + 1);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test
  public void checkWeightedMerge() {
    final int k = 1024;
    final int n1 = 16384;
    final int n2 = 2048;
    final ReservoirLongsSketch sketch1 = getBasicSketch(n1, k);
    final ReservoirLongsSketch sketch2 = getBasicSketch(n2, k);

    ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(k);
    rlu.update(sketch1);
    rlu.update(sketch2);

    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), k);

    // now merge into the sketch for updating -- results should match
    rlu = ReservoirLongsUnion.newInstance(k);
    rlu.update(sketch2);
    rlu.update(sketch1);

    assertNotNull(rlu.getResult());
    assertEquals(rlu.getResult().getK(), k);
    assertEquals(rlu.getResult().getN(), n1 + n2);
    assertEquals(rlu.getResult().getNumSamples(), k);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadPreLongs() {
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(1024);
    final MemorySegment seg = MemorySegment.ofArray(rlu.toByteArray());
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 0); // corrupt the preLongs count

    ReservoirLongsUnion.heapify(seg);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSerVer() {
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(1024);
    final MemorySegment seg = MemorySegment.ofArray(rlu.toByteArray());
    seg.set(JAVA_BYTE, SER_VER_BYTE, (byte) 0); // corrupt the serialization version

    ReservoirLongsUnion.heapify(seg);
    fail();
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadFamily() {
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.newInstance(1024);
    final MemorySegment seg = MemorySegment.ofArray(rlu.toByteArray());
    seg.set(JAVA_BYTE, FAMILY_BYTE, (byte) 0); // corrupt the family ID

    ReservoirLongsUnion.heapify(seg);
    fail();
  }

  private static void validateUnionEquality(final ReservoirLongsUnion rlu1,
                                            final ReservoirLongsUnion rlu2) {
    assertEquals(rlu1.getMaxK(), rlu2.getMaxK());

    ReservoirLongsSketchTest.validateReservoirEquality(rlu1.getResult(), rlu2.getResult());
  }

  private static ReservoirLongsSketch getBasicSketch(final int n, final int k) {
    final ReservoirLongsSketch rls = ReservoirLongsSketch.newInstance(k);

    for (int i = 0; i < n; ++i) {
      rls.update(i);
    }

    return rls;
  }

  /**
   * Wrapper around System.out.println() allowing a simple way to disable logging in tests
   *
   * @param msg The message to print
   */
  private static void println(final String msg) {
    //System.out.println(msg);
  }
}
