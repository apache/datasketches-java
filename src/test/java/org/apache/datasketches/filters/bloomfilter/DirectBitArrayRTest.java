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

package org.apache.datasketches.filters.bloomfilter;

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.positional.PositionalSegment;
import org.apache.datasketches.filters.bloomfilter.DirectBitArrayR;
import org.apache.datasketches.filters.bloomfilter.HeapBitArray;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.testng.annotations.Test;

public class DirectBitArrayRTest {

  private static MemorySegment bitArrayToMemorySegment(final HeapBitArray ba) {
    // assumes we're using small enough an array to test that
    // size can be measured with an int
    final int numBytes = (int) ba.getSerializedSizeBytes();
    final byte[] bytes = new byte[numBytes];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    ba.writeToSegmentAsStream(PositionalSegment.wrap(wseg));

    return wseg;
  }

  @Test
  public void createBitArrayTest() {
    final HeapBitArray hba = new HeapBitArray(119);
    assertTrue(hba.isEmpty());

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());
    assertTrue(dba.isEmpty());
    assertEquals(dba.getCapacity(), 128); // nearest multiple of 64
    assertEquals(dba.getArrayLength(), 2);
    assertEquals(dba.getNumBitsSet(), 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createNegativeSizeBitArrayTest() {
    final byte[] bytes = new byte[32];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    wseg.set(JAVA_INT_UNALIGNED, 0, -1); // negative length
    DirectBitArrayR.wrap(wseg, true);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void tooSmallCapacityTest() {
    final byte[] bytes = new byte[32];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    wseg.set(JAVA_INT_UNALIGNED, 0, 1024); // array length in longs
    wseg.set(JAVA_LONG_UNALIGNED, 8, 201); // number of bits seg (non-empty)
    DirectBitArrayR.wrap(wseg, false);
  }

  // no text of max size because the BitArray allows up to Integer.MAX_VALUE

  @Test
  public void basicOperationTest() {
    final HeapBitArray hba = new HeapBitArray(128);
    assertFalse(hba.getAndSetBit(1));
    assertFalse(hba.getAndSetBit(2));
    for (int i = 4; i < 64; i <<= 1) {
      hba.getAndSetBit(64 + i);
    }
    assertEquals(hba.getNumBitsSet(), 6);
    assertTrue(hba.getBit(68));
    assertFalse(hba.isEmpty());

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());

    assertTrue(dba.hasMemorySegment());
    assertFalse(dba.isOffHeap());
    assertTrue(dba.isReadOnly());
  }

  @Test
  public void countBitsWhenDirty() {
    // like basicOperationTest but with segBit which does
    // not necessarily track numBitsSet_
    final HeapBitArray hba = new HeapBitArray(128);
    assertFalse(hba.getAndSetBit(1));
    assertFalse(hba.getAndSetBit(2));
    for (int i = 4; i < 64; i <<= 1) {
      hba.setBit(64 + i);
    }
    assertEquals(hba.getNumBitsSet(), 6);
    assertTrue(hba.getBit(68));
    assertFalse(hba.isEmpty());

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());
  }

  @Test
  public void bitAddressOutOfBoundsEmptyTest() {
    final int numBits = 256;
    final HeapBitArray hba = new HeapBitArray(numBits);
    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());
    assertFalse(dba.getBit(19));   // in range
    assertFalse(dba.getBit(-10));        // out of bounds
    assertFalse(dba.getBit(2048)); // out of bounds
  }

  @Test
  public void bitAddressOutOfBoundsNonEmptyTest() {
    final int numBits = 1024;
    final HeapBitArray hba = new HeapBitArray(numBits);
    for (int i = 0; i < numBits; i += numBits / 8) {
      hba.getAndSetBit(i);
    }

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());
    assertThrows(IndexOutOfBoundsException.class, () -> dba.getBit(-10));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.getBit(2048));
  }

  @Test
  public void checkInvalidMethods() {
    final int numBits = 1024;
    final HeapBitArray hba = new HeapBitArray(numBits);
    for (int i = 0; i < numBits; i += numBits / 8) {
      hba.getAndSetBit(i);
    }

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    final DirectBitArrayR dba = DirectBitArrayR.wrap(seg, hba.isEmpty());

    // all of these try to modify a read-only MemorySegment
    assertThrows(SketchesReadOnlyException.class, () -> dba.setBit(14));
    assertThrows(SketchesReadOnlyException.class, () -> dba.getAndSetBit(100));
    assertThrows(SketchesReadOnlyException.class, () -> dba.reset());
    assertThrows(SketchesReadOnlyException.class, () -> dba.invert());
    assertThrows(SketchesReadOnlyException.class, () -> dba.intersect(hba));
    assertThrows(SketchesReadOnlyException.class, () -> dba.union(hba));
  }
}
