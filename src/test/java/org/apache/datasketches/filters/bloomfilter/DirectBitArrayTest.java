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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_CHAR_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.positional.PositionalSegment;
import org.apache.datasketches.filters.bloomfilter.BitArray;
import org.apache.datasketches.filters.bloomfilter.DirectBitArray;
import org.apache.datasketches.filters.bloomfilter.HeapBitArray;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public class DirectBitArrayTest {

  private static MemorySegment bitArrayToMemorySegment(final HeapBitArray ba) {
    // assumes we're using small enough an array to test that
    // size can be measured with an int
    final int numBytes = (int) ba.getSerializedSizeBytes();
    final byte[] bytes = new byte[numBytes];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    ba.writeToSegmentAsStream(PositionalSegment.wrap(wseg));

    return wseg;
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void writableWrapEmptyBitArrayTest() {
    final HeapBitArray hba = new HeapBitArray(119);
    assertTrue(hba.isEmpty());

    final MemorySegment seg = bitArrayToMemorySegment(hba);
    DirectBitArray.writableWrap(seg, hba.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createNegativeSizeBitArrayTest() {
    final byte[] bytes = new byte[32];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    wseg.set(JAVA_INT_UNALIGNED, 0, -1); // negative length
    DirectBitArray.writableWrap(wseg, true);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void tooSmallCapacityTest() {
    final byte[] bytes = new byte[32];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    wseg.set(JAVA_INT_UNALIGNED, 0, 1024); // array length in longs
    wseg.set(JAVA_LONG_UNALIGNED, 8, 201); // number of bits set (non-empty)
    DirectBitArray.writableWrap(wseg, false);
  }

  // no text of max size because the BitArray allows up to Integer.MAX_VALUE

  @Test
  public void initializeTooSmallTest() {
    final byte[] bytes = new byte[128];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    assertThrows(SketchesArgumentException.class, () -> DirectBitArray.initialize(128 * 65, wseg));
    assertThrows(SketchesArgumentException.class, () -> DirectBitArray.initialize(-5, wseg));
  }

  @Test
  public void basicInitializeOperationsTest() {
    final byte[] bytes = new byte[56];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);

    final DirectBitArray dba = DirectBitArray.initialize(192, wseg);
    assertTrue(dba.isEmpty());
    assertTrue(dba.hasMemorySegment());
    assertFalse(dba.isReadOnly());
    assertEquals(dba.getNumBitsSet(), 0);

    assertFalse(dba.getAndSetBit(13));
    assertTrue(dba.getBit(13));
    dba.setBit(17);
    assertTrue(dba.getAndSetBit(17));
    assertEquals(dba.getArrayLength(), 3);
    assertFalse(dba.isEmpty());
    assertFalse(dba.getBit(183));

    assertTrue(dba.isDirty());
    assertEquals(dba.getNumBitsSet(), 2);
    assertFalse(dba.isDirty());

    dba.reset();
    assertTrue(dba.isEmpty());
    assertTrue(dba.hasMemorySegment());
    assertFalse(dba.isReadOnly());
    assertEquals(dba.getNumBitsSet(), 0);
  }

  @Test
  public void basicWritableWrapTest() {
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
    final DirectBitArray dba = DirectBitArray.writableWrap(seg, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());

    assertTrue(dba.hasMemorySegment());
    assertFalse(dba.isOffHeap());
    assertFalse(dba.isReadOnly());

    assertFalse(dba.getAndSetBit(75));
    dba.setBit(100);
    assertTrue(dba.getAndSetBit(100));
    assertEquals(dba.getNumBitsSet(), 8);
  }

  @Test
  public void countWritableWrappedBitsWhenDirty() {
    // like basicOperationTest but with setBit which does
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
    final DirectBitArray dba = DirectBitArray.writableWrap(seg, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());

    dba.setBit(102);
    assertTrue(dba.isDirty());
  }

  @Test
  public void bitAddresOutOfBoundsNonEmptyTest() {
    final int numBits = 1024;
    final byte[] bytes = new byte[((numBits / 64) + 2) * Long.BYTES];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    final DirectBitArray dba = DirectBitArray.initialize(numBits, wseg);

    for (int i = 0; i < numBits; i += numBits / 8) {
      dba.getAndSetBit(i);
    }

    assertThrows(IndexOutOfBoundsException.class, () -> dba.getBit(-10));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.getBit(2048));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.setBit(-20));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.setBit(4096));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.getAndSetBit(-30));
    assertThrows(IndexOutOfBoundsException.class, () -> dba.getAndSetBit(8192));
  }

  @Test
  public void inversionTest() {
    final int numBits = 1024;
    final byte[] bytes = new byte[((numBits / 64) + 2) * Long.SIZE];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    final DirectBitArray dba = DirectBitArray.initialize(numBits, wseg);

    for (int i = 0; i < numBits; i += numBits / 8) {
      dba.getAndSetBit(i);
    }
    assertTrue(dba.getBit(0));

    final long numSet = dba.getNumBitsSet();
    dba.invert();

    assertEquals(dba.getNumBitsSet(), numBits - numSet);
    assertFalse(dba.getBit(0));

    // update to make dirty and invert again
    dba.setBit(0);
    dba.invert();
    assertEquals(dba.getNumBitsSet(), numSet - 1);
    assertFalse(dba.getBit(0));
  }

  @Test
  public void invalidUnionIntersectionTest() {
    final HeapBitArray hba = new HeapBitArray(128);
    hba.setBit(0);
    final MemorySegment wseg = bitArrayToMemorySegment(hba);
    final DirectBitArray dba = DirectBitArray.writableWrap(wseg, false);
    assertThrows(SketchesArgumentException.class, () -> dba.union(new HeapBitArray(64)));
    assertThrows(SketchesArgumentException.class, () -> dba.intersect(new HeapBitArray(512)));
  }

  @Test
  public void validUnionAndIntersectionTest() {
    final long numBits = 64;
    final int sizeBytes = (int) BitArray.getSerializedSizeBytes(64);
    final DirectBitArray ba1 = DirectBitArray.initialize(numBits, MemorySegment.ofArray(new byte[sizeBytes]));
    final DirectBitArray ba2 = DirectBitArray.initialize(numBits, MemorySegment.ofArray(new byte[sizeBytes]));
    final DirectBitArray ba3 = DirectBitArray.initialize(numBits, MemorySegment.ofArray(new byte[sizeBytes]));

    final int n = 10;
    for (int i = 0; i < n; ++i) {
      ba1.getAndSetBit(i);
      ba2.getAndSetBit(i + (n / 2));
      ba3.getAndSetBit(2 * i);
    }
    assertEquals(ba1.getNumBitsSet(), n);
    assertEquals(ba2.getNumBitsSet(), n);
    assertEquals(ba3.getNumBitsSet(), n);

    ba1.intersect(ba2);
    assertEquals(ba1.getNumBitsSet(), n / 2);

    ba3.union(ba2);
    assertEquals(ba3.getNumBitsSet(), (3 * n) / 2);
  }
}
