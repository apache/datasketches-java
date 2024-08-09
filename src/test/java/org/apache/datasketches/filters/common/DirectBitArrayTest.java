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

package org.apache.datasketches.filters.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class DirectBitArrayTest {

  private static WritableMemory bitArrayToWritableMemory(HeapBitArray ba) {
    // assumes we're using small enough an array to test that
    // size can be measured with an int
    final int numBytes = (int) ba.getSerializedSizeBytes();
    final byte[] bytes = new byte[numBytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    ba.writeToBuffer(wmem.asWritableBuffer());

    return wmem;
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void writableWrapEmptyBitArrayTest() {
    final HeapBitArray hba = new HeapBitArray(119);
    assertTrue(hba.isEmpty());

    final WritableMemory mem = bitArrayToWritableMemory(hba);
    DirectBitArray.writableWrap(mem, hba.isEmpty());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createNegativeSizeBitArrayTest() {
    final byte[] bytes = new byte[32];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    wmem.putInt(0, -1); // negative length
    DirectBitArray.writableWrap(wmem, true);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void tooSmallCapacityTest() {
    final byte[] bytes = new byte[32];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    wmem.putInt(0, 1024); // array length in longs
    wmem.putLong(8, 201); // number of bits set (non-empty)
    DirectBitArray.writableWrap(wmem, false);
  }

  // no text of max size because the BitArray allows up to Integer.MAX_VALUE
  // bits, which is the maximum size of an array in Java -- can't use it all,
  // (need 2 longs for preamble) but also can't allocate that large to test on most machines

  @Test
  public void initializeTooSmallTest() {
    final byte[] bytes = new byte[128];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    assertThrows(SketchesArgumentException.class, () -> DirectBitArray.initialize(128 * 65, wmem));
    assertThrows(SketchesArgumentException.class, () -> DirectBitArray.initialize(-5, wmem));
  }

  @Test
  public void basicInitializeOperationsTest() {
    final byte[] bytes = new byte[56];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);

    DirectBitArray dba = DirectBitArray.initialize(192, wmem);
    assertTrue(dba.isEmpty());
    assertTrue(dba.hasMemory());
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
    assertTrue(dba.hasMemory());
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

    final WritableMemory mem = bitArrayToWritableMemory(hba);
    DirectBitArray dba = DirectBitArray.writableWrap(mem, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());

    assertTrue(dba.hasMemory());
    assertFalse(dba.isDirect());
    assertFalse(dba.isReadOnly());

    assertFalse(dba.getAndSetBit(75));
    dba.setBit(100);
    assertTrue(dba.getAndSetBit(100));
    assertEquals(dba.getNumBitsSet(), 8);

    dba.reset();
    assertTrue(dba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 0);

    dba.setBit(0);
    dba.setLong(0, -1);
    assertTrue(dba.getBit(60));
    dba.clearBit(60);
    assertFalse(dba.getBit(60));

    assertTrue(dba.getBit(35));
    dba.assignBit(35, false);
    assertFalse(dba.getBit(35));
    dba.assignBit(35, true);
    assertTrue(dba.getBit(35));
  }

  @Test
  public void getBitsFromToTest() {
    final int numBits = 128;
    final WritableMemory wmem = WritableMemory.writableWrap(new byte[32]);
    final DirectBitArray dba = DirectBitArray.initialize(numBits, wmem);

    // single, full long test
    dba.setBit(0); // useless but forces non-empty when using setLong()
    dba.setLong(0, 0x5555555555555555L);
    assertEquals(dba.getBits(0, 64), 0x5555555555555555L);
    assertEquals(dba.getBits(64, 64), 0);

    // subset of single long, mostly ones with a stretch of zeros
    dba.setLong(1, 0xFFFFFFFFFC003FFFL);
    assertEquals(dba.getBits(64, 64), 0xFFFFFFFFFC003FFFL);
    assertEquals(dba.getBits(78, 12), 0);
    assertEquals(dba.getBits(77, 14), 8193);

    // spanning longs
    assertEquals(dba.getBits(60, 20), 0x3FFF5);
  }

  @Test
  public void setBitsFromToTest() {
    final int numBits = 128;
    WritableMemory wmem = WritableMemory.writableWrap(new byte[32]);
    DirectBitArray ba = DirectBitArray.initialize(numBits, wmem);

    // within a single long
    ba.setBits(0, 64, 0x80000000DAB8C730L);
    assertEquals(ba.getLong(0), 0x80000000DAB8C730L);
    assertEquals(ba.getLong(1), 0);

    ba.setBits(40, 8, 0xA6);
    assertEquals(ba.getLong(0), 0x8000A600DAB8C730L);

    // spanning longs
    ba.setBits(60, 20, 0x3FFF5);
    assertEquals(ba.getLong(0), 0x5000A600DAB8C730L);
    assertEquals(ba.getLong(1), 0x3FFFL);

    // found specific failure with this test
    wmem = WritableMemory.writableWrap(new byte[1272]);
    ba = DirectBitArray.initialize(10000, wmem);
    ba.setBits(601 * 10 + 3, 7, 125);
    assertEquals(ba.getBits(601 * 10 + 3, 7), 125);
  }

  @Test
  public void countWritableWrappedBitsWhenDirty() {
    // like basicOperationTest but with setBit which does
    // not neecssarily track numBitsSet_
    final HeapBitArray hba = new HeapBitArray(128);
    assertFalse(hba.getAndSetBit(1));
    assertFalse(hba.getAndSetBit(2));
    for (int i = 4; i < 64; i <<= 1) {
      hba.setBit(64 + i);
    }
    assertEquals(hba.getNumBitsSet(), 6);
    assertTrue(hba.getBit(68));
    assertFalse(hba.isEmpty());

    final WritableMemory mem = bitArrayToWritableMemory(hba);
    DirectBitArray dba = DirectBitArray.writableWrap(mem, hba.isEmpty());
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
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    final DirectBitArray dba = DirectBitArray.initialize(numBits, wmem);

    for (int i = 0; i < numBits; i += numBits / 8) {
      dba.getAndSetBit(i);
    }

    assertThrows(AssertionError.class, () -> dba.getBit(-10));
    assertThrows(AssertionError.class, () -> dba.getBit(2048));
    assertThrows(AssertionError.class, () -> dba.setBit(-20));
    assertThrows(AssertionError.class, () -> dba.setBit(4096));
    assertThrows(AssertionError.class, () -> dba.getAndSetBit(-30));
    assertThrows(AssertionError.class, () -> dba.getAndSetBit(8192));
  }

  @Test
  public void inversionTest() {
    final int numBits = 1024;
    final byte[] bytes = new byte[((numBits / 64) + 2) * Long.SIZE];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    final DirectBitArray dba = DirectBitArray.initialize(numBits, wmem);

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
    final WritableMemory wmem = bitArrayToWritableMemory(hba);
    final DirectBitArray dba = DirectBitArray.writableWrap(wmem, false);
    assertThrows(SketchesArgumentException.class, () -> dba.union(new HeapBitArray(64)));
    assertThrows(SketchesArgumentException.class, () -> dba.intersect(new HeapBitArray(512)));
  }

  @Test
  public void validUnionAndIntersectionTest() {
    final long numBits = 64;
    final int sizeBytes = (int) BitArray.getSerializedSizeBytes(64);
    final DirectBitArray ba1 = DirectBitArray.initialize(numBits, WritableMemory.allocate(sizeBytes));
    final DirectBitArray ba2 = DirectBitArray.initialize(numBits, WritableMemory.allocate(sizeBytes));
    final DirectBitArray ba3 = DirectBitArray.initialize(numBits, WritableMemory.allocate(sizeBytes));

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
    assertEquals(ba3.getNumBitsSet(), 3 * n / 2);
  }
}
