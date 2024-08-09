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
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class DirectBitArrayRTest {

  private static Memory bitArrayToMemory(HeapBitArray ba) {
    // assumes we're using small enough an array to test that
    // size can be measured with an int
    final int numBytes = (int) ba.getSerializedSizeBytes();
    final byte[] bytes = new byte[numBytes];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    ba.writeToBuffer(wmem.asWritableBuffer());

    return wmem;
  }

  @Test
  public void createBitArrayTest() {
    final HeapBitArray hba = new HeapBitArray(119);
    assertTrue(hba.isEmpty());

    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());
    assertTrue(dba.isEmpty());
    assertEquals(dba.getCapacity(), 128); // nearest multiple of 64
    assertEquals(dba.getArrayLength(), 2);
    assertEquals(dba.getNumBitsSet(), 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createNegativeSizeBitArrayTest() {
    final byte[] bytes = new byte[32];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    wmem.putInt(0, -1); // negative length
    DirectBitArrayR.wrap(wmem, true);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void tooSmallCapacityTest() {
    final byte[] bytes = new byte[32];
    final WritableMemory wmem = WritableMemory.writableWrap(bytes);
    wmem.putInt(0, 1024); // array length in longs
    wmem.putLong(8, 201); // number of bits set (non-empty)
    DirectBitArrayR.wrap(wmem, false);
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

    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());

    assertTrue(dba.hasMemory());
    assertFalse(dba.isDirect());
    assertTrue(dba.isReadOnly());
  }

  @Test
  public void getBitsFromToTest() {
    final HeapBitArray hba = new HeapBitArray(128);
    hba.setBit(1); // will override, but this forces non-empty
    hba.setLong(0, 0x5555555555555555L);
    hba.setLong(1, 0xFFFFFFFFFC003FFFL);
    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());

    // single, full long test
    assertEquals(dba.getBits(0, 64), 0x5555555555555555L);

    // subset of single long, mostly ones with a stretch of zeros
    assertEquals(dba.getBits(64, 64), 0xFFFFFFFFFC003FFFL);
    assertEquals(dba.getBits(78, 12), 0);
    assertEquals(dba.getBits(77, 14), 8193);

    // spanning longs
    assertEquals(dba.getBits(60, 20), 0x3FFF5);
  }

  @Test
  public void countBitsWhenDirty() {
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

    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());
    assertEquals(dba.getNumBitsSet(), 6);
    assertTrue(dba.getBit(68));
    assertFalse(dba.isEmpty());
    assertFalse(dba.isDirty());
  }

  @Test
  public void bitAddressOutOfBoundsEmptyTest() {
    final int numBits = 256;
    final HeapBitArray hba = new HeapBitArray(numBits);
    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());
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

    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());
    assertThrows(AssertionError.class, () -> dba.getBit(-10));
    assertThrows(AssertionError.class, () -> dba.getBit(2048));
  }

  @Test
  public void checkInvalidMethods() {
    final int numBits = 1024;
    final HeapBitArray hba = new HeapBitArray(numBits);
    for (int i = 0; i < numBits; i += numBits / 8) {
      hba.getAndSetBit(i);
    }

    final Memory mem = bitArrayToMemory(hba);
    DirectBitArrayR dba = DirectBitArrayR.wrap(mem, hba.isEmpty());

    // all of these try to modify a read-only memory
    assertThrows(SketchesReadOnlyException.class, () -> dba.setBit(14));
    assertThrows(SketchesReadOnlyException.class, () -> dba.clearBit(7));
    assertThrows(SketchesReadOnlyException.class, () -> dba.assignBit(924, false));
    assertThrows(SketchesReadOnlyException.class, () -> dba.setBits(100, 30, 0xFF));
    assertThrows(SketchesReadOnlyException.class, () -> dba.getAndSetBit(100));
    assertThrows(SketchesReadOnlyException.class, () -> dba.reset());
    assertThrows(SketchesReadOnlyException.class, () -> dba.invert());
    assertThrows(SketchesReadOnlyException.class, () -> dba.intersect(hba));
    assertThrows(SketchesReadOnlyException.class, () -> dba.union(hba));
  }
}
