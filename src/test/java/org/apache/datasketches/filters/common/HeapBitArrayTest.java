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
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class HeapBitArrayTest {

  @Test
  public void createBitArrayTest() {
    final HeapBitArray ba = new HeapBitArray(119);
    assertEquals(ba.getCapacity(), 128); // nearest multiple of 64
    assertEquals(ba.getArrayLength(), 2);
    assertEquals(ba.getNumBitsSet(), 0);
    assertTrue(ba.isEmpty());

    assertFalse(ba.hasMemory());
    assertFalse(ba.isDirect());
    assertFalse(ba.isReadOnly());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createNegativeSizeBitArrayTest() {
    new HeapBitArray(-64);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void createTooLargeBitArrayTest() {
    new HeapBitArray(1L + (long) Integer.MAX_VALUE * Long.SIZE);
  }

  @Test
  public void basicOperationTest() {
    final HeapBitArray ba = new HeapBitArray(128);
    assertEquals(ba.getAndSetBit(1), false);
    assertEquals(ba.getAndSetBit(2), false);
    for (int i = 4; i < 64; i <<= 1) {
      ba.getAndSetBit(64 + i);
    }
    assertEquals(ba.getNumBitsSet(), 6);
    assertEquals(ba.getBit(68), true);
    assertFalse(ba.isEmpty());

    assertFalse(ba.getBit(5));
    ba.setBit(5);
    assertTrue(ba.getAndSetBit(5));
    assertEquals(ba.getNumBitsSet(), 7);

    ba.reset();
    assertTrue(ba.isEmpty());
    assertEquals(ba.getNumBitsSet(), 0);

    ba.setLong(0, -1);
    assertTrue(ba.getBit(60));
    ba.clearBit(60);
    assertFalse(ba.getBit(60));

    assertTrue(ba.getBit(35));
    ba.assignBit(35, false);
    assertFalse(ba.getBit(35));
    ba.assignBit(35, true);
    assertTrue(ba.getBit(35));

    assertTrue(String.valueOf(ba).length() > 0);
  }

  @Test
  public void getBitsFromToTest() {
    final HeapBitArray ba = new HeapBitArray(128);

    // single, full long test
    ba.setLong(0, 0x5555555555555555L);
    assertEquals(ba.getBits(0, 64), 0x5555555555555555L);
    assertEquals(ba.getBits(64, 64), 0);

    // subset of single long, mostly ones with a stretch of zeros
    ba.setLong(1, 0xFFFFFFFFFC003FFFL);
    assertEquals(ba.getBits(64, 64), 0xFFFFFFFFFC003FFFL);
    assertEquals(ba.getBits(78, 12), 0);
    assertEquals(ba.getBits(77, 14), 8193);

    // spanning longs
    assertEquals(ba.getBits(60, 20), 0x3FFF5);
  }

  @Test
  public void setBitsFromToTest() {
    HeapBitArray ba = new HeapBitArray(128);

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
    ba = new HeapBitArray(10000);
    ba.setBits(601 * 10 + 3, 7, 125);
    assertEquals(ba.getBits(601 * 10 + 3, 7), 125);
  }

  @Test
  public void bitAddresOutOfBoundsTest() {
    final HeapBitArray ba = new HeapBitArray(1024);
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.getBit(-10));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.getBit(2048));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.setBit(-20));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.setBit(4096));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.getAndSetBit(-30));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ba.getAndSetBit(8192));
  }

  @Test
  public void inversionTest() {
    final int numBits = 1024;
    final HeapBitArray ba = new HeapBitArray(numBits);
    for (int i = 0; i < numBits; i += numBits / 8) {
      ba.getAndSetBit(i);
    }
    assertTrue(ba.getBit(0));

    final long numSet = ba.getNumBitsSet();
    ba.invert();

    assertEquals(ba.getNumBitsSet(), numBits - numSet);
    assertFalse(ba.getBit(0));

    // update to make dirty and invert again
    ba.setBit(0);
    ba.invert();
    assertEquals(ba.getNumBitsSet(), numSet - 1);
    assertFalse(ba.getBit(0));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidUnionTest() {
    final HeapBitArray ba = new HeapBitArray(128);
    ba.union(new HeapBitArray(64));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidIntersectionTest() {
    final HeapBitArray ba = new HeapBitArray(128);
    ba.intersect(new HeapBitArray(64));
  }

  @Test
  public void validUnionAndIntersectionTest() {
    final HeapBitArray ba1 = new HeapBitArray(64);
    final HeapBitArray ba2 = new HeapBitArray(64);
    final HeapBitArray ba3 = new HeapBitArray(64);

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

  @Test
  public void serializeEmptyTest() {
    final HeapBitArray ba = new HeapBitArray(64);
    final WritableBuffer wbuf = WritableMemory.allocate((int) ba.getSerializedSizeBytes()).asWritableBuffer();
    ba.writeToBuffer(wbuf);
    wbuf.resetPosition();
    final HeapBitArray newBA = HeapBitArray.heapify(wbuf, true);
    assertEquals(newBA.getArrayLength(), ba.getArrayLength());
    assertEquals(newBA.getCapacity(), ba.getCapacity());
    assertEquals(newBA.getNumBitsSet(), ba.getNumBitsSet());
    assertTrue(newBA.isEmpty());
  }

  @Test
  public void serializeNonEmptyTest() {
    final long n = 8192;
    final HeapBitArray ba = new HeapBitArray(n);
    for (int i = 0; i < n; i += 3)
      ba.getAndSetBit(i);
    final WritableBuffer wbuf = WritableMemory.allocate((int) ba.getSerializedSizeBytes()).asWritableBuffer();
    ba.writeToBuffer(wbuf);
    wbuf.resetPosition();
    final HeapBitArray newBA = HeapBitArray.heapify(wbuf, false);
    assertEquals(newBA.getArrayLength(), ba.getArrayLength());
    assertEquals(newBA.getCapacity(), ba.getCapacity());
    assertEquals(newBA.getNumBitsSet(), ba.getNumBitsSet());
    assertFalse(newBA.isEmpty());
  }
}
