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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class BitArrayTest {

  @Test
  public void createBitArrayTest() {
    final HeapBitArray ba = new HeapBitArray(119);
    assertEquals(ba.getCapacity(), 128); // nearest multiple of 64
    assertEquals(ba.getArrayLength(), 2);
    assertEquals(ba.getNumBitsSet(), 0);
    assertTrue(ba.isEmpty());
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
