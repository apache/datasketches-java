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

import static org.testng.Assert.assertNotNull;

import org.apache.datasketches.memory.XxHash;
import org.testng.annotations.Test;

public class BloomFilterTest {

  @Test
  public void checkHash() {
    final long seed = 985403L;
    final int numBits = 23;
    final long longVal = 8592L;
    final int intVal = 8592;
    System.out.println((XxHash.hashLong(longVal, seed) >>> 1) % numBits);
    final int[] intArr = { intVal };
    System.out.println((XxHash.hashIntArr(intArr, 0, intArr.length, seed) >>> 1) % numBits);
  }

  @Test
  public void testBitArray() {
    BitArray ba = new BitArray(120);
    int numNewSets = 0;
    numNewSets += ba.getAndSetBit(0) ? 1 : 0;
    numNewSets += ba.getAndSetBit(2) ? 1 : 0;
    numNewSets += ba.getAndSetBit(4) ? 1 : 0;
    numNewSets += ba.getAndSetBit(64) ? 1 : 0;
    numNewSets += ba.getAndSetBit(4) ? 1 : 0;
    numNewSets += ba.getAndSetBit(64) ? 1 : 0;
    numNewSets += ba.getAndSetBit(63) ? 1 : 0;
    System.out.println("Num repeat sets: " + numNewSets);
    System.out.println(ba);
  }

  @Test
  public void createBasicFilter() {
    BloomFilter bf = new BloomFilter(2048, 3);
    assertNotNull(bf);
    final long n = 1000;

    for (long i = 0; i < n; ++i) {
      bf.checkAndUpdate(i);
    }

    System.out.println("Num bits set: " + bf.getBitsUsed());
    System.out.println("% Capacity: " + bf.getFillPercentage());
    System.out.println(bf);

    int numFound = 0;
    for (long i = n / 2; i < 2 * n; ++i)
      if (bf.check(i))
        ++numFound;
    System.out.println("Num found: " + numFound);
  }
}

