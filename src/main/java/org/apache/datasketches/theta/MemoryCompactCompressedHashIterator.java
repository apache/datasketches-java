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

import static org.apache.datasketches.theta.PreambleUtil.wholeBytesToHoldBits;

import org.apache.datasketches.memory.Memory;

/*
 * This is to uncompress serial version 4 sketch incrementally
 */
class MemoryCompactCompressedHashIterator implements HashIterator {
  private Memory mem;
  private int offset;
  private int entryBits;
  private int numEntries;
  private int index;
  private long previous;
  private int offsetBits;
  private long[] buffer;
  private byte[] bytes;
  private boolean isBlockMode;
  private boolean isFirstUnpack1;

  MemoryCompactCompressedHashIterator(
      final Memory mem,
      final int offset,
      final int entryBits,
      final int numEntries
  ) {
    this.mem = mem;
    this.offset = offset;
    this.entryBits = entryBits;
    this.numEntries = numEntries;
    index = -1;
    previous = 0;
    offsetBits = 0;
    buffer = new long[8];
    bytes = new byte[entryBits];
    isBlockMode = numEntries >= 8;
    isFirstUnpack1 = true;
  }

  @Override
  public long get() {
    return buffer[index & 7];
  }

  @Override
  public boolean next() {
    if (++index == numEntries) { return false; }
    if (isBlockMode) {
      if ((index & 7) == 0) {
        if (numEntries - index >= 8) {
          unpack8();
        } else {
          isBlockMode = false;
          unpack1();
        }
      }
    } else {
      unpack1();
    }
    return true;
  }

  private void unpack1() {
    if (isFirstUnpack1) {
      mem.getByteArray(offset, bytes, 0, wholeBytesToHoldBits((numEntries - index) * entryBits));
      offset = 0;
      isFirstUnpack1 = false;
    }
    final int i = index & 7;
    BitPacking.unpackBits(buffer, i, entryBits, bytes, offset, offsetBits);
    offset += (offsetBits + entryBits) >>> 3;
    offsetBits = (offsetBits + entryBits) & 7;
    buffer[i] += previous;
    previous = buffer[i];
  }

  private void unpack8() {
    mem.getByteArray(offset, bytes, 0, entryBits);
    BitPacking.unpackBitsBlock8(buffer, 0, bytes, 0, entryBits);
    offset += entryBits;
    for (int i = 0; i < 8; i++) {
      buffer[i] += previous;
      previous = buffer[i];
    }
  }
}
