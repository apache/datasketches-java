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

package org.apache.datasketches;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.UnsafeUtil;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of Boolean as a bit array.
 *
 * @author Jon Malkin
 */
public class ArrayOfBooleansSerDe extends ArrayOfItemsSerDe<Boolean> {
  /**
   * Computes number of bytes needed for packed bit encoding of the array of booleans. Rounds
   * partial bytes up to return a whole number of bytes.
   *
   * @param arrayLength Number of items in the array to serialize
   * @return Number of bytes needed to encode the array
   */
  public static int computeBytesNeeded(final int arrayLength) {
    return (arrayLength >>> 3) + ((arrayLength & 0x7) > 0 ? 1 : 0);
  }

  @Override
  public byte[] serializeToByteArray(final Boolean[] items) {
    final int bytesNeeded = computeBytesNeeded(items.length);
    final byte[] bytes = new byte[bytesNeeded];
    final WritableMemory mem = WritableMemory.wrap(bytes);

    byte val = 0;
    for (int i = 0; i < items.length; ++i) {
      if (items[i]) {
        val |= 0x1 << (i & 0x7);
      }

      if ((i & 0x7) == 0x7) {
        mem.putByte(i >>> 3, val);
        val = 0;
      }
    }

    // write out any remaining values (if val=0, still good to be explicit)
    if ((items.length & 0x7) > 0) {
      mem.putByte(bytesNeeded - 1, val);
    }

    return bytes;
  }

  @Override
  public Boolean[] deserializeFromMemory(final Memory mem, final int length) {
    final int numBytes = computeBytesNeeded(length);
    UnsafeUtil.checkBounds(0, numBytes, mem.getCapacity());
    final Boolean[] array = new Boolean[length];

    byte srcVal = 0;
    for (int i = 0, b = 0; i < length; ++i) {
      if ((i & 0x7) == 0x0) { // should trigger on first iteration
        srcVal = mem.getByte(b++);
      }
      array[i] = ((srcVal >>> (i & 0x7)) & 0x1) == 1;
    }

    return array;
  }
}
