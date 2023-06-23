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

package org.apache.datasketches.common;

import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of Double.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfDoublesSerDe extends ArrayOfItemsSerDe<Double> {

  @Override
  public byte[] serializeToByteArray(final Double item) {
    final byte[] byteArr = new byte[Double.BYTES];
    putDoubleLE(byteArr, 0, item.doubleValue());
    return byteArr;
  }

  @Override
  public byte[] serializeToByteArray(final Double[] items) {
    final byte[] bytes = new byte[Double.BYTES * items.length];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    long offset = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putDouble(offset, items[i]);
      offset += Double.BYTES;
    }
    return bytes;
  }

  @Override
  public Double deserializeOneFromMemory(final Memory mem, final long offset) {
    return mem.getDouble(offset);
  }

  @Override
  @Deprecated
  public Double[] deserializeFromMemory(final Memory mem, final int numItems) {
    return deserializeFromMemory(mem, 0, numItems);
  }

  @Override
  public Double[] deserializeFromMemory(final Memory mem, final long offsetBytes, final int numItems) {
    long offset = offsetBytes;
    Util.checkBounds(offset, Double.BYTES * numItems, mem.getCapacity());
    final Double[] array = new Double[numItems];

    for (int i = 0; i < numItems; i++) {
      array[i] = mem.getDouble(offset);
      offset += Double.BYTES;
    }
    return array;
  }

  @Override
  public int sizeOf(final Double item) { return Double.BYTES; }

  @Override
  public int sizeOf(final Double[] items) { return items.length * Double.BYTES; }

  @Override
  public int sizeOf(final Memory mem, final long offsetBytes, final int numItems) {
    return numItems * Double.BYTES;
  }

}
