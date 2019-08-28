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
 * Methods of serializing and deserializing arrays of Double.
 *
 * @author Alexander Saydakov
 */
public class ArrayOfDoublesSerDe extends ArrayOfItemsSerDe<Double> {

  @Override
  public byte[] serializeToByteArray(final Double[] items) {
    final byte[] bytes = new byte[Double.BYTES * items.length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putDouble(offsetBytes, items[i]);
      offsetBytes += Double.BYTES;
    }
    return bytes;
  }

  @Override
  public Double[] deserializeFromMemory(final Memory mem, final int length) {
    UnsafeUtil.checkBounds(0, Double.BYTES, mem.getCapacity());
    final Double[] array = new Double[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getDouble(offsetBytes);
      offsetBytes += Double.BYTES;
    }
    return array;
  }

}
