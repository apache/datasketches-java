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

import org.apache.datasketches.common.ByteArrayUtil;

/*
 * This is to iterate over serial version 3 sketch representation
 */
final class BytesCompactHashIterator implements HashIterator {
  final private byte[] bytes;
  final private int offset;
  final private int numEntries;
  private int index;

  BytesCompactHashIterator(
      final byte[] bytes,
      final int offset,
      final int numEntries
  ) {
    this.bytes = bytes;
    this.offset = offset;
    this.numEntries = numEntries;
    index = -1;
  }

  @Override
  public long get() {
    return ByteArrayUtil.getLongLE(bytes, offset + index * Long.BYTES);
  }

  @Override
  public boolean next() {
    return ++index < numEntries;
 }
}
