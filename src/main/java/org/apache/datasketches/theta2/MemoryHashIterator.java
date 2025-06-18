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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

import java.lang.foreign.MemorySegment;

/**
 * @author Lee Rhodes
 */
final class MemoryHashIterator implements HashIterator {
  private MemorySegment seg;
  private int arrLongs;
  private long thetaLong;
  private long offsetBytes;
  private int index;
  private long hash;

  MemoryHashIterator(final MemorySegment srcSeg, final int arrLongs, final long thetaLong) {
    this.seg = srcSeg;
    this.arrLongs = arrLongs;
    this.thetaLong = thetaLong;
    offsetBytes = PreambleUtil.extractPreLongs(srcSeg) << 3;
    index = -1;
    hash = 0;
  }

  @Override
  public long get() {
    return hash;
  }

  @Override
  public boolean next() {
    while (++index < arrLongs) {
      hash = seg.get(JAVA_LONG_UNALIGNED, offsetBytes + (index << 3));
      if ((hash != 0) && (hash < thetaLong)) {
        return true;
      }
    }
    return false;
  }

}
