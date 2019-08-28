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

/**
 * @author Lee Rhodes
 */
class HeapHashIterator implements HashIterator {
  private long[] cache;
  private int arrLongs;
  private long thetaLong;
  private int index;
  private long hash;

  HeapHashIterator(final long[] cache, final int arrLongs, final long thetaLong) {
    this.cache = cache;
    this.arrLongs = arrLongs;
    this.thetaLong = thetaLong;
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
      hash = cache[index];
      if ((hash != 0) && (hash < thetaLong)) {
        return true;
      }
    }
    return false;
  }

}
