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

package org.apache.datasketches.quantiles;

import java.util.Objects;

import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIteratorAPI;

/**
 * Iterator over QuantilesItemsSketch. The order is not defined.
 * @param <T> type of item
 */
public final class QuantilesItemsSketchIterator<T> implements QuantilesGenericSketchIteratorAPI<T> {
  private Object[] combinedBuffer;
  private long bitPattern;
  private int level;
  private long weight;
  private int index;
  private int offset;
  private int num;
  private int k;

  QuantilesItemsSketchIterator(final QuantilesItemsSketch<T> sketch, final long bitPattern) {
    Objects.requireNonNull(sketch, "sketch must not be null");
    combinedBuffer = sketch.combinedBuffer_;
    num = sketch.getBaseBufferCount();
    this.k = sketch.getK();
    this.bitPattern = bitPattern;
    this.level = -1;
    this.weight = 1;
    this.index = -1;
    this.offset = 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T getQuantile() {
    if (index < 0) { throw new SketchesStateException("index < 0; getQuantile() was called before next()"); }
    return (T) combinedBuffer[offset + index];
  }

  @Override
  public long getWeight() {
    return weight;
  }

  @Override
  public boolean next() {
    index++; // advance index within the current level
    if (index < num) {
      return true;
    }
    // go to the next non-empty level
    do {
      level++;
      if (level > 0) {
        bitPattern >>>= 1;
      }
      if (bitPattern == 0L) {
        return false; // run out of levels
      }
      weight *= 2;
    } while ((bitPattern & 1L) == 0L);
    index = 0;
    offset = (2 + level) * k;
    num = k;
    return true;
  }

}
