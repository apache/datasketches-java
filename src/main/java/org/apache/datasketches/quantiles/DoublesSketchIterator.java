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
import org.apache.datasketches.quantilescommon.QuantilesDoublesSketchIterator;

/**
 * Iterator over DoublesSketch. The order is not defined.
 */
public final class DoublesSketchIterator implements QuantilesDoublesSketchIterator {
  private DoublesSketchAccessor sketchAccessor;
  private long bitPattern;
  private int level;
  private long weight;
  private int index;

  DoublesSketchIterator(final DoublesSketch sketch, final long bitPattern) {
    Objects.requireNonNull(sketch, "sketch must not be null");
    sketchAccessor = DoublesSketchAccessor.wrap(sketch);
    this.bitPattern = bitPattern;
    this.level = -1;
    this.weight = 1;
    this.index = -1;
  }

  @Override
  public double getQuantile() {
    if (index < 0) { throw new SketchesStateException("index < 0; getQuantile() was called before next()"); }
    return sketchAccessor.get(index);
  }

  @Override
  public long getWeight() {
    return weight;
  }

  @Override
  public boolean next() {
    index++; // advance index within the current level
    if (index < sketchAccessor.numItems()) {
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
    sketchAccessor.setLevel(level);
    return true;
  }

}
