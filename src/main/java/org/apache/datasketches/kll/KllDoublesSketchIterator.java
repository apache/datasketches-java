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

package org.apache.datasketches.kll;

import org.apache.datasketches.QuantilesDoublesSketchIterator;

/**
 * Iterator over KllDoublesSketch. The order is not defined.
 */
public class KllDoublesSketchIterator implements QuantilesDoublesSketchIterator {

  private final double[] values;
  private final int[] levels;
  private final int numLevels;
  private int level;
  private int index;
  private long weight;
  private boolean isInitialized;

  KllDoublesSketchIterator(final double[] values, final int[] levels, final int numLevels) {
    this.values = values;
    this.levels = levels;
    this.numLevels = numLevels;
    this.isInitialized = false;
  }

  @Override
  public double getValue() {
    return values[index];
  }

  @Override
  public long getWeight() {
    return weight;
  }

  @Override
  public boolean next() {
    if (!isInitialized) {
      level = 0;
      index = levels[level];
      weight = 1;
      isInitialized = true;
    } else {
      index++;
    }
    if (index < levels[level + 1]) {
      return true;
    }
    // go to the next non-empty level
    do {
      level++;
      if (level == numLevels) {
        return false; // run out of levels
      }
      weight *= 2;
    } while (levels[level] == levels[level + 1]);
    index = levels[level];
    return true;
  }

}
