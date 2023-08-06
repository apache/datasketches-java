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

import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;

/**
 * Iterator over KllFloatsSketch. The order is not defined.
 */
public final class KllFloatsSketchIterator implements QuantilesFloatsSketchIterator {
  private final float[] quantiles;
  private final int[] levelsArr;
  private final int numLevels;
  private int level;
  private int index;
  private long weight;
  private boolean isInitialized_;

  KllFloatsSketchIterator(final float[] quantiles, final int[] levelsArr, final int numLevels) {
    this.quantiles = quantiles;
    this.levelsArr = levelsArr;
    this.numLevels = numLevels;
    this.isInitialized_ = false;
  }

  @Override
  public float getQuantile() {
    return quantiles[index];
  }

  @Override
  public long getWeight() {
    return weight;
  }

  @Override
  public boolean next() {
    if (!isInitialized_) {
      level = 0;
      index = levelsArr[level];
      weight = 1;
      isInitialized_ = true;
    } else {
      index++;
    }
    if (index < levelsArr[level + 1]) {
      return true;
    }
    // go to the next non-empty level
    do {
      level++;
      if (level == numLevels) {
        return false; // run out of levels
      }
      weight *= 2;
    } while (levelsArr[level] == levelsArr[level + 1]);
    index = levelsArr[level];
    return true;
  }

}
