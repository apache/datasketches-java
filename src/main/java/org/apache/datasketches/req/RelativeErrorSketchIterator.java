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

package org.apache.datasketches.req;

import java.util.List;

/**
 * Iterator over KllFloatsSketch. The order is not defined.
 *
 * @author Lee Rhodes
 */
public class RelativeErrorSketchIterator {
  private List<RelativeCompactor> compactors;
  private int cIndex;
  private int bIndex;
  private int retainedItems;
  private Buffer currentBuf;

  RelativeErrorSketchIterator(final RelativeErrorQuantiles sketch) {
    compactors = sketch.compactors;
    retainedItems = sketch.size;
    currentBuf = compactors.get(0).getBuffer();
    cIndex = 0;
    bIndex = -1;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if ((retainedItems == 0)
        || ((cIndex == (compactors.size() - 1)) && (bIndex == currentBuf.getItemCount()))) {
      return false;
    }
    if (bIndex == currentBuf.getItemCount()) {
      cIndex++;
      currentBuf = compactors.get(cIndex).getBuffer();
      bIndex = 0;
    } else {
      bIndex++;
    }
    return true;
  }

  /**
   * Gets a value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return value from the current entry
   */
  public float getValue() {
    return currentBuf.getItem(bIndex);
  }

  /**
   * Gets a weight for the value from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return weight for the value from the current entry
   */
  public long getWeight() {
    return 1 << cIndex;
  }
}
