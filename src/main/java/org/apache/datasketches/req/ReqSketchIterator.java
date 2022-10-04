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

import org.apache.datasketches.QuantilesFloatsSketchIterator;

/**
 * Iterator over all retained values of the ReqSketch. The order is not defined.
 *
 * @author Lee Rhodes
 */
public class ReqSketchIterator implements QuantilesFloatsSketchIterator {
  private List<ReqCompactor> compactors;
  private int cIndex;
  private int bIndex;
  private int retainedValues;
  private FloatBuffer currentBuf;

  ReqSketchIterator(final ReqSketch sketch) {
    compactors = sketch.getCompactors();
    retainedValues = sketch.getNumRetained();
    currentBuf = compactors.get(0).getBuffer();
    cIndex = 0;
    bIndex = -1;
  }

  @Override
  public float getQuantile() {
    return currentBuf.getValue(bIndex);
  }

  @Override
  public long getWeight() {
    return 1 << cIndex;
  }

  @Override
  public boolean next() {
    if ((retainedValues == 0)
        || ((cIndex == (compactors.size() - 1)) && (bIndex == (currentBuf.getCount() - 1)))) {
      return false;
    }
    if (bIndex == (currentBuf.getCount() - 1)) {
      cIndex++;
      currentBuf = compactors.get(cIndex).getBuffer();
      bIndex = 0;
    } else {
      bIndex++;
    }
    return true;
  }
}
