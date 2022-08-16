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

import static org.apache.datasketches.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.FloatsSortedViewIterator;
import org.apache.datasketches.QuantileSearchCriteria;

/**
 * Iterator over KllFloatsSketchSortedView
 * @author Alexander Saydakov
 * @author Lee Rhodes
 */
public class KllFloatsSketchSortedViewIterator implements FloatsSortedViewIterator {

  private final float[] values;
  private final long[] cumWeights;
  private final long totalN;
  private int index;

  KllFloatsSketchSortedViewIterator(final float[] values, final long[] cumWeights) {
    this.values = values;
    this.cumWeights = cumWeights;
    this.totalN = (cumWeights.length > 0) ? cumWeights[cumWeights.length - 1] : 0;
    index = -1;
  }

  @Override
  public long getCumulativeWeight(final QuantileSearchCriteria searchCrit) {
    if (searchCrit == INCLUSIVE) { return cumWeights[index]; }
    return (index == 0) ? 0 : cumWeights[index - 1];
  }

  @Override
  public long getN() {
    return totalN;
  }

  @Override
  public double getNormalizedRank(final QuantileSearchCriteria searchCrit) {
    return (double) getCumulativeWeight(searchCrit) / totalN;
  }

  @Override
  public float getValue() {
    return values[index];
  }

  @Override
  public long getWeight() {
    if (index == 0) { return cumWeights[0]; }
    return cumWeights[index] - cumWeights[index - 1];
  }

  @Override
  public boolean next() {
    index++;
    return index < values.length;
  }

}
