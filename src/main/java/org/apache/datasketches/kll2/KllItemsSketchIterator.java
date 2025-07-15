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

package org.apache.datasketches.kll2;

import org.apache.datasketches.quantilescommon.QuantilesGenericSketchIterator;

/**
 * Iterator over KllItemsSketch. The order is not defined.
 * @param <T> the item class type
 */
public final class KllItemsSketchIterator<T> extends KllSketchIterator implements QuantilesGenericSketchIterator<T> {
  private final Object[] quantiles;

  KllItemsSketchIterator(final Object[] quantiles, final int[] levelsArr, final int numLevels) {
    super(levelsArr, numLevels);
    this.quantiles = quantiles;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getQuantile() {
    return (T)quantiles[index];
  }

}
