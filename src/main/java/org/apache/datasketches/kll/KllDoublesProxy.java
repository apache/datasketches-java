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

import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

abstract class KllDoublesProxy extends KllSketch {

  KllDoublesProxy(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.DOUBLES_SKETCH, wmem, memReqSvr);
  }

  /**
   * @return full size of internal items array including garbage.
   */
  abstract double[] getDoubleItemsArray();

  abstract double getDoubleSingleItem();

  abstract double getMaxDoubleItem();

  abstract double getMinDoubleItem();

  abstract void setDoubleItemsArray(double[] doubleItems);

  abstract void setDoubleItemsArrayAt(int index, double item);

  abstract void setMaxDoubleItem(double item);

  abstract void setMinDoubleItem(double item);

  /**
   * Merges another sketch into this one.
   * Attempting to merge a KllDoublesSketch with a KllFloatsSketch will
   * throw an exception.
   * @param other sketch to merge into this one
   */
  public final void merge(final KllDoublesSketch other) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllDoublesHelper.mergeDoubleImpl((KllDoublesSketch)this, other);
  }


  /**
   * {@inheritDoc}
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public final void reset() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinDoubleItem(Double.NaN);
    setMaxDoubleItem(Double.NaN);
    setDoubleItemsArray(new double[k]);
  }

}
