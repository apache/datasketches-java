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

import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySingleItemFlag;
import static org.apache.datasketches.kll.KllSketch.Error.NOT_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

class KllDirectCompactFloatsSketch extends KllDirectFloatsSketch {

  KllDirectCompactFloatsSketch(final Memory srcMem, final KllMemoryValidate memVal) {
    super((WritableMemory) srcMem, null, memVal);
  }

  @Override
  public long getN() {
    if (getMemoryEmptyFlag(wmem)) { return 0; }
    if (getMemorySingleItemFlag(wmem)) { return 1; }
    return getMemoryN(wmem);
  }

  @Override
  public byte[] toByteArray() {
    final int bytes = (int) wmem.getCapacity();
    final byte[] byteArr = new byte[bytes];
    wmem.getByteArray(0, byteArr, 0, bytes);
    return byteArr;
  }

  @Override //returns expanded array including empty space at bottom
  float[] getFloatItemsArray() {
    final int k = getK();
    if (isEmpty()) { return new float[k]; }
    if (isSingleItem()) {
      final float[] itemsArr = new float[k];
      itemsArr[k - 1] = wmem.getFloat(DATA_START_ADR_SINGLE_ITEM);
      return itemsArr;
    }
    final int capacityItems =  levelsArr[getNumLevels()];
    final float[] itemsArr = new float[capacityItems];
    final int levelsBytes = (levelsArr.length - 1) * Integer.BYTES; //compact format!
    final int offset = DATA_START_ADR + levelsBytes + 2 * Float.BYTES;
    final int shift = levelsArr[0];
    wmem.getFloatArray(offset, itemsArr, shift, capacityItems - shift);
    return itemsArr;
  }

  @Override
  float getFloatSingleItem() {
    if (!isSingleItem()) { kllSketchThrow(NOT_SINGLE_ITEM); }
    return wmem.getFloat(DATA_START_ADR_SINGLE_ITEM);
  }

  @Override
  float getMaxFloatValue() {
    if (isEmpty()) { return Float.NaN; }
    if (isSingleItem()) { return getFloatSingleItem(); }
    final int offset =
        DATA_START_ADR + (getLevelsArray().length - 1) * Integer.BYTES + Float.BYTES;
    return wmem.getFloat(offset);
  }

  @Override
  float getMinFloatValue() {
    if (isEmpty()) { return Float.NaN; }
    if (isSingleItem()) { return getFloatSingleItem(); }
    final int offset =
        DATA_START_ADR + (getLevelsArray().length - 1) * Integer.BYTES;
    return wmem.getFloat(offset);
  }

}
