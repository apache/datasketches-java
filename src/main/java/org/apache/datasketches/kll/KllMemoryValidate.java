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

import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SRC_NOT_KLL;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.UPDATABLEBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_SINGLEBIT;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SINGLEBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SINGLEBIT_AND_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.INVALID_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.memoryValidateThrow;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.extractDoubleSketchFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractM;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.extractPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.extractSerVer;
import static org.apache.datasketches.kll.KllPreambleUtil.extractSingleItemFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractUpdatableFlag;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class performs all the error checking of an incoming Memory object and extracts the key fields in the process.
 * This is used by all sketches that read or import Memory objects.
 *
 * @author lrhodes
 *
 */
final class KllMemoryValidate {
  // first 8 bytes
  final int preInts; // = extractPreInts(srcMem);
  final int serVer;
  final int familyID;
  final String famName;
  final int flags;
  boolean empty;
  boolean singleItem;
  final boolean level0Sorted;
  final boolean doublesSketch;
  final boolean updatable;
  final int k;
  final int m;
  final int memCapacity;

  Layout layout;
  // depending on the layout, the next 8-16 bytes of the preamble, may be filled with assumed values.
  // For example, if the layout is compact & empty, n = 0, if compact and single, n = 1, etc.
  long n;
  // next 4 bytes
  int minK;
  int numLevels;
  // derived
  int capacityItems; //capacity of Items array for exporting and for Updatable form
  int itemsRetained; //actual items retained in Compact form
  int itemsArrStart;
  int sketchBytes;
  Memory levelsArrCompact; //if sk = empty or single, this is derived
  Memory minMaxArrCompact; //if sk = empty or single, this is derived
  Memory itemsArrCompact;  //if sk = empty or single, this is derived
  WritableMemory levelsArrUpdatable;
  WritableMemory minMaxArrUpdatable;
  WritableMemory itemsArrUpdatable;

  KllMemoryValidate(final Memory srcMem) {
    memCapacity = (int) srcMem.getCapacity();
    preInts = extractPreInts(srcMem);
    serVer = extractSerVer(srcMem);

    familyID = extractFamilyID(srcMem);
    if (familyID != Family.KLL.getID()) { memoryValidateThrow(SRC_NOT_KLL, familyID); }
    famName = idToFamily(familyID).toString();
    flags = extractFlags(srcMem);
    empty = extractEmptyFlag(srcMem);
    level0Sorted  = extractLevelZeroSortedFlag(srcMem);
    singleItem    = extractSingleItemFlag(srcMem);
    doublesSketch = extractDoubleSketchFlag(srcMem);
    updatable    = extractUpdatableFlag(srcMem);
    k = extractK(srcMem);
    m = extractM(srcMem);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    if ((serVer == SERIAL_VERSION_UPDATABLE) ^ updatable) { memoryValidateThrow(UPDATABLEBIT_AND_SER_VER, 1); }

    if (updatable) { updatableMemoryValidate((WritableMemory) srcMem); }
    else { compactMemoryValidate(srcMem); }
  }

  void compactMemoryValidate(final Memory srcMem) {
    if (empty && singleItem) { memoryValidateThrow(EMPTYBIT_AND_SINGLEBIT, flags); }
    final int typeBytes = doublesSketch ? Double.BYTES : Float.BYTES;
    final int sw = (empty ? 1 : 0) | (singleItem ? 4 : 0);
    switch (sw) {
      case 0: { //FULL_COMPACT
        if (preInts != PREAMBLE_INTS_FULL) { memoryValidateThrow(INVALID_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(EMPTYBIT_AND_SER_VER, serVer); }
        layout = doublesSketch ? Layout.DOUBLE_FULL_COMPACT : Layout.FLOAT_FULL_COMPACT;
        n = extractN(srcMem);
        minK = extractMinK(srcMem);
        numLevels = extractNumLevels(srcMem);
        int offset = DATA_START_ADR;

        // LEVELS MEM
        final int[] myLevelsArr = new int[numLevels + 1];
        srcMem.getIntArray(offset, myLevelsArr, 0, numLevels); //copies all except the last one
        myLevelsArr[numLevels] = KllHelper.computeTotalItemCapacity(k, m, numLevels); //load the last one
        levelsArrCompact = Memory.wrap(myLevelsArr); //separate from srcMem,
        offset += (int)levelsArrCompact.getCapacity() - Integer.BYTES; // but one larger than srcMem

        minMaxArrCompact = srcMem.region(offset, 2L * typeBytes); // MIN/MAX MEM
        offset += (int)minMaxArrCompact.getCapacity();

        // ITEMS MEM
        itemsArrStart = offset;
        capacityItems = myLevelsArr[numLevels];
        itemsRetained = capacityItems - myLevelsArr[0];
        if (doublesSketch) {
          final double[] myItemsArr = new double[capacityItems];
          srcMem.getDoubleArray(offset, myItemsArr, myLevelsArr[0], itemsRetained);
          itemsArrCompact = Memory.wrap(myItemsArr);
        } else {
          final float[] myItemsArr = new float[capacityItems];
          srcMem.getFloatArray(offset, myItemsArr, myLevelsArr[0], itemsRetained);
          itemsArrCompact = Memory.wrap(myItemsArr);
        }
        sketchBytes = offset + itemsRetained * typeBytes;
        break;
      }
      case 1: { //EMPTY_COMPACT
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(EMPTYBIT_AND_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(EMPTYBIT_AND_SER_VER, serVer); }
        layout = doublesSketch ? Layout.DOUBLE_EMPTY_COMPACT : Layout.FLOAT_EMPTY_COMPACT;
        n = 0;           //assumed
        minK = k;      //assumed
        numLevels = 1;   //assumed
        capacityItems = k;
        itemsRetained = 0;

        levelsArrCompact = Memory.wrap(new int[] {k, k}); // LEVELS MEM
        if (doublesSketch) {
          minMaxArrCompact = Memory.wrap(new double[] {Double.NaN, Double.NaN}); // MIN/MAX MEM
          itemsArrCompact = Memory.wrap(new double[k]);                          // ITEMS MEM
        } else { //Floats Sketch
          minMaxArrCompact = Memory.wrap(new float[] {Float.NaN, Float.NaN});    // MIN/MAX MEM
          itemsArrCompact = Memory.wrap(new float[k]);                           // ITEMS MEM
        }
        sketchBytes = DATA_START_ADR_SINGLE_ITEM;   //used for empty and single item
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case 4: { //SINGLE_COMPACT
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(SINGLEBIT_AND_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryValidateThrow(SINGLEBIT_AND_SER_VER, serVer); }
        layout = doublesSketch ? Layout.DOUBLE_SINGLE_COMPACT : Layout.FLOAT_SINGLE_COMPACT;
        n = 1;
        minK = k;
        numLevels = 1;
        capacityItems = k;
        itemsRetained = 1;

        levelsArrCompact = Memory.wrap(new int[] {k - 1, k}); // LEVELS MEM
        if (doublesSketch) {
          final double minMax = srcMem.getDouble(DATA_START_ADR_SINGLE_ITEM);
          minMaxArrCompact = Memory.wrap(new double[] {minMax, minMax}); // MIN/MAX MEM
          final double[] myDoubleItems = new double[k];                    // ITEMS MEM
          myDoubleItems[k - 1] = minMax;
          itemsArrCompact = Memory.wrap(myDoubleItems);
        } else {
          final float minMax = srcMem.getFloat(DATA_START_ADR_SINGLE_ITEM);
          minMaxArrCompact = Memory.wrap(new float[] {minMax, minMax}); // MIN/MAX MEM
          final float[] myFloatItems = new float[k];                    // ITEMS MEM
          myFloatItems[k - 1] = minMax;
          itemsArrCompact = Memory.wrap(myFloatItems);
        }
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + typeBytes;
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      default: //can not happen
    }
  }

  void updatableMemoryValidate(final WritableMemory wSrcMem) {
    final int typeBytes = doublesSketch ? Double.BYTES : Float.BYTES;
    if (preInts != PREAMBLE_INTS_FULL) { memoryValidateThrow(INVALID_PREINTS, preInts); }
    layout = doublesSketch ? Layout.DOUBLE_UPDATABLE : Layout.FLOAT_UPDATABLE;

    n = extractN(wSrcMem);
    empty = n == 0;       //empty & singleItem are set for convenience
    singleItem = n == 1;  // there is no error checking on these bits
    minK = extractMinK(wSrcMem);
    numLevels = extractNumLevels(wSrcMem);

    int offset = DATA_START_ADR;

    levelsArrUpdatable = wSrcMem.writableRegion(offset, (numLevels + 1L) * Integer.BYTES); //LEVELS
    offset += (int)levelsArrUpdatable.getCapacity();

    minMaxArrUpdatable = wSrcMem.writableRegion(offset, 2L * typeBytes);        //MIN/MAX
    offset += (int)minMaxArrUpdatable.getCapacity();

    capacityItems = levelsArrUpdatable.getInt((long)numLevels * Integer.BYTES); //ITEMS
    final int itemsArrBytes = capacityItems * typeBytes;
    itemsArrStart = offset;
    itemsArrUpdatable = wSrcMem.writableRegion(offset, itemsArrBytes);
    sketchBytes = offset + itemsArrBytes;
  }

  enum MemoryInputError {
    SRC_NOT_KLL("FamilyID Field must be: " + Family.KLL.getID() + ", NOT: "),
    EMPTYBIT_AND_PREINTS("Empty Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: "),
    EMPTYBIT_AND_SER_VER("Empty Bit: 1 -> SerVer: " + SERIAL_VERSION_EMPTY_FULL + ", NOT: "),
    SINGLEBIT_AND_SER_VER("Single Item Bit: 1 -> SerVer: " + SERIAL_VERSION_SINGLE + ", NOT: "),
    SINGLEBIT_AND_PREINTS("Single Item Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: "),
    INVALID_PREINTS("PreInts Must Be: " + PREAMBLE_INTS_FULL + ", NOT: "),
    UPDATABLEBIT_AND_SER_VER("((SerVer == 3) ^ (Updatable Bit)) must = 0, NOT: "),
    EMPTYBIT_AND_SINGLEBIT("Empty flag bit and SingleItem flag bit cannot both be set. Flags: ");

    private String msg;

    private MemoryInputError(final String msg) {
      this.msg = msg;
    }

    private String getMessage() {
      return msg;
    }

    final static void memoryValidateThrow(final MemoryInputError errType, final int value) {
      throw new SketchesArgumentException(errType.getMessage() + value);
    }

  }

}
