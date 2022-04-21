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
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.EMPTYBIT_AND_SINGLEBIT;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.INVALID_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SINGLEBIT_AND_PREINTS;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SINGLEBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.SRC_NOT_KLL;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.UPDATABLEBIT_AND_SER_VER;
import static org.apache.datasketches.kll.KllMemoryValidate.MemoryInputError.memoryValidateThrow;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryDoubleSketchFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySerVer;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySingleItemFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemoryUpdatableFormatFlag;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
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
  boolean updatableMemFormat = false;
  final boolean readOnly;
  final int k;
  final int m;
  final int typeBytes;

  // depending on the layout, the next 8-16 bytes of the preamble, may be filled with assumed values.
  // For example, if the layout is compact & empty, n = 0, if compact and single, n = 1, etc.
  long n;
  // next 4 bytes
  int minK;
  int numLevels;
  // derived
  int sketchBytes;
  int[] levelsArr; //adjusted to include top index

  KllMemoryValidate(final Memory srcMem) {
    readOnly = srcMem.isReadOnly();
    preInts = getMemoryPreInts(srcMem);
    serVer = getMemorySerVer(srcMem);

    familyID = getMemoryFamilyID(srcMem);
    if (familyID != Family.KLL.getID()) { memoryValidateThrow(SRC_NOT_KLL, familyID); }
    famName = idToFamily(familyID).toString();
    flags = getMemoryFlags(srcMem);
    updatableMemFormat = getMemoryUpdatableFormatFlag(srcMem);
    empty = getMemoryEmptyFlag(srcMem);
    singleItem = getMemorySingleItemFlag(srcMem);
    level0Sorted  = getMemoryLevelZeroSortedFlag(srcMem);
    doublesSketch = getMemoryDoubleSketchFlag(srcMem);
    k = getMemoryK(srcMem);
    m = getMemoryM(srcMem);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    if ((serVer == SERIAL_VERSION_UPDATABLE) ^ updatableMemFormat) { memoryValidateThrow(UPDATABLEBIT_AND_SER_VER, 1); }
    typeBytes = doublesSketch ? Double.BYTES : Float.BYTES;

    if (updatableMemFormat) { updatableMemFormatValidate((WritableMemory) srcMem); }
    else { compactMemoryValidate(srcMem); }
  }

  void compactMemoryValidate(final Memory srcMem) { //FOR HEAPIFY
    if (empty && singleItem) { memoryValidateThrow(EMPTYBIT_AND_SINGLEBIT, flags); }
    final int sw = (empty ? 1 : 0) | (singleItem ? 4 : 0);
    switch (sw) {
      case 0: { //FULL_COMPACT
        if (preInts != PREAMBLE_INTS_FULL) { memoryValidateThrow(INVALID_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(EMPTYBIT_AND_SER_VER, serVer); }
        n = getMemoryN(srcMem);
        minK = getMemoryMinK(srcMem);
        numLevels = getMemoryNumLevels(srcMem);

        // Create Levels Arr
        levelsArr = new int[numLevels + 1];
        srcMem.getIntArray(DATA_START_ADR, levelsArr, 0, numLevels); //copies all except the last one
        final int capacityItems = KllHelper.computeTotalItemCapacity(k, m, numLevels);
        levelsArr[numLevels] = capacityItems; //load the last one

        final int retainedItems = (levelsArr[numLevels] - levelsArr[0]);
        sketchBytes = DATA_START_ADR + numLevels * Integer.BYTES + 2 * typeBytes + retainedItems * typeBytes;
        break;
      }
      case 1: { //EMPTY_COMPACT
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(EMPTYBIT_AND_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(EMPTYBIT_AND_SER_VER, serVer); }
        n = 0;           //assumed
        minK = k;        //assumed
        numLevels = 1;   //assumed
        levelsArr = new int[] {k, k};
        sketchBytes = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case 4: { //SINGLE_COMPACT
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(SINGLEBIT_AND_PREINTS, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryValidateThrow(SINGLEBIT_AND_SER_VER, serVer); }
        n = 1;           //assumed
        minK = k;        //assumed
        numLevels = 1;   //assumed
        levelsArr = new int[] {k - 1, k};
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + typeBytes;
        break;
      }
      default: //can not happen
    }
  }

  void updatableMemFormatValidate(final WritableMemory wSrcMem) {
    if (preInts != PREAMBLE_INTS_FULL) { memoryValidateThrow(INVALID_PREINTS, preInts); }
    n = getMemoryN(wSrcMem);
    empty = n == 0;       //empty & singleItem are set for convenience
    singleItem = n == 1;  // there is no error checking on these bits
    minK = getMemoryMinK(wSrcMem);
    numLevels = getMemoryNumLevels(wSrcMem);

    levelsArr = new int[numLevels + 1];
    wSrcMem.getIntArray(DATA_START_ADR, levelsArr, 0, numLevels + 1);

    final int capacity = levelsArr[numLevels];

    sketchBytes =
        DATA_START_ADR + levelsArr.length * Integer.BYTES + 2 * typeBytes + capacity * typeBytes;
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
