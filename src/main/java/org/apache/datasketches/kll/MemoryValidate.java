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
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.DOUBLES_SKETCH_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SINGLE_ITEM_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.UPDATABLE_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractM;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.extractPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.extractSerVer;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

final class MemoryValidate {
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

  Layout layout;
  // depending on the layout, the next 8-16 bytes of the preamble, may be filled with assumed values.
  // For example, if the layout is compact & empty, n = 0, if compact and single, n = 1, etc.
  long n;
  // next 4 bytes
  int dyMinK;
  int numLevels;
  // derived
  int memItemsCap; //capacity of Items array for exporting and for Updatable form
  int memItemsRetained; //actual items retained in Compact form
  int sketchBytes;
  Memory levelsMem; //if sk = empty or single, this is derived
  Memory minMaxMem; //if sk = empty or single, this is derived
  Memory itemsMem;  //if sk = empty or single, this is derived
  WritableMemory levelsWmem;
  WritableMemory minMaxWmem;
  WritableMemory itemsWmem;

  MemoryValidate(final Memory srcMem) {
    preInts = extractPreInts(srcMem);
    serVer = extractSerVer(srcMem);

    familyID = extractFamilyID(srcMem);
    if (familyID != Family.KLL.getID()) { memoryCheckThrow(0, familyID); }
    famName = idToFamily(familyID).toString();
    if (famName != "KLL") { memoryCheckThrow(23, 0); }

    flags = extractFlags(srcMem);
    empty = (flags & EMPTY_BIT_MASK) > 0;
    level0Sorted  = (flags & LEVEL_ZERO_SORTED_BIT_MASK) > 0;
    singleItem    = (flags & SINGLE_ITEM_BIT_MASK) > 0;
    doublesSketch = (flags & DOUBLES_SKETCH_BIT_MASK) > 0;
    updatable    = (flags & UPDATABLE_BIT_MASK) > 0;
    k = extractK(srcMem);
    KllHelper.checkK(k);
    m = extractM(srcMem);
    if (m != 8) { memoryCheckThrow(7, m); }

    if (updatable) { updatableMemoryValidate((WritableMemory) srcMem); }
    else { compactMemoryValidate(srcMem); }

  }

  void compactMemoryValidate(final Memory srcMem) {
    final int checkFlags = (empty ? 1 : 0) | (singleItem ? 4 : 0) | (doublesSketch ? 8 : 0);
    if ((checkFlags & 5) == 5) { memoryCheckThrow(20, flags); }

    switch (checkFlags) {
      case 0: { //Float Compact FULL
        if (preInts != PREAMBLE_INTS_FLOAT) { memoryCheckThrow(6, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryCheckThrow(2, serVer); }
        layout = Layout.FLOAT_FULL_COMPACT;
        n = extractN(srcMem);
        dyMinK = extractDyMinK(srcMem);
        numLevels = extractNumLevels(srcMem);
        int offset = DATA_START_ADR_FLOAT;
        // LEVELS MEM
        final int[] myLevelsArr = new int[numLevels + 1];
        srcMem.getIntArray(offset, myLevelsArr, 0, numLevels); //copies all except the last one
        myLevelsArr[numLevels] = KllHelper.computeTotalItemCapacity(k, m, numLevels); //load the last one
        levelsMem = Memory.wrap(myLevelsArr); //separate from srcMem,
        offset += levelsMem.getCapacity() - Integer.BYTES; // but one larger than srcMem
        // MIN/MAX MEM
        minMaxMem = srcMem.region(offset, 2 * Float.BYTES);
        offset += minMaxMem.getCapacity();
        // ITEMS MEM
        memItemsCap = myLevelsArr[numLevels];
        memItemsRetained = memItemsCap - myLevelsArr[0];
        final float[] myItemsArr = new float[memItemsCap];
        srcMem.getFloatArray(offset, myItemsArr, myLevelsArr[0], memItemsRetained);
        itemsMem = Memory.wrap(myItemsArr);
        sketchBytes = offset + memItemsRetained * Float.BYTES;
        break;
      }
      case 1: { //Float Compact EMPTY
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryCheckThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryCheckThrow(2, serVer); }
        layout = Layout.FLOAT_EMPTY_COMPACT;
        n = 0;           //assumed
        dyMinK = k;      //assumed
        numLevels = 1;   //assumed

        // LEVELS MEM
        levelsMem = Memory.wrap(new int[] {k, k});
        // MIN/MAX MEM
        minMaxMem = Memory.wrap(new float[] {Float.NaN, Float.NaN});
        // ITEMS MEM
        memItemsCap = k;
        memItemsRetained = 0;
        itemsMem = Memory.wrap(new float[k]);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM; //also used for empty
        break;
      }
      case 4: { //Float Compact SINGLE
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryCheckThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryCheckThrow(4, serVer); }
        layout = Layout.FLOAT_SINGLE_COMPACT;
        n = 1;
        dyMinK = k;
        numLevels = 1;

        // LEVELS MEM
        levelsMem = Memory.wrap(new int[] {k - 1, k});
        final float minMax = srcMem.getFloat(DATA_START_ADR_SINGLE_ITEM);
        // MIN/MAX MEM
        minMaxMem = Memory.wrap(new float[] {minMax, minMax});
        // ITEMS MEM
        memItemsCap = k;
        memItemsRetained = 1;
        final float[] myFloatItems = new float[k];
        myFloatItems[k - 1] = minMax;
        itemsMem = Memory.wrap(myFloatItems);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + Float.BYTES;
        break;
      }
      case 8: { //Double Compact FULL
        if (preInts != PREAMBLE_INTS_DOUBLE) { memoryCheckThrow(5, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryCheckThrow(2, serVer); }
        layout = Layout.DOUBLE_FULL_COMPACT;
        n = extractN(srcMem);
        dyMinK = extractDyMinK(srcMem);
        numLevels = extractNumLevels(srcMem);
        int offset = DATA_START_ADR_DOUBLE;
        // LEVELS MEM
        final int[] myLevelsArr = new int[numLevels + 1];
        srcMem.getIntArray(offset, myLevelsArr, 0, numLevels); //all except the last one
        myLevelsArr[numLevels] = KllHelper.computeTotalItemCapacity(k, m, numLevels); //load the last one
        levelsMem = Memory.wrap(myLevelsArr); //separate from srcMem
        offset += levelsMem.getCapacity() - Integer.BYTES;
        // MIN/MAX MEM
        minMaxMem = srcMem.region(offset, 2 * Double.BYTES);
        offset += minMaxMem.getCapacity();
        // ITEMS MEM
        memItemsCap = myLevelsArr[numLevels];
        memItemsRetained = memItemsCap - myLevelsArr[0];
        final double[] myItemsArr = new double[memItemsCap];
        srcMem.getDoubleArray(offset, myItemsArr, myLevelsArr[0], memItemsRetained);
        itemsMem = Memory.wrap(myItemsArr);
        sketchBytes = offset + memItemsRetained * Double.BYTES;
        break;
      }
      case 9: { //Double Compact EMPTY
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryCheckThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryCheckThrow(2, serVer); }
        layout = Layout.DOUBLE_EMPTY_COMPACT;
        n = 0;
        dyMinK = k;
        numLevels = 1;

        // LEVELS MEM
        levelsMem = Memory.wrap(new int[] {k, k});
        // MIN/MAX MEM
        minMaxMem = Memory.wrap(new double[] {Double.NaN, Double.NaN});
        // ITEMS MEM
        memItemsCap = k;
        memItemsRetained = 0;
        itemsMem = Memory.wrap(new double[k]);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM; //also used for empty
        break;
      }
      case 12: { //Double Compact SINGLE
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryCheckThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryCheckThrow(4, serVer); }
        layout = Layout.DOUBLE_SINGLE_COMPACT;
        n = 1;
        dyMinK = k;
        numLevels = 1;

        // LEVELS MEM
        levelsMem = Memory.wrap(new int[] {k - 1, k});
        final double minMax = srcMem.getDouble(DATA_START_ADR_SINGLE_ITEM);
        // MIN/MAX MEM
        minMaxMem = Memory.wrap(new double[] {minMax, minMax});
        // ITEMS MEM
        memItemsCap = k;
        memItemsRetained = 1;
        final double[] myDoubleItems = new double[k];
        myDoubleItems[k - 1] = minMax;
        itemsMem = Memory.wrap(myDoubleItems);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + Double.BYTES;
        break;
      }
      default: break; //can't happen
    }
  }

  void updatableMemoryValidate(final WritableMemory wSrcMem) {
    final int checkFlags = (doublesSketch ? 8 : 0);
    if ((checkFlags & 5) == 5) { memoryCheckThrow(20, flags); }
    //System.out.println(KllPreambleUtil.memoryToString(wSrcMem));

    switch (checkFlags) {
      case 0: { //Float Updatable FULL
        if (preInts != PREAMBLE_INTS_FLOAT) { memoryCheckThrow(6, preInts); }
        if (serVer != SERIAL_VERSION_UPDATABLE) { memoryCheckThrow(10, serVer); }
        layout = Layout.FLOAT_UPDATABLE;
        n = extractN(wSrcMem);
        empty = n == 0;
        singleItem = n == 1;
        dyMinK = extractDyMinK(wSrcMem);
        numLevels = extractNumLevels(wSrcMem);
        int offset = DATA_START_ADR_FLOAT;
        //LEVELS
        levelsWmem = wSrcMem.writableRegion(offset, (numLevels + 1) * Integer.BYTES);
        offset += (int)levelsWmem.getCapacity();
        //MIN/MAX
        minMaxWmem = wSrcMem.writableRegion(offset, 2 * Float.BYTES);
        offset += (int)minMaxWmem.getCapacity();
        //ITEMS
        memItemsCap = levelsWmem.getInt(numLevels * Integer.BYTES);
        itemsWmem = wSrcMem.writableRegion(offset, memItemsCap * Float.BYTES);
        offset += itemsWmem.getCapacity();
        sketchBytes = offset;
        break;
      }

      case 8: { //Double Updatable FULL
        if (preInts != PREAMBLE_INTS_DOUBLE) { memoryCheckThrow(5, preInts); }
        if (serVer != SERIAL_VERSION_UPDATABLE) { memoryCheckThrow(10, serVer); }
        layout = Layout.DOUBLE_UPDATABLE;
        n = extractN(wSrcMem);
        empty = n == 0;
        singleItem = n == 1;
        dyMinK = extractDyMinK(wSrcMem);
        numLevels = extractNumLevels(wSrcMem);

        int offset = DATA_START_ADR_DOUBLE;
        //LEVELS
        levelsWmem = wSrcMem.writableRegion(offset, (numLevels + 1) * Integer.BYTES);
        offset += (int)levelsWmem.getCapacity();
        //MIN/MAX
        minMaxWmem = wSrcMem.writableRegion(offset, 2 * Double.BYTES);
        offset += (int)minMaxWmem.getCapacity();
        //ITEMS
        memItemsCap = levelsWmem.getInt(numLevels * Integer.BYTES);
        itemsWmem = wSrcMem.writableRegion(offset, memItemsCap * Double.BYTES);
        offset += itemsWmem.getCapacity();
        sketchBytes = offset;
        break;
      }
      default: break; //can't happen
    }
  }

//  @SuppressWarnings("unused")
//  private static void printMemInts(final Memory mem) {
//    final int capInts = (int)(mem.getCapacity() / 4);
//    for (int i = 0; i < capInts; i++) {
//      System.out.println(mem.getInt(i * 4));
//    }
//  }
//
//  @SuppressWarnings("unused")
//  private static void printMemFloats(final Memory mem) {
//    final int capFlts = (int)(mem.getCapacity() / 4);
//    for (int i = 0; i < capFlts; i++) {
//      System.out.println(mem.getFloat(i * 4));
//    }
//  }


  private static void memoryCheckThrow(final int errNo, final int value) {
    String msg = "";
    switch (errNo) {
      case 0: msg = "FamilyID Field must be: " + Family.KLL.getID() + ", NOT: " + value; break;
      case 1: msg = "Empty Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
      case 2: msg = "Empty Bit: 1 -> SerVer: " + SERIAL_VERSION_EMPTY_FULL + ", NOT: " + value; break;
      case 3: msg = "Single Item Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
      case 4: msg = "Single Item Bit: 1 -> SerVer: " + SERIAL_VERSION_SINGLE + ", NOT: " + value; break;
      case 5: msg = "Double Sketch Bit: 1 -> PreInts: " + PREAMBLE_INTS_DOUBLE + ", NOT: " + value; break;
      case 6: msg = "Double Sketch Bit: 0 -> PreInts: " + PREAMBLE_INTS_FLOAT + ", NOT: " + value; break;
      case 7: msg = "The M field must be set to " + DEFAULT_M + ", NOT: " + value; break;
      case 8: msg = "The dynamic MinK must be equal to K, NOT: " + value; break;
      case 9: msg = "numLevels must be one, NOT: " + value; break;
      case 10: msg = "Updatable Bit: 1 -> SerVer: " + SERIAL_VERSION_UPDATABLE + ", NOT: " + value; break;
      case 20: msg = "Empty flag bit and SingleItem flag bit cannot both be set. Flags: " + value; break;
      case 21: msg = "N != 0 and empty bit is set. N: " + value; break;
      case 22: msg = "N != 1 and single item bit is set. N: " + value; break;
      case 23: msg = "Family name is not KLL"; break;
    }
    throw new SketchesArgumentException(msg);
  }

}

