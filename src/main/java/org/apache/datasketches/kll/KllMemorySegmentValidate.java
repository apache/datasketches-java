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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentM;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentN;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySegmentSerVer;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_ITEMS_SKETCH;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.apache.datasketches.kll.KllSketch.SketchType;

/**
 * This class performs all the error checking of an incoming MemorySegment object and extracts the key fields in the process.
 * This is used by all KLL sketches that read or import MemorySegment objects.
 * @author lrhodes
 *
 */
final class KllMemorySegmentValidate {
  final MemorySegment srcSeg;
  final ArrayOfItemsSerDe<?> serDe;
  final SketchType sketchType;
  final SketchStructure sketchStructure;

  // first 8 bytes of preamble
  final int preInts;  //used by KllPreambleUtil
  final int serVer;   //used by KllPreambleUtil
  final int familyID; //used by KllPreambleUtil
  final int flags;    //used by KllPreambleUtil
  final int k;        //used multiple places
  final int m;        //used multiple places
  //byte 7 is unused

  //Flag bits:
  final boolean emptyFlag;        //used multiple places
  final boolean level0SortedFlag; //used multiple places

  // depending on the layout, the next 8-16 bytes of the preamble, may be derived by assumption.
  // For example, if the layout is compact & empty, n = 0, if compact and single, n = 1.
  long n;        //8 bytes (if present), used multiple places
  int minK;      //2 bytes (if present), used multiple places
  int numLevels; //1 byte  (if present), used by KllPreambleUtil
  //skip unused byte
  int[] levelsArr; //starts at byte 20, adjusted to include top index here, used multiple places

  // derived.
  int sketchBytes = 0; //used by KllPreambleUtil
  private int typeBytes = 0; //always 0 for generic

  KllMemorySegmentValidate(final MemorySegment srcSeg, final SketchType sketchType) {
    this(srcSeg, sketchType, null);
  }

  KllMemorySegmentValidate(final MemorySegment srcSeg, final SketchType sketchType, final ArrayOfItemsSerDe<?> serDe) {
    final long segCapBytes = srcSeg.byteSize();
    if (segCapBytes < 8) { throw new SketchesArgumentException(MEMORY_TOO_SMALL + segCapBytes); }
    this.srcSeg = srcSeg;
    this.sketchType = sketchType;
    this.serDe = serDe;
    preInts = getMemorySegmentPreInts(srcSeg);
    serVer = getMemorySegmentSerVer(srcSeg);
    sketchStructure = SketchStructure.getSketchStructure(preInts, serVer);
    familyID = getMemorySegmentFamilyID(srcSeg);
    if (familyID != Family.KLL.getID()) { throw new SketchesArgumentException(SRC_NOT_KLL + familyID); }
    flags = getMemorySegmentFlags(srcSeg);
    k = getMemorySegmentK(srcSeg);
    m = getMemorySegmentM(srcSeg);
    KllHelper.checkM(m);
    KllHelper.checkK(k, m);
    //flags
    emptyFlag = getMemorySegmentEmptyFlag(srcSeg);
    level0SortedFlag  = getMemorySegmentLevelZeroSortedFlag(srcSeg);
    if (sketchType == KLL_DOUBLES_SKETCH) { typeBytes = Double.BYTES; }
    else if (sketchType == KLL_FLOATS_SKETCH) { typeBytes = Float.BYTES; }
    else { typeBytes = 0; }
    validate();
  }

  private void validate() {

    switch (sketchStructure) {
      case COMPACT_FULL: {
        if (emptyFlag) { throw new SketchesArgumentException(EMPTY_FLAG_AND_COMPACT_FULL); }
        n = getMemorySegmentN(srcSeg);
        minK = getMemorySegmentMinK(srcSeg);
        numLevels = getMemorySegmentNumLevels(srcSeg);
        // Get Levels Arr and add the last element
        levelsArr = new int[numLevels + 1];
        MemorySegment.copy(srcSeg, JAVA_INT_UNALIGNED, DATA_START_ADR, levelsArr, 0, numLevels); //copies all except the last one
        final int capacityItems = KllHelper.computeTotalItemCapacity(k, m, numLevels);
        levelsArr[numLevels] = capacityItems; //load the last one
        sketchBytes = computeSketchBytes(srcSeg, sketchType, levelsArr, false, serDe);
        break;
      }
      case COMPACT_EMPTY: {
        if (!emptyFlag) { throw new SketchesArgumentException(EMPTY_FLAG_AND_COMPACT_EMPTY); }
        n = 0;           //assumed
        minK = k;        //assumed
        numLevels = 1;   //assumed
        levelsArr = new int[] {k, k};
        sketchBytes = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case COMPACT_SINGLE: {
        if (emptyFlag) { throw new SketchesArgumentException(EMPTY_FLAG_AND_COMPACT_SINGLE); }
        n = 1;           //assumed
        minK = k;        //assumed
        numLevels = 1;   //assumed
        levelsArr = new int[] {k - 1, k};
        if (sketchType == KLL_ITEMS_SKETCH) {
          sketchBytes = DATA_START_ADR_SINGLE_ITEM + serDe.sizeOf(srcSeg, DATA_START_ADR_SINGLE_ITEM, 1);
        } else {
          sketchBytes = DATA_START_ADR_SINGLE_ITEM + typeBytes;
        }
        break;
      }
      case UPDATABLE: {
        n = getMemorySegmentN(srcSeg);
        minK = getMemorySegmentMinK(srcSeg);
        numLevels = getMemorySegmentNumLevels(srcSeg);
        levelsArr = new int[numLevels + 1];
        MemorySegment.copy(srcSeg, JAVA_INT_UNALIGNED, DATA_START_ADR, levelsArr, 0, numLevels + 1);


        sketchBytes = computeSketchBytes(srcSeg, sketchType, levelsArr, true, serDe);
        break;
      }
      default: break; //can not happen
    }
  }

  static int computeSketchBytes( //for COMPACT_FULL or UPDATABLE only
      final MemorySegment srcSeg,
      final SketchType sketchType,
      final int[] levelsArr, //full levels array
      final boolean updatable,
      final ArrayOfItemsSerDe<?> serDe) { //serDe only valid for ITEMS_SKETCH
    final int numLevels = levelsArr.length - 1;
    final int capacityItems = levelsArr[numLevels];
    final int retainedItems = (levelsArr[numLevels] - levelsArr[0]);
    final int levelsLen = updatable ? levelsArr.length : levelsArr.length - 1;
    final int numItems = updatable ? capacityItems : retainedItems;

    int offsetBytes = DATA_START_ADR + (levelsLen * Integer.BYTES); //levels array
    if (sketchType == KLL_ITEMS_SKETCH) {
      if (serDe instanceof ArrayOfBooleansSerDe) {
        offsetBytes += serDe.sizeOf(srcSeg, offsetBytes, numItems) + 2; //2 for min & max
      } else {
        offsetBytes += serDe.sizeOf(srcSeg, offsetBytes, numItems + 2); //2 for min & max
      }
    } else {
      final int typeBytes = sketchType.getBytes();
      offsetBytes += (numItems + 2) * typeBytes; //2 for min & max
    }
    return offsetBytes;
  }

  static final String EMPTY_FLAG_AND_COMPACT_EMPTY = "A compact empty sketch should have empty flag set. ";
  static final String EMPTY_FLAG_AND_COMPACT_FULL = "A compact full sketch should not have empty flag set. ";
  static final String EMPTY_FLAG_AND_COMPACT_SINGLE = "A single item sketch should not have empty flag set. ";
  //static final String N_AND_COMPACT_FULL = "A compact full sketch should have n > 1. ";
  static final String SRC_NOT_KLL = "FamilyID Field must be: " + Family.KLL.getID() + ", NOT: ";
  static final String MEMORY_TOO_SMALL = "A sketch MemorySegment image must be at least 8 bytes. ";

}
