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

package org.apache.datasketches.theta;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class converts current compact sketches into prior SerVer 1 and SerVer 2 format for testing.
 *
 * @author Lee Rhodes
 */
public class BackwardConversions {

  /**
   * Converts a SerVer3 ordered, heap CompactSketch to a SerVer1 ordered, SetSketch in Memory.
   * This is exclusively for testing purposes.
   *
   * <p>V1 dates from roughly Aug 2014 to about May 2015.
   * The library at that time had an early Theta sketch with set operations based on ByteBuffer,
   * the Alpha sketch, and an early HLL sketch. It also had an early adaptor for Pig.
   * It also had code for the even earlier CountUniqueSketch (for backward compatibility),
   * which was the bucket sketch based on Giroire.
   *
   * <p><b>Serialization:</b></p>
   * <pre>
   * Long || Start Byte Adr:
   * Adr:
   *      ||  7 |   6   |     5    |   4   |   3   |    2   |    1   |     0    |
   *  0   ||    | Flags | LgResize | LgArr | lgNom | SkType | SerVer | MD_LONGS |
   *
   *      || 15 |  14   |    13    |  12   |  11   |   10   |    9   |     8    |
   *  1   ||                               | ------------CurCount-------------- |
   *
   *      || 23 |  22   |    21    |  20   |  19   |   18   |   17   |    16    |
   *  2   || --------------------------THETA_LONG------------------------------ |
   *
   *      ||                                                         |    24    |
   *  3   || ----------------------Start of Long Array------------------------  |
   * </pre>
   *
   * <ul>
   * <li>The serialization for V1 was always to a compact form (no hash table spaces).</li>
   * <li><i>MD_LONGS</i> (Metadata Longs, now Preamble Longs) was always 3.</li>
   * <li><i>SerVer</i> is always 1.</li>
   * <li>The <i>SkType</i> had three values: 1,2,3 for Alpha, QuickSelect, and SetSketch,
   * repectively.</li>
   * <li>Bytes <i>lgNom</i> and <i>lgArr</i> were only used by the QS and Alpha sketches.</li>
   * <li>V1 <i>LgResize</i> (2 bits) was only relevant to the Alpha and QS sketches.</li>
   * <li>The flags byte is in byte 6 (moved to 5 in V2).</li>
   * <li>The only flag bits are BE(bit0)=0, and Read-Only(bit1)=1. Read-only was only set for the
   * SetSketch.</li>
   * <li>There is no seedHash.</li>
   * <li>There is no concept of p-sampling so bytes 12-15 of Pre1 are empty.</li>
   * <li>The determination of empty is when both curCount=0 and thetaLong = Long.MAX_VALUE.</li>
   * </ul>
   *
   * @param skV3 a SerVer3, ordered CompactSketch
   * @return a SerVer1 SetSketch as Memory object.
   */
  public static Memory convertSerVer3toSerVer1(final CompactSketch skV3) {
    //Check input sketch
    final boolean validIn = skV3.isCompact() && skV3.isOrdered() && !skV3.hasMemory();
    if (!validIn) {
      throw new SketchesArgumentException("Invalid input sketch.");
    }

    //Build V1 SetSketch in memory
    final int curCount = skV3.getRetainedEntries(true);
    final WritableMemory wmem = WritableMemory.allocate((3 + curCount) << 3);
    //Pre0
    wmem.putByte(0, (byte) 3); //preLongs
    wmem.putByte(1, (byte) 1); //SerVer
    wmem.putByte(2, (byte) 3); //Compact (SetSketch)
    wmem.putByte(6, (byte) 2); //Flags ReadOnly, LittleEndian
    //Pre1
    wmem.putInt(8, curCount);
    //Pre2
    wmem.putLong(16, skV3.getThetaLong());
    //Data
    if (curCount > 0) {
      wmem.putLongArray(24, skV3.getCache(), 0, curCount);
    }
    return wmem;
  }

  /**
   * Converts a SerVer3 ordered, heap CompactSketch to a SerVer2 ordered, SetSketch in Memory.
   * This is exclusively for testing purposes.
   *
   * <p>V2 is short-lived and dates from roughly Mid May 2015 to about June 1st, 2015.
   * (V3 was created about June 15th in preparation for OpenSource in July.)
   * The Theta sketch had evolved but still based on ByteBuffer. There was an UpdateSketch,
   * the Alpha sketch, and the early HLL sketch. It also had an early adaptor for Pig.
   *
   *
   * <p><b>Serialization:</b></p>
   * <pre>
   * Long || Start Byte Adr:
   * Adr:
   *      ||  7 |   6   |     5    |   4   |   3   |    2   |    1   |     0         |
   *  0   || Seed Hash  |  Flags   | lgArr | lgNom | SkType | SerVer | MD_LONGS + RR |
   *
   *      || 15 |  14   |    13    |  12   |  11   |   10   |    9   |     8         |
   *  1   || --------------p-------------- | ---------Retained Entries Count-------- |
   *
   *      || 23 |  22   |    21    |  20   |  19   |   18   |   17   |    16         |
   *  2   || --------------------------THETA_LONG----------------------------------- |
   *
   *      ||                                                         |    24         |
   *  3   || ----------Start of Long Array, could be at 2 or 3 --------------------  |
   *  </pre>
   *
   * <ul>
   * <li>The serialization for V2 was always to a compact form (no hash table spaces).</li>
   * <li><i>MD_LONGS</i> low 6 bits: 1 (Empty), 2 (Exact), 3 (Estimating).</li>
   * <li><i>SerVer</i> is always 2.</li>
   * <li>The <i>SkType</i> had 4 values: 1,2,3,4; see below.</li>
   * <li>Bytes <i>lgNom</i> and <i>lgArr</i> were only used by the QS and Alpha sketches.</li>
   * <li>V2 <i>LgResize</i> top 2 bits if byte 0. Only relevant to the Alpha and QS sketches.</li>
   * <li>The flags byte is in byte 5.</li>
   * <li>The flag bits are specified below.</li>
   * <li>There is a seedHash in bytes 6-7.</li>
   * <li>p-sampling is bytes 12-15 of Pre1.</li>
   * <li>The determination of empty based on the sketch field empty_.</li>
   * </ul>
   * <pre>
   *   // Metadata byte Addresses
   *   private static final int METADATA_LONGS_BYTE        = 0; //low 6 bits
   *   private static final int LG_RESIZE_RATIO_BYTE       = 0; //upper 2 bits
   *   private static final int SER_VER_BYTE               = 1;
   *   private static final int SKETCH_TYPE_BYTE           = 2;
   *   private static final int LG_NOM_LONGS_BYTE          = 3;
   *   private static final int LG_ARR_LONGS_BYTE          = 4;
   *   private static final int FLAGS_BYTE                 = 5;
   *   private static final int SEED_HASH_SHORT            = 6;  //byte 6,7
   *   private static final int RETAINED_ENTRIES_COUNT_INT = 8;  //4 byte aligned
   *   private static final int P_FLOAT                    = 12; //4 byte aligned
   *   private static final int THETA_LONG                 = 16; //8-byte aligned
   *   //Backward compatibility
   *   private static final int FLAGS_BYTE_V1              = 6;
   *   private static final int LG_RESIZE_RATIO_BYTE_V1    = 5;
   *
   *   // Constant Values
   *   static final int SER_VER                        = 2;
   *   static final int ALPHA_SKETCH                   = 1; //SKETCH_TYPE_BYTE
   *   static final int QUICK_SELECT_SKETCH            = 2;
   *   static final int SET_SKETCH                     = 3;
   *   static final int BUFFERED_QUICK_SELECT_SKETCH   = 4;
   *   static final String[] SKETCH_TYPE_STR     =
   *       { "None", "AlphaSketch", "QuickSelectSketch", "SetSketch", "BufferedQuickSelectSketch" };
   *
   *   // flag bit masks
   *   static final int BIG_ENDIAN_FLAG_MASK     = 1;
   *   static final int READ_ONLY_FLAG_MASK      = 2;
   *   static final int EMPTY_FLAG_MASK          = 4;
   *   static final int NO_REBUILD_FLAG_MASK     = 8;
   *   static final int UNORDERED_FLAG_MASK     = 16;
   * </pre>
   *
   * @param skV3 a SerVer3, ordered CompactSketch
   * @param seed used for checking the seed hash (if one exists).
   * @return a SerVer2 SetSketch as Memory object.
   */
  public static Memory convertSerVer3toSerVer2(final CompactSketch skV3, final long seed) {
    final short seedHash = Util.computeSeedHash(seed);
    WritableMemory wmem = null;

    if (skV3 instanceof EmptyCompactSketch) {
      wmem = WritableMemory.allocate(8);
      wmem.putByte(0, (byte) 1); //preLongs
      wmem.putByte(1, (byte) 2); //SerVer
      wmem.putByte(2, (byte) 3); //SetSketch
      final byte flags = (byte) 0xE;  //NoRebuild, Empty, ReadOnly, LE
      wmem.putByte(5, flags);
      wmem.putShort(6, seedHash);
      return wmem;
    }
    if (skV3 instanceof SingleItemSketch) {
      final SingleItemSketch sis = (SingleItemSketch) skV3;
      wmem = WritableMemory.allocate(24);
      wmem.putByte(0, (byte) 2); //preLongs
      wmem.putByte(1, (byte) 2); //SerVer
      wmem.putByte(2, (byte) 3); //SetSketch
      final byte flags = (byte) 0xA;  //NoRebuild, notEmpty, ReadOnly, LE
      wmem.putByte(5, flags);
      wmem.putShort(6, seedHash);
      wmem.putInt(8, 1);
      final long[] arr = sis.getCache();
      wmem.putLong(16,  arr[0]);
      return wmem;
    }
    //General CompactSketch
    final int preLongs = skV3.getCompactPreambleLongs();
    final int entries = skV3.getRetainedEntries(true);
    final boolean unordered = !(skV3.isOrdered());
    final byte flags = (byte) (0xA | (unordered ? 16 : 0)); //Unordered, NoRebuild, notEmpty, ReadOnly, LE
    wmem = WritableMemory.allocate((preLongs + entries) << 3);
    wmem.putByte(0, (byte) preLongs); //preLongs
    wmem.putByte(1, (byte) 2); //SerVer
    wmem.putByte(2, (byte) 3); //SetSketch

    wmem.putByte(5, flags);
    wmem.putShort(6, seedHash);
    wmem.putInt(8, entries);
    if (preLongs == 3) {
      wmem.putLong(16, skV3.getThetaLong());
    }
    final long[] arr = skV3.getCache();
    wmem.putLongArray(preLongs * 8, arr, 0, entries);
    return wmem;
  }
}
