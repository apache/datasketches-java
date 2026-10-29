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

package org.apache.datasketches.tuple;

//@formatter:off

/**
 * This class documents the serialized forms of the generic tuple sketches,
 * {@link CompactTupleSketch} and the updatable {@link QuickSelectSketch}, in one place.
 * The ArrayOfDoubles sketches in the <i>arrayofdoubles</i> sub-package have their own layouts,
 * which are documented in those classes.
 *
 * <p>MAP: Low significance bytes of this <i>long</i> data structure are on the right.
 * All multi-byte values (<i>short</i>, <i>int</i>, <i>long</i>, <i>float</i> and <i>double</i>)
 * are stored little-endian. The <i>byte</i> values are treated as unsigned.</p>
 *
 * <p>Unlike the theta sketches, a tuple sketch image is not a fixed-size table of longs.
 * Each retained entry is a hash (8 bytes) followed by the bytes of its Summary, and a Summary
 * may have any length. The Summary bytes are written by {@link Summary#toByteArray()} and read by a
 * {@link SummaryDeserializer}, both supplied by the user. The sketch image does not record the
 * Summary type. A reader must therefore know which Summary was used to write the image.</p>
 *
 * <h2>CompactTupleSketch, serial version 3 (current)</h2>
 *
 * <p>This form is binary compatible with the C++ and Go compact tuple sketches, as long as
 * every language uses the same Summary encoding.</p>
 *
 * <p>Flags: ReadOnly, Compact, Empty if empty. The Ordered flag is defined but Java does not set it.
 * The seed hash is zero when the sketch is empty, as in theta.</p>
 *
 * <p>An empty sketch, or a single-item sketch that is not in estimation mode, uses 1 preamble long.
 * The single entry, if any, starts at byte 8.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  | unused | SkType | FamID  | SerVer |   PreLongs = 1     |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-------------------------Hash of the single entry (if not empty)-------------------|
 *
 *      ||        |        |        |        |        |        |        |    16              |
 *  2   ||-------------------------Summary of the single entry-------------------------------|
 * </pre>
 *
 * <p>An exact (not estimating) sketch with more than one entry uses 2 preamble longs.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  | unused | SkType | FamID  | SerVer |   PreLongs = 2     |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-------------unused----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16              |
 *  2   ||-------------Entries: hash (8 bytes), then Summary bytes, repeated Count times-----|
 * </pre>
 *
 * <p>An estimating sketch uses 3 preamble longs.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  | unused | SkType | FamID  | SerVer |   PreLongs = 3     |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-------------unused----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16              |
 *  2   ||------------------------------THETA_LONG-------------------------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||-------------Entries: hash (8 bytes), then Summary bytes, repeated Count times-----|
 * </pre>
 *
 * <h2>CompactTupleSketch, serial versions 1 and 2 (legacy, read only)</h2>
 *
 * <p>Java no longer writes these forms but can still read them. The fields after byte 4 are
 * packed with no alignment, and each one is present only if its flag is set.</p>
 *
 * <pre>
 * Byte Adr | Size | Field
 *    0     |  1   | PreLongs
 *    1     |  1   | SerVer = 1 or 2
 *    2     |  1   | FamID
 *    3     |  1   | SkType = CompactTupleSketch
 *    4     |  1   | Flags (legacy bits, see below)
 *    5     |  8   | THETA_LONG, if IS_THETA_INCLUDED
 *    next  |  1   | Summary class-name length N, if HAS_ENTRIES and SerVer = 1
 *    next  |  4   | Retained Entries Count, if HAS_ENTRIES
 *    next  |  N   | Summary class name, if HAS_ENTRIES and SerVer = 1 (skipped)
 *    next  | 8*C  | Count hashes, if HAS_ENTRIES
 *    next  | var  | Count Summaries, if HAS_ENTRIES
 * </pre>
 *
 * <h2>QuickSelectSketch (updatable), serial version 2 (deprecated)</h2>
 *
 * <p>Serializing an updatable sketch is deprecated. This form has no seed hash and is
 * read only by Java. As with the legacy compact form, the fields after byte 7 are
 * packed, and each one is present only if its flag is set.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||  LgRF  |  LgArr |  LgNom | Flags  | SkType | FamID  | SerVer |   PreLongs = 1     |
 *
 * Byte Adr | Size | Field
 *    8     |  4   | Sampling probability p (float), if IS_IN_SAMPLING_MODE
 *    next  |  8   | THETA_LONG, if IS_THETA_INCLUDED
 *    next  |  4   | Retained Entries Count, if HAS_ENTRIES
 *    next  | var  | Entries: hash (8 bytes), then Summary bytes, repeated Count times
 * </pre>
 *
 * <h2>Flags byte</h2>
 *
 * <pre>
 * Bit | CompactTupleSketch v3 | CompactTupleSketch v1, v2 | QuickSelectSketch v2
 *  0  | Reserved              | IS_BIG_ENDIAN (unused)    | Reserved
 *  1  | IS_READ_ONLY          | IS_EMPTY                  | IS_IN_SAMPLING_MODE
 *  2  | IS_EMPTY              | HAS_ENTRIES               | IS_EMPTY
 *  3  | IS_COMPACT            | IS_THETA_INCLUDED         | HAS_ENTRIES
 *  4  | IS_ORDERED            |                           | IS_THETA_INCLUDED
 * </pre>
 *
 * <h2>Summary encodings of the example Summaries in this library</h2>
 *
 * <p>The Summaries in the <i>adouble</i>, <i>aninteger</i> and <i>strings</i> sub-packages are
 * examples of user-defined Summaries. Their encodings are specific to Java, and other languages
 * can read them only through custom code. The Mode byte is the ordinal of the Summary's Mode:
 * Sum = 0, Min = 1, Max = 2, AlwaysOne = 3.</p>
 *
 * <pre>
 * Summary               | Bytes | Encoding
 * DoubleSummary         |   9   | double value (bytes 0-7), Mode (byte 8)
 * IntegerSummary        |   5   | int value (bytes 0-3), Mode (byte 4)
 * ArrayOfStringsSummary |  var  | int total bytes, byte number of strings,
 *                       |       | then for each string: int length, then its UTF-8 bytes
 * </pre>
 *
 * @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // Byte addresses common to all forms
  static final int PREAMBLE_LONGS_BYTE   = 0;
  static final int SER_VER_BYTE          = 1;
  static final int FAMILY_BYTE           = 2;
  static final int SKETCH_TYPE_BYTE      = 3;

  // CompactTupleSketch, serial version 3
  static final int COMPACT_FLAGS_BYTE    = 5;
  static final int SEED_HASH_SHORT       = 6;  //bytes 6,7
  static final int RETAINED_ENTRIES_INT  = 8;  //only if PreLongs > 1
  static final int THETA_LONG            = 16; //only if PreLongs = 3

  // CompactTupleSketch, serial versions 1 and 2, and QuickSelectSketch
  static final int LEGACY_FLAGS_BYTE     = 4;

  // QuickSelectSketch, serial version 2
  static final int LG_NOM_ENTRIES_BYTE   = 5;
  static final int LG_CUR_CAPACITY_BYTE  = 6;
  static final int LG_RESIZE_FACTOR_BYTE = 7;
  static final int QS_VARIABLE_START     = 8;

  // Serial versions
  static final byte COMPACT_SER_VER_WITH_CLASS_NAME = 1;
  static final byte COMPACT_SER_VER_LEGACY          = 2;
  static final byte COMPACT_SER_VER                 = 3;
  static final byte QUICK_SELECT_SER_VER            = 2;

}
