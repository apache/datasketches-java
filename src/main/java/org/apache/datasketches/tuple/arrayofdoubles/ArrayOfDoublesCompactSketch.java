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

package org.apache.datasketches.tuple.arrayofdoubles;

/**
 * Top level compact tuple sketch of type ArrayOfDoubles. Compact sketches are never created 
 * directly.  They are created as a result of the compact() method on a QuickSelectSketch
 * or the getResult() method of a set operation like Union, Intersection or AnotB.
 * Compact sketch consists of a compact list (i.e. no intervening spaces) of hash values,
 * corresponding list of double values, and a value for theta. The lists may or may
 * not be ordered. A compact sketch is read-only.
 */
public abstract class ArrayOfDoublesCompactSketch extends ArrayOfDoublesSketch {

  static final byte serialVersionUID = 1;

  // Layout of retained entries:
  // Long || Start Byte Adr:
  // Adr: 
  //      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16     |
  //  3   ||-----------------------------------|----------Retained Entries------------|

  static final int EMPTY_SIZE = 16;
  static final int RETAINED_ENTRIES_INT = 16;
  // 4 bytes of padding for 8 byte alignment
  static final int ENTRIES_START = 24;

  ArrayOfDoublesCompactSketch(final int numValues) {
    super(numValues);
  }
}
