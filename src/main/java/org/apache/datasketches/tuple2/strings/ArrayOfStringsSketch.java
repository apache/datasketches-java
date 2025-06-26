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

package org.apache.datasketches.tuple2.strings;

import static org.apache.datasketches.tuple2.Util.stringArrHash;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.tuple2.UpdatableSketch;

/**
 * Extends UpdatableSketch&lt;String[], ArrayOfStringsSummary&gt;
 * @author Lee Rhodes
 */
public class ArrayOfStringsSketch extends UpdatableSketch<String[], ArrayOfStringsSummary> {

  /**
   * Constructs new sketch with default <i>K</i> = 4096 (<i>lgK</i> = 12), default ResizeFactor=X8,
   * and default <i>p</i> = 1.0.
   */
  public ArrayOfStringsSketch() {
    this(12);
  }

  /**
   * Constructs new sketch with default ResizeFactor=X8, default <i>p</i> = 1.0 and given <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   */
  public ArrayOfStringsSketch(final int lgK) {
    this(lgK, ResizeFactor.X8, 1.0F);
  }

  /**
   * Constructs new sketch with given ResizeFactor, <i>p</i> and <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param rf ResizeFactor
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param p sampling probability
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   */
  public ArrayOfStringsSketch(final int lgK, final ResizeFactor rf, final float p) {
    super(1 << lgK, rf.lg(), p, new ArrayOfStringsSummaryFactory());
  }

  /**
   * Constructs this sketch from a MemorySegment image, which must be from an ArrayOfStringsSketch, and
   * usually with data.
   * @param seg the given MemorySegment
   * @deprecated As of 3.0.0, heapifying an UpdatableSketch is deprecated.
   * This capability will be removed in a future release.
   * Heapifying a CompactSketch is not deprecated.
   */
  @Deprecated
  public ArrayOfStringsSketch(final MemorySegment seg) {
    super(seg, new ArrayOfStringsSummaryDeserializer(), new ArrayOfStringsSummaryFactory());
  }

  /**
   * Copy Constructor
   * @param sketch the sketch to copy
   */
  public ArrayOfStringsSketch(final ArrayOfStringsSketch sketch) {
    super(sketch);
  }

  /**
   * @return a deep copy of this sketch
   */
  @Override
  public ArrayOfStringsSketch copy() {
    return new ArrayOfStringsSketch(this);
  }

  /**
   * Updates the sketch with String arrays for both key and value.
   * @param strArrKey the given String array key
   * @param strArr the given String array value
   */
  public void update(final String[] strArrKey, final String[] strArr) {
    super.update(stringArrHash(strArrKey), strArr);
  }

}
