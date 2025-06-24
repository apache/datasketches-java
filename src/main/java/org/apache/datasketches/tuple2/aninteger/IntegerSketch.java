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

package org.apache.datasketches.tuple2.aninteger;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.tuple2.UpdatableSketch;

/**
 * Extends UpdatableSketch&lt;Integer, IntegerSummary&gt;
 * @author Lee Rhodes
 */
public class IntegerSketch extends UpdatableSketch<Integer, IntegerSummary> {

  /**
   * Constructs this sketch with given <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param mode The IntegerSummary mode to be used
   */
  public IntegerSketch(final int lgK, final IntegerSummary.Mode mode) {
    this(lgK, ResizeFactor.X8.ordinal(), 1.0F, mode);
  }

  /**
   * Creates this sketch with the following parameters:
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param samplingProbability
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param mode The IntegerSummary mode to be used
   */
  public IntegerSketch(final int lgK, final int lgResizeFactor, final float samplingProbability,
      final IntegerSummary.Mode mode) {
    super(1 << lgK, lgResizeFactor, samplingProbability, new IntegerSummaryFactory(mode));
  }

  /**
   * Constructs this sketch from a MemorySegment image, which must be from an IntegerSketch, and
   * usually with data.
   * @param seg the given MemorySegment
   * @param mode The IntegerSummary mode to be used
   * @deprecated As of 3.0.0, heapifying an UpdatableSketch is deprecated.
   * This capability will be removed in a future release.
   * Heapifying a CompactSketch is not deprecated.
   */
  @Deprecated
  public IntegerSketch(final MemorySegment seg, final IntegerSummary.Mode mode) {
    super(seg, new IntegerSummaryDeserializer(), new IntegerSummaryFactory(mode));
  }

  @Override
  public void update(final String key, final Integer value) {
    super.update(key, value);
  }

  @Override
  public void update(final long key, final Integer value) {
    super.update(key, value);
  }

}
