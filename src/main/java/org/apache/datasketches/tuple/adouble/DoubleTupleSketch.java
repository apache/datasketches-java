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

package org.apache.datasketches.tuple.adouble;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.tuple.UpdatableTupleSketch;

/**
 * Extends UpdatableTupleSketch&lt;Double, DoubleSummary&gt;
 * @author Lee Rhodes
 */
public class DoubleTupleSketch extends UpdatableTupleSketch<Double, DoubleSummary> {

  /**
   * Constructs this sketch with given <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param mode The DoubleSummary mode to be used
   */
  public DoubleTupleSketch(final int lgK, final DoubleSummary.Mode mode) {
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
   * @param mode The DoubleSummary mode to be used
   */
  public DoubleTupleSketch(final int lgK, final int lgResizeFactor, final float samplingProbability,
      final DoubleSummary.Mode mode) {
    super(1 << lgK, lgResizeFactor, samplingProbability, new DoubleSummaryFactory(mode));
  }

  /**
   * Constructs this sketch from a MemorySegment image, which must be from an DoubleTupleSketch, and
   * usually with data.
   * @param seg the given MemorySegment
   * @param mode The DoubleSummary mode to be used
   * @deprecated As of 3.0.0, heapifying an UpdatableTupleSketch is deprecated.
   * This capability will be removed in a future release.
   * Heapifying a CompactTupleSketch is not deprecated.
   */
  @Deprecated
  public DoubleTupleSketch(final MemorySegment seg, final DoubleSummary.Mode mode) {
    super(seg, new DoubleSummaryDeserializer(), new DoubleSummaryFactory(mode));
  }

  @Override
  public void update(final String key, final Double value) {
    super.update(key, value);
  }

  @Override
  public void update(final long key, final Double value) {
    super.update(key, value);
  }
}
