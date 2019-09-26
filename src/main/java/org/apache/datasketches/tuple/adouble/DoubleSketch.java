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

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.UpdatableSketch;

/**
 * @author Lee Rhodes
 */
public class DoubleSketch extends UpdatableSketch<Double, DoubleSummary> {

  /**
   * Constructs this sketch with given <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param mode The DoubleSummary mode to be used
   */
  public DoubleSketch(final int lgK, final DoubleSummary.Mode mode) {
    super(1 << lgK, ResizeFactor.X8.ordinal(), 1.0F, new DoubleSummaryFactory(mode));
  }

  /**
   * Constructs this sketch from a Memory image, which must be from an DoubleSketch, and
   * usually with data.
   * @param mem the given Memory
   * @param mode The DoubleSummary mode to be used
   */
  public DoubleSketch(final Memory mem, final DoubleSummary.Mode mode) {
    super(mem, new DoubleSummaryDeserializer(), new DoubleSummaryFactory(mode));
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
