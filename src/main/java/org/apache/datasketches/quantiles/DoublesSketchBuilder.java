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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.Util.LS;
import static org.apache.datasketches.quantiles.Util.TAB;

import org.apache.datasketches.memory.WritableMemory;

/**
 * For building a new quantiles DoublesSketch.
 *
 * @author Lee Rhodes
 */
public class DoublesSketchBuilder {
  private int bK = PreambleUtil.DEFAULT_K;

  /**
   * Constructor for a new DoublesSketchBuilder. The default configuration is
   * <ul>
   * <li>k: 128. This produces a normalized rank error of about 1.7%</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public DoublesSketchBuilder() {}

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch.
   * @param k determines the accuracy and size of the sketch.
   * It is recommended that <i>k</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>. It is only possible to union from
   * larger values of <i>k</i> to smaller values.
   * @return this builder
   */
  public DoublesSketchBuilder setK(final int k) {
    Util.checkK(k);
    bK = k;
    return this;
  }

  /**
   * Gets the current configured value of <i>k</i>
   * @return the current configured value of <i>k</i>
   */
  public int getK() {
    return bK;
  }

  /**
   * Returns an UpdateDoublesSketch with the current configuration of this Builder.
   * @return a UpdateDoublesSketch
   */
  public UpdateDoublesSketch build() {
    return HeapUpdateDoublesSketch.newInstance(bK);
  }

  /**
   * Returns a quantiles UpdateDoublesSketch with the current configuration of this builder
   * and the specified backing destination Memory store.
   * @param dstMem destination memory for use by the sketch
   * @return an UpdateDoublesSketch
   */
  public UpdateDoublesSketch build(final WritableMemory dstMem) {
    return DirectUpdateDoublesSketch.newInstance(bK, dstMem);
  }

  /**
   * Creates a human readable string that describes the current configuration of this builder.
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("QuantileSketchBuilder configuration:").append(LS);
    sb.append("K     : ").append(TAB).append(bK).append(LS);
    return sb.toString();
  }

}
