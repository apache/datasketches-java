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

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.TAB;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentRequest;

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
   * <li>MemorySegment: null</li>
   * </ul>
   */
  public DoublesSketchBuilder() {}

  /**
   * Sets the parameter <i>k</i> that determines the accuracy and size of the sketch.
   * @param k determines the accuracy and size of the sketch.
   * It is recommended that <i>k</i> be a power of 2 to enable unioning of sketches with
   * different <i>k</i>. It is only possible to union from
   * larger <i>k</i> to smaller <i>k</i>.
   * @return this builder
   */
  public DoublesSketchBuilder setK(final int k) {
    ClassicUtil.checkK(k);
    bK = k;
    return this;
  }

  /**
   * Gets the current configured <i>k</i>
   * @return the current configured <i>k</i>
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
   * Returns a UpdateDoublesSketch with the current configuration of this builder
   * and the specified backing destination MemorySegment store that can grow.
   * @param dstSeg destination MemorySegment for use by the sketch
   * @return an UpdateDoublesSketch
   */
  public UpdateDoublesSketch build(final MemorySegment dstSeg) {
    return this.build(dstSeg, null);
  }

  /**
   * Returns a UpdateDoublesSketch with the current configuration of this builder
   * and the specified backing destination MemorySegment store that can grow.
   * @param dstSeg destination MemorySegment for use by the sketch
   * @param mSegReq the MemorySegmentRequest used if the incoming MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return an UpdateDoublesSketch
   */
  public UpdateDoublesSketch build(final MemorySegment dstSeg, final MemorySegmentRequest mSegReq) {
    return DirectUpdateDoublesSketch.newInstance(bK, dstSeg, mSegReq);
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
