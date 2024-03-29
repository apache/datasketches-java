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

package org.apache.datasketches.req;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.TAB;

/**
 * For building a new ReqSketch
 *
 * @author Lee Rhodes
 */
public class ReqSketchBuilder {
  private final static int DEFAULT_K = 12;
  private int bK = DEFAULT_K;
  private boolean bHRA;
  private ReqDebug bReqDebug;

  /**
   * Constructor for the ReqSketchBuilder.
   */
  public ReqSketchBuilder() {
    bK = DEFAULT_K;
    bHRA = true;
    bReqDebug = null;
  }

  /**
   * Returns a new ReqSketch with the current configuration of the builder.
   * @return a new ReqSketch
   */
  public ReqSketch build() {
    final ReqSketch sk = new ReqSketch(bK, bHRA, bReqDebug);
    return sk;
  }

  /**
   * Gets the builder configured High Rank Accuracy.
   * @return the builder configured High Rank Accuracy.
   */
  public boolean getHighRankAccuracy() {
    return bHRA;
  }

  /**
   * Gets the builder configured k.
   * @return the builder configured k.
   */
  public int getK() {
    return bK;
  }

  /**
   * Gets the builder configured ReqDebug
   * @return the builder configured ReqDebug, or null.
   */
  public ReqDebug getReqDebug() {
    return bReqDebug;
  }

  /**
   * This sets the parameter highRankAccuracy.
   * @param hra See  <i>ReqSketch#ReqSketch(int, boolean, ReqDebug)</i>
   * @return this
   */
  public ReqSketchBuilder setHighRankAccuracy(final boolean hra) {
    bHRA = hra;
    return this;
  }

  /**
   * This sets the parameter k.
   * @param k See <i>ReqSketch#ReqSketch(int, boolean, ReqDebug)</i>
   * @return this
   */
  public ReqSketchBuilder setK(final int k) {
    bK = k;
    return this;
  }

  /**
   * This sets the parameter reqDebug.
   * @param reqDebug See <i>ReqSketch#ReqSketch(int, boolean, ReqDebug)</i>
   * @return this
   */
  public ReqSketchBuilder setReqDebug(final ReqDebug reqDebug) {
    bReqDebug = reqDebug;
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("ReqSketchBuilder configuration:").append(LS);
    sb.append("K:").append(TAB).append(bK).append(LS);
    sb.append("HRA:").append(TAB).append(bHRA).append(LS);
    final String valid = bReqDebug != null ? "valid" : "invalid";
    sb.append("ReqDebug:").append(TAB).append(valid).append(LS);
    return sb.toString();
  }

}
