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

import static java.lang.Math.max;
import static org.apache.datasketches.Util.LS;
import static org.apache.datasketches.Util.TAB;
import static org.apache.datasketches.req.ReqSketch.MIN_K;

/**
 * For building a new ReqSketch
 *
 * @author Lee Rhodes
 */
public class ReqSketchBuilder {
  private final static int DEFAULT_K = 50;
  private int bK = DEFAULT_K;
  private boolean bHRA;
  private boolean bLtEq;
  private boolean bCompatible;
  private ReqDebug bReqDebug;

  /**
   * Constructor for the ReqSketchBuilder. The default configuration is:
   * <ul>
   * <li><b>K = 50:</b> Controls the size and error of the sketch. It must be even and larger than
   * or equal to 4. If not even, it will be rounded down by one.
   * rounded down by one. A value of 50 roughly corresponds to 0.01-relative error guarantee with
   * constant probability.</li>
   * <li><b>High Rank Accuracy (HRA) = true:</b> The high ranks are prioritized for better accuracy.
   * Otherwise, if false, the low ranks are prioritized for better accuracy.</li>
   * <li><b>Less-Than-Or-Equals = false:</b> The sketch will use "&lt;" as the comparison criterion
   * when computing ranks or quantiles. If true, the sketch will use "&le;" as the comparison
   * criterion. This parameter can also be modified after the sketch has been constructed. It is
   * included here for convenience.</li>
   * <li><b>ReqDebug = null:</b> This is the debug signaling interface.
   * </ul>
   */
  public ReqSketchBuilder() {
    bK = DEFAULT_K;
    bHRA = true;
    bLtEq = false;
    bCompatible = true;
    bReqDebug = null;
  }

  /**
   * Gets the builder configured value of k.
   * @return the builder configured value of k.
   */
  public int getK() {
    return bK;
  }

  /**
   * This sets the parameter k.
   * @param k See {@link ReqSketch#ReqSketch(int, boolean, ReqDebug)}
   * @return this
   */
  public ReqSketchBuilder setK(final int k) {
    bK = max(k & -2, MIN_K); //make even and no smaller than MIN_K
    return this;
  }

  /**
   * Gets the builder confibured value of High Rank Accuracy.
   * @return the builder confibured value of High Rank Accuracy.
   */
  public boolean getHighRankAccuracy() {
    return bHRA;
  }

  /**
   * This sets the parameter highRankAccuracy.
   * @param hra See {@link ReqSketch#ReqSketch(int, boolean, ReqDebug)}
   * @return this
   */
  public ReqSketchBuilder setHighRankAccuracy(final boolean hra) {
    bHRA = hra;
    return this;
  }

  /**
   * Gets the builder configured value of Less-Than-Or-Equal.
   * @return the builder confibured value of Less-Than-Or-Equal
   */
  public boolean getLessThanOrEqual() {
    return bLtEq;
  }

  /**
   * Sets the parameter lessThanOrEqual
   * @param ltEq See {@link ReqSketch#setLessThanOrEqual(boolean)}
   * @return this
   */
  public ReqSketchBuilder setLessThanOrEqual(final boolean ltEq) {
    bLtEq = ltEq;
    return this;
  }

  /**
   * gets the builder configured value of compatible.
   * @return the builder configured value of compatible.
   */
  public boolean getCompatible() {
    return bCompatible;
  }

  /**
   * Sets the parameter compatible.
   * @param compatible See {@link ReqSketch#setCompatible(boolean)}.
   * @return this
   */
  public ReqSketchBuilder setCompatible(final boolean compatible) {
    bCompatible = compatible;
    return this;
  }

  /**
   * Gets the builder configured value of ReqDebug
   * @return the builder configured value of ReqDebug, or null.
   */
  public ReqDebug getReqDebug() {
    return bReqDebug;
  }

  /**
   * This sets the parameter reqDebug
   * @param reqDebug See {@link ReqSketch#ReqSketch(int, boolean, ReqDebug)}
   * @return this
   */
  public ReqSketchBuilder setReqDebug(final ReqDebug reqDebug) {
    bReqDebug = reqDebug;
    return this;
  }

  /**
   * Returns a new ReqSketch with the current configuration of the builder.
   * @return a new ReqSketch
   */
  public ReqSketch build() {
    final ReqSketch sk = new ReqSketch(bK, bHRA, bReqDebug);
    sk.setLessThanOrEqual(bLtEq);
    sk.setCompatible(bCompatible);
    return sk;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("ReqSketchBuilder configuration:").append(LS);
    sb.append("K:").append(TAB).append(bK).append(LS);
    sb.append("HRA:").append(TAB).append(bHRA).append(LS);
    sb.append("LtEq").append(TAB).append(bLtEq).append(LS);
    sb.append("ReqDebug").append(TAB).append(bReqDebug).append(LS);
    return sb.toString();
  }

}
