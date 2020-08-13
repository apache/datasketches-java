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

package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.RelativeErrorUtil.DEFAULT_EPS;
import static org.apache.datasketches.kll.RelativeErrorUtil.EPS_UPPER_BOUND;
import static org.apache.datasketches.kll.RelativeErrorUtil.INIT_NUMBER_OF_SECTIONS;
import static org.apache.datasketches.kll.RelativeErrorUtil.NEVER_SIZE_SCALAR;
import static org.apache.datasketches.kll.RelativeErrorUtil.SECTION_SIZE_SCALAR;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.RelativeErrorUtil.Schedule;

/**
 * @author Lee Rhodes
 */
public class RelativeErrorSketchBuilder {
  private double bEps = DEFAULT_EPS;
  private Schedule bSchedule = Schedule.DETERMINISTIC;
  private int bInitNumSections = INIT_NUMBER_OF_SECTIONS;
  private boolean bLazy = true;
  private boolean bAlternate = true;
  private boolean bNeverGrows = false;
  private int bAlways = -1;
  private int bNever = -1;
  private int bSectionSize = -1;

  /**
   * Default constructor
   */
  public RelativeErrorSketchBuilder() {}

  /**
   * Builds a RelativeErrorSketch
   * @return a RelativeErrorSketch
   */
  public RelativeErrorSketch build() {
    // default setting of sectionSize, always, and "never", according to eps
    if (bSectionSize == -1) {
      // ensured to be even and positive (thus >= 2)
      bSectionSize = 2 * ((int)(SECTION_SIZE_SCALAR / bEps) + 1);
    }
    if (bAlways == -1) { bAlways = bSectionSize; }

    bNeverGrows = false; //if never is set true by the user, then we do not let it grow
    if (bNever == -1) {
      bNever = (int)(NEVER_SIZE_SCALAR * bSectionSize * bInitNumSections);
      bNeverGrows = true;
    }
    return new RelativeErrorSketch(bEps, bSchedule, bAlways, bNever, bSectionSize, bInitNumSections,
        bLazy, bAlternate, bNeverGrows);
  }

  /**
   * Set the target error rate. Must be less than .1. Default = .01.
   * @param eps the target error rate
   */
  public void setEps(final double eps) {
    if (eps > EPS_UPPER_BOUND) {
      throw new SketchesArgumentException("eps must be at most " + EPS_UPPER_BOUND);
    }
    bEps = eps;
  }

  /**
   * Set whether DETERMINISTIC, RANDOMIZED or RANDOMIZED_LINAR. Default = DETERMINISTIC.
   * @param schedule whether DETERMINISTIC, RANDOMIZED or RANDOMIZED_LINAR.
   */
  public void setSchedule(final Schedule schedule) {
    bSchedule = schedule;
  }

  /**
   * Sets the initial number of sections. Default = 2.
   * @param initNumSections the initial number of sections.
   */
  public void setInitNumSections(final int initNumSections) {
    bInitNumSections = initNumSections;
  }

  /**
   * Sets lazy.
   * @param lazy if true, stop compressing after the first compressible candidate found.
   */
  public void setLazy(final boolean lazy) {
    bLazy = lazy;
  }

  /**
   * Sets alternate.
   * @param alternate if true, then random choice of odd/even done every other time.
   */
  public void setAlternate(final boolean alternate) {
    bAlternate = alternate;
  }

  /**
   * Sets always
   * @param always the size of the buffer that is always compacted
   */
  public void setAlways(final int always) {
    bAlways = always;
  }

  /**
   * Sets never
   * @param never the size of the buffer that is never compacted
   */
  public void setNever(final int never) {
    bNever = never;
  }
}
