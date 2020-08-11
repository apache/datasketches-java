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

import static java.lang.Math.ceil;
import static java.lang.Math.sqrt;

import java.util.Random;

/**
 * Proof-of-concept code for paper "Relative Error Streaming Quantiles",
 * https://arxiv.org/abs/2004.01668.
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 * <ul><li>The algorithm requires no upper bound on the stream length (input size).
 * Instead, each relative-compactor (i.e. buffer) counts the number of compaction operations performed
 * so far (variable numCompactions). Initially, the relative-compactor starts with 2 buffer sections
 * and each time the numCompactions exceeds 2^{# of sections}, we double the number of sections
 * (variable numSections).</li>
 * <li>The size of each buffer section (variable sectionSize in the code and parameter k in the paper)
 * is initialized with a value set by the user via variable sectionSize (parameter -sec)
 * or via setting epsilon (parameter -eps). Setting the failure probability delta is not implememnted.
 * When the number of sections doubles, we decrease sectionSize by a factor of sqrt(2)
 * (for which we use a float variable sectionSizeF). As in item 1), this is applied
 * at each level separately.
 * Thus, when we double the number of section, the buffer size increases by a factor of sqrt(2)
 * (up to +-1 after rounding). For experimental purposes, the buffer consists of three parts:
 * <ul><li>a part that is never compacted (its size can be set by variable never),</li>
 * <li>numSections many sections of size sectionSize, and</li>
 * <li>a part that is always involved in a compaction (its size can be set by variable always).</li>
 * </ul></li>
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight analysis of the sketch.</li>
 * </ul>
 *
 * @author Edo Liberty
 * @author Pavel Vesely
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class KllReFloatsSketch {

  static {
    final double x = sqrt(2);
    final double y = ceil(x);
    final Random rand =  new Random();
  }

  // Constants

  private final static double SECTION_SIZE_SCALAR = 0.5;
  private final static double NEVER_SIZE_SCALAR = 0.5;
  private final static int INIT_NUMBER_OF_SECTIONS = 2;
  private final static int SMALLEST_MEANINGFUL_SECTION_SIZE = 4;
  private final static double DEFAULT_EPS = 0.01;
  private final static double EPS_UPPER_BOUND = 0.1; //the sketch gives rather bad results for eps > 0.1

  /**
   * Schedule
   * @author Lee Rhodes
   */
  @SuppressWarnings("javadoc")
  public enum Schedule { DETERMINISTIC, RANDOMIZED }

  //class variables
  private double eps = DEFAULT_EPS;
  private Schedule sch = Schedule.DETERMINISTIC;
  private int always = -1;
  private int never = -1;
  private int sectionSize = -1;
  private int initNumSections = INIT_NUMBER_OF_SECTIONS;
  private boolean lazy = true;
  private boolean alternate = true;
  private boolean neverGrows = false;
  private RelativeCompactor compactor = new RelativeCompactor();
  private RelativeCompactor[] compactors = new RelativeCompactor[0];
  private int H = 0;
  private int size = 0;


  /**
   * Constructor.
   * @param eps blah
   * @param sch blah
   * @param always blah
   * @param never blah
   * @param sectionSize blah
   * @param initNumSections blah
   * @param lazy blah
   * @param alternate blah
   */
  public KllReFloatsSketch(
      final double eps,
      final Schedule sch,
      final int always,
      final int never,
      final int sectionSize,
      final int initNumSections,
      final boolean lazy,
      final boolean alternate) {
    this.eps = eps;
    this.sch = sch;
    this.always = always;
    this.never = never;
    this.sectionSize = sectionSize;
    //an initial upper bound on log_2 of the number of compactions
    this.initNumSections = initNumSections;
    this.lazy = lazy;
    this.alternate = alternate;

    // default setting of sectionSize, always, and never according to eps
    if (this.sectionSize == -1) {
      // ensured to be even and positive (thus >= 2)
      this.sectionSize = (2 * (int)(SECTION_SIZE_SCALAR / eps)) + 1;
    }
    if (this.always == -1) { this.always = sectionSize; }

    neverGrows = false; //if never is set by the user, then we do not let it grow
    if (this.never == -1) {
      this.never = (int)(NEVER_SIZE_SCALAR * this.sectionSize * this.initNumSections);
      neverGrows = true;
    }
    compactors = new RelativeCompactor[0];
    H = 0;
    size = 0;
    grow();
  }

  void grow() {

  }


  class RelativeCompactor {
    int numCompactions = 0;

    public RelativeCompactor() {
      numCompactions = 0; // number of compaction operations performed
    }
  }

}


