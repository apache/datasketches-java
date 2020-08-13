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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.datasketches.kll.RelativeErrorUtil.Schedule;

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
public class RelativeErrorSketch {

  static {
    final double x = sqrt(2);
    final double y = ceil(x);
    final Random rand =  new Random();
  }

  //final class parameters
  private final double eps; //default = DEFAULT_EPS = .01
  private final Schedule schedule; //default = Schedule.DETERMINISTIC;
  private final int initNumSections; //default = INIT_NUMBER_OF_SECTIONS;
  private final boolean lazy; //default = true;
  private final boolean alternate;//default = true;
  private final boolean neverGrows; //default = false;

  //class variables
  private int always;
  private int never; //the part that is never compacted. Default = sectionSize * numSections
  private int sectionSize; //the number of sections & determined by eps

  private List<RelativeCompactor> compactors = new ArrayList<>();
  private int H = 0;
  private int size = 0;
  private int maxSize = 0;

  /**
   * Constructor.
   * @param eps target error rate. Must be less than .1. Default = .01.
   * @param sch schedule, whether deterministic or randomized. Default = deterministic.
   * @param always the size of the buffer that is always compacted
   * @param never the size of the buffer that is never compacted
   * @param sectionSize the size of a section in the buffer
   * @param initNumSections the initial number of sections. Default = 2.
   * @param lazy if true, stop compressing after the first compressible candidate found.
   * @param alternate if true, then random choice of odd/even done every other time.
   * @param neverGrows if true, we do not let "never" grow.
   */
  RelativeErrorSketch(
      final double eps,
      final Schedule schedule,
      final int always,
      final int never,
      final int sectionSize,
      final int initNumSections,
      final boolean lazy,
      final boolean alternate,
      final boolean neverGrows) {
    this.eps = eps;
    this.schedule = schedule;
    this.always = always;
    this.never = never;
    this.sectionSize = sectionSize;
    //an initial upper bound on log_2 of the number of compactions
    this.initNumSections = initNumSections;
    this.lazy = lazy;
    this.alternate = alternate;
    this.neverGrows = neverGrows;

    H = 0;
    size = 0;
    maxSize = 0;
    grow();
  }

  void grow() {
    compactors.add(
        new RelativeCompactor(schedule,
                              sectionSize,
                              initNumSections,
                              always,
                              never,
                              neverGrows,
                              H,
                              alternate
        ));
    H = compactors.size();
    updateMaxSize();
  }

  void updateMaxSize() {
    int maxSize = 0;
    for (RelativeCompactor c : compactors) { maxSize += c.capacity(); }
    this.maxSize = maxSize;
  }

  void update(final float item) {
    final RelativeCompactor c = compactors.get(0);

  }

  void compress(final boolean lazy) {

  }

  void mergeIntoSelf(final RelativeErrorSketch sketch) {

  }

  void merge(final RelativeErrorSketch sketch1, final RelativeErrorSketch sketch2) {

  }

  int rank(final float value) {
    return 0;
  }

  Pair[] cdf() {
    return null;
  }

  Pair[] ranks() {
    return null;
  }

  class Pair {
    float rank;
    float value;
  }

}


