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

import static org.apache.datasketches.kll.RelativeErrorUtil.INIT_NUMBER_OF_SECTIONS;

import java.util.Arrays;

import org.apache.datasketches.kll.RelativeErrorUtil.Schedule;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"javadoc","unused"})
public class RelativeCompactor {
  private int numCompactions = 0; //number of compaction operations performed
  private int state = 0; //state of the deterministic compaction schedule
  private int offset = 0; //0 or 1 uniformly at random in each compaction
  private float[] buf;
  private boolean sorted = false;
  private int numValues = 0;

  //extracted from constructor
  private boolean alternate = true; //every other compaction has the opposite offset
  private int sectionSize = 32;
  private int numSections = INIT_NUMBER_OF_SECTIONS; //# of sections in the buffer
  private int always = sectionSize;
  private int never = sectionSize * numSections;
  private boolean neverGrows = true;
  private int height = 0;
  private Schedule schedule = Schedule.DETERMINISTIC;

  //derived
  private float sectionSizeF = sectionSize;

  //Empty Constructor, assume all defaults
  public RelativeCompactor() { }

  //Constructor
  public RelativeCompactor(
      final Schedule schedule,
      final int sectionSize,
      final int numSections,
      final int always,
      final int never,
      final boolean neverGrows,
      final int height,
      final boolean alternate) {
    this.schedule = schedule;
    this.sectionSize = sectionSize;
    this.numSections = numSections;
    this.always = always;
    this.never = never;
    this.neverGrows = neverGrows;
    this.height = height;
    this.alternate = alternate;
    final int cap = never + (numSections * sectionSize) + always;
    buf = new float[cap];
  }

  public float[] getBuf() { return buf; }

  public int getNumValues() { return numValues; }

  public void incrementNumValus() { numValues++; }

  public void compact() {
    //assert
  }

  public int capacity() {
    final int cap = never + (numSections * sectionSize) + always;
    assert cap > 1;
    return cap;
  }

  public int rank(final float value) { //one-based
    sort();
    int count = 0;
    for (int i = 0; i < numValues; i++) {
      if (buf[i] <= value) { count++; continue; }
      break;
    }
    return count;
  }

  public void sort() {
    if (!sorted) {
      Arrays.sort(buf, 0, numValues);
      sorted = true;
    }
  }
}
