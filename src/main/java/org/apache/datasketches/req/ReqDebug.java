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

/**
 * The signaling interface that allows comprehensive analysis of the ReqSketch and ReqCompactor
 * while eliminating code clutter in the main classes. The implementation of this interface can be
 * found in the test tree.
 *
 * @author Lee Rhodes
 */
public interface ReqDebug {

  //Sketch signals

  /**
   * Emit the start signal
   * @param sk the sketch
   */
  void emitStart(ReqSketch sk);

  /**
   * Emit Start Compress
   */
  void emitStartCompress();

  /**
   * Emit compress done.
   */
  void emitCompressDone();

  /**
   * Emit all horizontal lists
   */
  void emitAllHorizList();

  /**
   * Emit Must add compactor
   */
  void emitMustAddCompactor();

  //Compactor signals

  /**
   * Emit Compaction Start.
   * @param lgWeight compactor lgWeight or height
   */
  void emitCompactingStart(byte lgWeight);

  /**
   * Emit new compactor configuration
   * @param lgWeight the log weight
   */
  void emitNewCompactor(byte lgWeight);

  /**
   * Emit adjusting section size and number of sections.
   * @param lgWeight the log weight
   */
  void emitAdjSecSizeNumSec(byte lgWeight);

  /**
   * Emit Compaction details.
   * @param compactionStart the offset of compaction start
   * @param compactionEnd the offset of compaction end
   * @param secsToCompact the number of sections to compact
   * @param promoteLen the length of the promotion field
   * @param coin the state of the random coin.
   */
  void emitCompactionDetail(int compactionStart, int compactionEnd,
      int secsToCompact, int promoteLen, boolean coin);

  /**
   * Emit compaction done and number of compactions so far.
   * @param lgWeight the log weight
   */
  void emitCompactionDone(byte lgWeight);

}
