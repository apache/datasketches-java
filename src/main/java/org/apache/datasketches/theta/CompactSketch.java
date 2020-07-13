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

package org.apache.datasketches.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The parent class of all the CompactSketches. CompactSketches are never created directly.
 * They are created as a result of the compact() method of an UpdateSketch or as a result of a
 * getResult() of a SetOperation.
 *
 * <p>A CompactSketch is the simplest form of a Theta Sketch. It consists of a compact list
 * (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta
 * and a seed hash.  A CompactSketch is read-only,
 * and the space required when stored is only the space required for the hash values and 8 to 24
 * bytes of preamble. An empty CompactSketch consumes only 8 bytes.</p>
 *
 * @author Lee Rhodes
 */
public abstract class CompactSketch extends Sketch {

  //Sketch

  @Override
  public abstract CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem);

  @Override
  public int getCompactBytes() {
    return getCurrentBytes();
  }

  @Override
  int getCurrentDataLongs() {
    return getRetainedEntries(true);
  }

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

}
