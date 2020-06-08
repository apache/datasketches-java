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

package org.apache.datasketches.tuple;

/**
 * Iterator over a generic tuple sketch
 * @param <S> Type of Summary
 */
public class SketchIterator<S extends Summary> {

  private long[] hashArrTbl_; //could be either hashArr or hashTable
  private S[] summaryArrTbl_; //could be either summaryArr or summaryTable
  private int i_;

  SketchIterator(final long[] hashes, final S[] summaries) {
    hashArrTbl_ = hashes;
    summaryArrTbl_ =  summaries;
    i_ = -1;
  }

  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next() {
    if (hashArrTbl_ == null) { return false; }
    i_++;
    while (i_ < hashArrTbl_.length) {
      if (hashArrTbl_[i_] > 0) { return true; }
      i_++;
    }
    return false;
  }

  /**
   * Gets the hash from the current entry in the sketch, which is a hash
   * of the original key passed to update(). The original keys are not
   * retained. Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return hash from the current entry
   * @deprecated Please use {@link #getHash()}
   */
  @Deprecated
  public long getKey() {
    return hashArrTbl_[i_];
  }

  /**
   * Gets the hash from the current entry in the sketch, which is a hash
   * of the original key passed to update(). The original keys are not
   * retained. Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return hash from the current entry
   */
  public long getHash() {
    return hashArrTbl_[i_];
  }

  /**
   * Gets a Summary object from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return Summary object for the current entry (this is not a copy!)
   */
  public S getSummary() {
    return summaryArrTbl_[i_];
  }

}
