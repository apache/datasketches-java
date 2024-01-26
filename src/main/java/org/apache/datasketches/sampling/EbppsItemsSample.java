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

package org.apache.datasketches.sampling;

import static org.apache.datasketches.common.Util.LS;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

// this is a supporting class used to hold the raw data sample
class EbppsItemsSample<T> {

  private double c_;            // Current sample size, including fractional part
  private T partialItem_;       // a sample item corresponding to a partial weight
  private ArrayList<T> data_;   // full sample items
  
  private Random rand_;         // ThreadLocalRandom.current() in general

  // basic constructor
  EbppsItemsSample(final int reservedSize) {
    c_ = 0.0;
    data_ = new ArrayList<>(reservedSize);
    rand_ = ThreadLocalRandom.current();
  }

  // copy constructor used during merge
  EbppsItemsSample(final EbppsItemsSample<T> other) {
    c_ = other.c_;
    partialItem_ = other.partialItem_;
    data_ = new ArrayList<>(other.data_);
    rand_ = other.rand_;
  }

  // constructor used for deserialization and testing
  // does NOT copy the incoming ArrayList since this is an internal
  // class's package-private constructor, not something directly
  // taking user data
  EbppsItemsSample(ArrayList<T> data, T partialItem, final double c) {
    c_ = c;
    partialItem_ = partialItem;
    data_ = data;
    rand_ = ThreadLocalRandom.current();
  }

  // Used in lieu of a constructor to populate a temporary sample
  // with data before immediately merging it. This approach
  // avoids excessive object allocation calls.
  // rand_ is not set since it is not expected to be used from
  // this object
  void replaceContent(final T item, final double theta) {
    c_ = theta;
    if (theta == 1.0) {
      if (data_ != null && data_.size() == 1) {
        data_.set(0, item);
      } else {
        data_ = new ArrayList<T>(1);
        data_.add(item);  
      }
      partialItem_ = null;
    } else {
      data_ = null;
      partialItem_ = item;
    }
  }

  public void reset() {
    c_ = 0.0;
    partialItem_ = null;
    data_.clear();
  }

  public ArrayList<T> getSample() {
    final double cFrac = c_ % 1;
    final boolean includePartial = partialItem_ != null && rand_.nextDouble() < cFrac;
    final int resultSize = (data_ != null ? data_.size() : 0) + (includePartial ? 1 : 0);

    if (resultSize == 0)
      return null;

    ArrayList<T> result = new ArrayList<>(resultSize);
    for (T item : data_)
      result.add(item);

    if (includePartial)
      result.add(partialItem_);

    return result;      
  }

  @SuppressWarnings("unchecked")
  T[] getAllSamples(final Class<?> clazz) {
    // Is it faster to use sublist and append 1?
    final T[] itemsArray = (T[]) Array.newInstance(clazz, getNumRetainedItems());
    int i = 0;
    if (data_ != null) {
      for (T item : data_) {
        if (item != null) {
          itemsArray[i++] = item;
        }
      }
    }
    if (partialItem_ != null)
      itemsArray[i] = partialItem_; // no need to increment i again

    return itemsArray;
  }

  // package-private for use in merge and serialization
  ArrayList<T> getFullItems() {
    return data_;
  }

  // package-private for use in merge and serialization
  T getPartialItem() {
    return partialItem_;
  }

  public double getC() { return c_; }

  boolean hasPartialItem() { return partialItem_ != null; }

  // for testing to allow setting the seed
  void replaceRandom(Random r) {
    rand_ = r;
  }

  public void downsample(final double theta) {
    if (theta >= 1.0) return;

    double newC = theta * c_;
    double newCInt = Math.floor(newC);
    double newCFrac = newC % 1;
    double cInt = Math.floor(c_);
    double cFrac = c_ % 1;

    if (newCInt == 0.0) {
      // no full items retained
      if (rand_.nextDouble() > (cFrac / c_)) {
        swapWithPartialItem();
      }
      data_.clear();
    } else if (newCInt == cInt) {
      // no items deleted
      if (rand_.nextDouble() > (1 - theta * cFrac)/(1 - newCFrac)) {
        swapWithPartialItem();
      }
    } else {
      if (rand_.nextDouble() < theta * cFrac) {
        // subsample data in random order; last item is partial
        // create sample size newC then swapWithPartialItem()
        subsample((int) newCInt);
        swapWithPartialItem();
      } else {
        // create sample size newCInt + 1 then moveOneToPartialItem()
        subsample((int) newCInt + 1);
        moveOneToPartialItem();
      }
    }

    if (newC == newCInt)
      partialItem_ = null;

    c_ = newC;
  }

  public void merge(final EbppsItemsSample<T> other) {
    //double cInt = Math.floor(c_);
    double cFrac = c_ % 1;
    double otherCFrac = other.c_ % 1;

    // update c_ here but do NOT recompute fractional part yet
    c_ += other.c_;

    if (other.data_ != null)
      data_.addAll(other.data_);

    // This modifies the original algorithm slightly due to numeric
    // precision issues. Specifically, the test if cFrac + otherCFrac == 1.0
    // happens before tests for < 1.0 or > 1.0 and can also be triggered
    // if c_ == floor(c_) (the updated value of c_, not the input).
    //
    // We can still run into issues where cFrac + otherCFrac == epsilon
    // and the first case would have ideally triggered. As a result, we must
    // check if the partial item exists before adding to the data_ vector.

    if (cFrac == 0.0 && otherCFrac == 0.0) {
      partialItem_ = null;
    } else if (cFrac + otherCFrac == 1.0 || c_ == Math.floor(c_)) {
      if (rand_.nextDouble() <= cFrac) {
        if (partialItem_ != null) {
          data_.add(partialItem_);
        }
      } else {
        if (other.partialItem_ != null) {
          data_.add(other.partialItem_);
        }
      }
      partialItem_ = null;
    } else if (cFrac + otherCFrac < 1.0) {
      if (rand_.nextDouble() > cFrac / (cFrac + otherCFrac)) {
        partialItem_ = other.partialItem_;
      }
    } else { // cFrac + otherCFrac > 1
      if (rand_.nextDouble() <= (1 - cFrac) / ((1 - cFrac) + (1 - otherCFrac))) {
        data_.add(other.partialItem_);
      } else {
        data_.add(partialItem_);
        partialItem_ = other.partialItem_;
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append("  sample:").append(LS);
    int idx = 0;
    for (T item : data_)
      sb.append("\t").append(idx++).append(":\t").append(item.toString()).append(LS);
    sb.append("  partial: ");
    if (partialItem_ != null)
      sb.append(partialItem_.toString()).append(LS);
    else
      sb.append("NULL").append(LS);

    return sb.toString();
  }

  void subsample(final int numSamples) {
    // we can perform a Fisher-Yates style shuffle, stopping after
    // numSamples points since subsequent swaps would only be
    // between items after num_samples. This is valid since a
    // point from anywhere in the initial array would be eligible
    // to end up in the final subsample.

    if (numSamples == data_.size()) return;

    int dataLen = data_.size();
    for (int i = 0; i < numSamples; ++i) {
      int j = i + rand_.nextInt(dataLen - i);
      // swap i and j
      T tmp = data_.get(i);
      data_.set(i, data_.get(j));
      data_.set(j, tmp);
    }

    // clear anything beyond numSamples
    data_.subList(numSamples, data_.size()).clear();
  }

  void swapWithPartialItem() {
    if (partialItem_ == null) {
      moveOneToPartialItem();
    } else {
      int idx = rand_.nextInt(data_.size());
      T tmp = partialItem_;
      partialItem_ = data_.get(idx);
      data_.set(idx, tmp);
    }
  }

  void moveOneToPartialItem() {
    int idx = rand_.nextInt(data_.size());
    // swap selected item to end so we can delete it easily
    int lastIdx = data_.size() - 1;
    if (idx != lastIdx) {
      T tmp = data_.get(idx);
      data_.set(idx, data_.get(lastIdx));
      partialItem_ = tmp;
    } else {
      partialItem_ = data_.get(lastIdx);
    }

    data_.remove(lastIdx);
  }

  public int getNumRetainedItems() {
    return (data_ != null ? data_.size() : 0)
         + (partialItem_ != null ? 1 : 0);
  }
}
