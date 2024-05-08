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


package org.apache.datasketches.filters.quotientfilter;

import java.util.ArrayDeque;
import java.util.Queue;

public class Iterator  {

    QuotientFilter qf;
    long index;
    long bucket_index;
    long fingerprint;
    Queue<Long> s;

    Iterator(QuotientFilter new_qf) {
        qf = new_qf;
        s = new ArrayDeque<Long>();
        //s = new ArrayDeque<Integer>();
        index = 0;
        bucket_index = -1;
        fingerprint = -1;
    }

    void clear() {
        s.clear();
        index = 0;
        bucket_index = -1;
        fingerprint = -1;
    }

    boolean next() {

        if (index == qf.get_logical_num_slots_plus_extensions()) {
            return false;
        }

        long slot = qf.get_slot(index);
        boolean occupied = (slot & 1) != 0;
        boolean continuation = (slot & 2) != 0;
        boolean shifted = (slot & 4) != 0;


        while (!occupied && !continuation && !shifted && index < qf.get_logical_num_slots_plus_extensions()) {
            index++;
            if (index == qf.get_logical_num_slots_plus_extensions()) {
                return false;
            }
            slot = qf.get_slot(index);
            occupied = (slot & 1) != 0;
            continuation = (slot & 2) != 0;
            shifted = (slot & 4) != 0;
        }

        if (occupied && !continuation && !shifted) {
            s.clear();
            s.add(index);
            bucket_index = index;
        }
        else if (occupied && continuation && shifted) {
            s.add(index);
        }
        else if (!occupied && !continuation && shifted) {
            s.remove();
            bucket_index = s.peek();
        }
        else if (!occupied && continuation && shifted) {
            // do nothing
        }
        else if (occupied && !continuation && shifted) {
            s.add(index);
            s.remove();
            bucket_index = s.peek();
        }
        fingerprint = slot >> 3;
        index++;
        return true;
    }

    void print() {
        System.out.println("original slot: " + index + "  " + bucket_index);
    }


}
