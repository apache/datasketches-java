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

public abstract class Bitmap {

    public abstract long size();
    public abstract void set(long bit_index, boolean value);
    public abstract void setFromTo(long from, long to, long value);
    public abstract boolean get(long bit_index);
    public abstract long getFromTo(long from, long to);

    public static boolean get_fingerprint_bit(long index, long fingerprint) {
        long mask = 1 << index;
        long and = fingerprint & mask;
        return and != 0;
    }
}
