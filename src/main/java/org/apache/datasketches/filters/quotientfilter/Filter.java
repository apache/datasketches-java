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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


import org.apache.datasketches.memory.XxHash;


public abstract class Filter {

    //HashType hash_type;

    //abstract boolean rejuvenate(long key);
    //abstract boolean expand();
    //protected abstract boolean _delete(long large_hash);
    abstract protected boolean _insert(long large_hash);
    abstract protected boolean _search(long large_hash);


    //public boolean delete(long input) {
//        return _delete(get_hash(input));
//    }

//    public boolean delete(String input) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
//        //return _delete(HashFunctions.xxhash(input_buffer));
//        return _delete(XxHash.hashLong(input_buffer));
//    }

//    public boolean delete(byte[] input) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input);
//        return _delete(HashFunctions.xxhash(input_buffer));
//    }
//
    public boolean insert(long input) {
        //System.out.println("The ABC input is " + input);
        long hash = get_hash(input);
        //System.out.println("The ABC hash  is " + hash);
        return _insert(hash);
    }
//
//    public boolean insert(String input, boolean insert_only_if_no_match) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
//        return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
//    }
//
//    public boolean insert(byte[] input, boolean insert_only_if_no_match) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input);
//        return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
//    }
//
    public boolean search(long input) {
        return _search(get_hash(input));
    }
//
//    public boolean search(String input) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
//        return _search(HashFunctions.xxhash(input_buffer));
//    }
//
//    public boolean search(byte[] input) {
//        ByteBuffer input_buffer = ByteBuffer.wrap(input);
//        return _search(HashFunctions.xxhash(input_buffer));
//    }
//
    long get_hash(long input) {
//        long hash = 0;
//        if (hash_type == HashType.arbitrary) {
//            hash = HashFunctions.normal_hash((int)input);
//        }
//        else if (hash_type == HashType.xxh) {
//            hash = HashFunctions.xxhash(input);
//        }
//        else {
//            System.exit(1);
//        }
//        return hash;
        return XxHash.hashLong(input, 0L) ; // CD edit for datasketches hash function using same seed.
    }

    public long getSpaceUse() { return 0 ; }
//    public int get_bits_per_entry() { return 0 ; }
//
//    public abstract long get_num_entries(boolean include_all_internal_filters);
//
//    public double get_utilization() {
//        return 0;
//    }
//
//    public double measure_num_bits_per_entry() {
//        return 0;
//    }
//
//    static void print_int_in_binary(int num, int length) {
//        String str = "";
//        for (int i = 0; i < length; i++) {
//            int mask = (int)Math.pow(2, i);
//            int masked = num & mask;
//            str += masked > 0 ? "1" : "0";
//        }
//        System.out.println(str);
//    }
//
//    static void print_long_in_binary(long num, int length) {
//        String str = "";
//        for (int i = 0; i < length; i++) {
//            long mask = (long)Math.pow(2, i);
//            long masked = num & mask;
//            str += masked > 0 ? "1" : "0";
//        }
//        System.out.println(str);
//    }
//
//    String get_fingerprint_str(long fp, int length) {
//        String str = "";
//        for (int i = 0; i < length; i++) {
//            str += Bitmap.get_fingerprint_bit(i, fp) ? "1" : "0";
//        }
//        return str;
//    }
//
//    public void pretty_print() {
//
//    }


}

