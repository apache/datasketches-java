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

/**
 * <p>The hash package contains a high-performing and extended Java implementation 
 * of Austin Appleby's 128-bit MurmurHash3 hash function originally coded in C. 
 * This core MurmurHash3.java class is used throughout all the sketch classes for consistentancy 
 * and as long as the user specifies the same seed will result in coordinated hash operations.
 * This package also contains an adaptor class that extends the basic class with more functions 
 * commonly associated with hashing.
 * </p>
 * 
 * @author Lee Rhodes
 */
package org.apache.datasketches.hash;
