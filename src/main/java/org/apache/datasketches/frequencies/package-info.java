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
 * <p>This package is dedicated to streaming algorithms that enable estimation of the 
 * frequency of occurence of items in a weighted multiset stream of items.  
 * If the frequency distribution of items is sufficiently skewed, these algorithms are very 
 * useful in identifying the "Heavy Hitters" that occured most frequently in the stream.  
 * The accuracy of the estimation of the frequency of an item has well understood error 
 * bounds that can be returned by the sketch.</p>
 * 
 * <p>These sketches are mergable and can be serialized and deserialized to/from a compact 
 * form.</p>
 * 
 * @author Lee Rhodes
 */
package org.apache.datasketches.frequencies;
