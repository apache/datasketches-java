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
 * The hll package contains a high performance implementation of Phillipe
 * Flajolet's HLL sketch with significantly improved error behavior.
 *
 * <p>If the ONLY use case for sketching is counting uniques and merging, the
 * HLL sketch is the highest performing in terms of accuracy for space
 * consumed. For large counts, this HLL version will be 2 to 16 times
 * smaller for the same accuracy than the Theta Sketches.
 *
 * <p>HLL sketches do not retain any of the hash values of the associated unique identifiers,
 * so if there is any anticipation of a future need to leverage associations with these
 * retained hash values, Theta Sketches would be a better choice.</p>
 *
 * <p>HLL sketches cannot be intermixed or merged in any way with Theta Sketches.
 * </p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */

package org.apache.datasketches.hll;
