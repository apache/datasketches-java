/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
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
package com.yahoo.sketches.hash;
