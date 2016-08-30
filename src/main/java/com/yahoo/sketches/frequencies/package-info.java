/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root 
 * for terms.
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
package com.yahoo.sketches.frequencies;
