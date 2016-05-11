/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.demo;

import com.yahoo.sketches.demo.DemoImpl;

/**
 * <p>This demo computes a stream of values and feeds them first to
 * an exact sort-based method of computing the number of unique values
 * in the stream and then feeds a similar stream to two different types of
 * sketches from the library.
 * 
 * <p>This demo becomes most significant in the case where the number of uniques in the 
 * stream exceeds what the computer can hold in memory.
 * 
 * <p>This demo utilizes the Unix/Linux/OS-X sort and wc commands for the brute force compuation.
 * So this needs to be run on a linux or mac machine. A windows machine with a suitable unix
 * library installed might also work, but it has not been tested.
 * 
 * <p>To run this demo from the command line:</p>
 * <ul><li>Clone the lastest snapshot from https://github.com/DataSketches/sketches-core.</li>
 * <li>Change to the directory where you did the clone</li>
 * <li>Do a Maven Install: "mvn install"</li>
 * <li>In the following commands replace X.Y.Z with the actual jar version from the target 
 * directory:<br>
 * javac -cp target/sketches-core-X.Y.Z.jar  src/test/java/com/yahoo/sketches/demo/*.java<br>
 * java -cp target/sketches-core-X.Y.Z.jar:src/test/java com.yahoo.sketches.demo.ExactVsSketchDemo 
 * 1E6</li>
 * <li>The demo will output results to the console.  You can change the 1E6 (1 million) to even 
 * larger values (e.g., 1E8) but be patient.  The exact sort can take a long, long time!</li>
 * </ul>
 * 
 */
public class ExactVsSketchDemo {
  
  /**
   * Runs the demo.
   * 
   * @param args 
   * <ul><li>arg[0]: (Optional) The stream length and can be expressed as a positive double value.
   * The default is 1E6.</li>
   * <li>arg[1] (Optional) The fraction of the stream length that will be unique, the remainder 
   * will be duplicates. The default is 1.0. Note that if this argument is less than 1.0, 
   * the actual number of exact uniques is statistically determined for each trial and then 
   * separately counted. That is, the number of exact uniques for the "sort" trial 
   * will be different from the exact uniques for each of the sketch trial. </li>
   * </ul>
   */
  public static void main(String[] args) {
    int argsLen = args.length;
    long streamLen = (long)1E6;   //The default stream length
    double uFrac = 1.0;          //The default fraction that are unique
    if (argsLen == 1) {
      streamLen = (long)(Double.parseDouble(args[0]));
    } else if (argsLen > 1) {
      streamLen = (long)(Double.parseDouble(args[0]));
      uFrac = Double.parseDouble(args[1]);
    }
    
    DemoImpl demo = new DemoImpl(streamLen, uFrac);
    
    demo.runDemo();
  }
  
}
