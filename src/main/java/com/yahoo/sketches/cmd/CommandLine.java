/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cmd;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.TAB;
import static java.lang.Math.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;
import com.yahoo.sketches.quantiles.QuantilesSketchBuilder;
import com.yahoo.sketches.quantiles.QuantilesSketch;
import com.yahoo.sketches.frequencies.FrequentItemsSketch;
import com.yahoo.sketches.frequencies.FrequentLongsSketch.Row;
import com.yahoo.sketches.frequencies.ErrorType;

/**
 * Command line access to the basic sketch functions. 
 */
public class CommandLine {
  private static final String BOLD = "\033[1m";
  private static final String OFF = "\033[0m";
  
  public static void main(String[] args) {
    if (args.length == 0) help();
    else parseType(args);
  }
  
   static void parseType(String[] args) {
    String token1 = args[0].toLowerCase();
    switch (token1) {
      case "uniq": parseUniq(args); break;
      case "rank": parseRank(args); break;
      case "hist": parseHist(args); break;
      case "loghist": parseLogHist(args); break;
      case "freq": parseFreq(args); break;
      case "help": help(); break;
      default: {
        printlnErr("Unrecognized TYPE: "+token1);
        help();
      }
    }
  }
  
  private static int parseArgsCase(String[] args) { //we already know type is valid
    int len = args.length;
    int ret = 0;
    switch (len) {
      case 1: ret = 1; break; //only type, assume default k, System.in
      case 2: {
        String token2 = args[1]; //2nd arg could be help, k (numeric) or a fileName
        if (token2.equalsIgnoreCase("help")) { ret = 2; break; } //help
        if (!isNumeric(token2)) { ret = 3; break; } //2nd arg not numeric, must be a filename
        ret = 4; //2nd arg must be numeric, assume System.in
        break;
      }
      default: { //3 or more
        String token2 = args[1]; //2nd arg could be help, k (numeric) or a fileName
        if (token2.equalsIgnoreCase("help")) { ret = 2; break; } //help
        if (!isNumeric(token2)) { ret = 3; break; } //2nd arg not numeric, must be a filename
        //2nd arg is numeric, 3rd arg must be filename
        ret = 5; 
        break;
      }
    }
    return ret;
  }
  
  private static void parseUniq(String[] args) {
    UpdateSketchBuilder bldr = Sketches.updateSketchBuilder();
    UpdateSketch sketch;
    int argsCase = parseArgsCase(args);
    switch (argsCase) {
      case 1:
        doUniq(getBR(null), bldr.build()); break; //[default k], [System.in]
      case 2:
        uniqHelp(); break; //help
      case 3: //2nd arg not numeric, must be a filename
        doUniq(getBR(args[1]), bldr.build()); break; //[default k], file
      case 4: //2nd arg is numeric, no filename
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doUniq(getBR(null), sketch); //user k, [System.in]
        break;
      case 5: //3 valid args
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doUniq(getBR(args[2]), sketch);
    }
  }
  
  private static void doUniq(BufferedReader br, UpdateSketch sketch) {
    String itemStr = "";
    try {
      while ((itemStr = br.readLine()) != null) {
        sketch.update(itemStr);
      }
    } catch (IOException e) {
      printlnErr("Read Error: Item: "+itemStr +", "+br.toString());
      System.exit(1);
    }
    println(sketch.toString());
  }
  
  private static void parseRank(String[] args) {
    QuantilesSketchBuilder bldr = new QuantilesSketchBuilder();
    QuantilesSketch sketch;
    int argsCase = parseArgsCase(args);
    switch (argsCase) {
      case 1:
        doRank(getBR(null), bldr.build()); break; //[default k], [System.in]
      case 2:
        rankHelp(); break; //help
      case 3: //2nd arg not numeric, must be a filename
        doRank(getBR(args[1]), bldr.build()); break; //[default k], file
      case 4: //2nd arg is numeric, no filename
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doRank(getBR(null), sketch); //user k, [System.in]
        break;
      case 5: //3 valid args
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doRank(getBR(args[2]), sketch);
    }
  }
  
  private static void doRank(BufferedReader br, QuantilesSketch sketch) {
    String itemStr = "";
    try {
      while ((itemStr = br.readLine()) != null) {
        double item = Double.parseDouble(itemStr);
        sketch.update(item);
      }
    } catch (IOException | NumberFormatException e ) {
      printlnErr("Read Error: Item: "+itemStr +", "+br.toString());
      System.exit(1);
    }
    int ranks = 101;
    double[] valArr = sketch.getQuantiles(ranks);
    println("Rank"+TAB+ "Value");
    for (int i=0; i<ranks; i++) {
      String r = String.format("%.2f",(double)i/ranks);
      println(r + TAB + valArr[i]);
    }
  }
  
  private static void parseHist(String[] args) {
    QuantilesSketchBuilder bldr = new QuantilesSketchBuilder();
    QuantilesSketch sketch;
    int argsCase = parseArgsCase(args);
    switch (argsCase) {
      case 1:
        doHist(getBR(null), bldr.build()); break; //[default k], [System.in]
      case 2:
        histHelp(); break; //help
      case 3: //2nd arg not numeric, must be a filename
        doHist(getBR(args[1]), bldr.build()); break; //[default k], file
      case 4: //2nd arg is numeric, no filename
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doHist(getBR(null), sketch); //user k, [System.in]
        break;
      case 5: //3 valid args
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doHist(getBR(args[2]), sketch);
    }
  }
  
  private static void doHist(BufferedReader br, QuantilesSketch sketch) {
    String itemStr = "";
    try {
      while ((itemStr = br.readLine()) != null) {
        double item = Double.parseDouble(itemStr);
        sketch.update(item);
      }
    } catch (IOException | NumberFormatException e ) {
      printlnErr("Read Error: Item: "+itemStr +", "+br.toString());
      System.exit(1);
    }
    int splitPoints = 30;
    long n = sketch.getN();
    double[] splitsArr = getEvenSplits(sketch, splitPoints);
    double[] histArr = sketch.getPMF(splitsArr);
    println("Value"+TAB+ "Freq");
    //int histArrLen = histArr.length; //one larger than splitsArr
    double min = sketch.getMinValue();
    String splitVal = String.format("%,f", min);
    String freqVal = String.format("%,d", (long)(histArr[0] * n));
    println(splitVal+TAB+freqVal);
    for (int i=0; i<splitsArr.length; i++) {
      splitVal = String.format("%,f", splitsArr[i] * n);
      freqVal = String.format("%,d", (long)(histArr[i+1] * n));
      println(splitVal+TAB+freqVal);
    }
  }
  
  private static void parseLogHist(String[] args) {
    QuantilesSketchBuilder bldr = new QuantilesSketchBuilder();
    QuantilesSketch sketch;
    int argsCase = parseArgsCase(args);
    switch (argsCase) {
      case 1:
        doLogHist(getBR(null), bldr.build()); break; //[default k], [System.in]
      case 2:
        logHistHelp(); break; //help
      case 3: //2nd arg not numeric, must be a filename
        doLogHist(getBR(args[1]), bldr.build()); break; //[default k], file
      case 4: //2nd arg is numeric, no filename
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doLogHist(getBR(null), sketch); //user k, [System.in]
        break;
      case 5: //3 valid args
        sketch = bldr.build(Integer.parseInt(args[1])); //args[1] is numeric = k
        doLogHist(getBR(args[2]), sketch);
    }
  }
  
  private static void doLogHist(BufferedReader br, QuantilesSketch sketch) {
    String itemStr = "";
    try {
      while ((itemStr = br.readLine()) != null) {
        double item = Double.parseDouble(itemStr);
        if (Double.isNaN(item) || (item <= 0.0)) continue;
        sketch.update(item);
      }
    } catch (IOException | NumberFormatException e ) {
      printlnErr("Read Error: Item: "+itemStr +", "+br.toString());
      System.exit(1);
    }
    int splitPoints = 30;
    long n = sketch.getN();
    double[] splitsArr = getLogSplits(sketch, splitPoints);
    double[] histArr = sketch.getPMF(splitsArr);
    println("Value"+TAB+ "Freq");
    //int histArrLen = histArr.length; //one larger than splitsArr
    double min = sketch.getMinValue();
    String splitVal = String.format("%,f", min);
    String freqVal = String.format("%,d", (long)(histArr[0] * n));
    println(splitVal+TAB+freqVal);
    for (int i=0; i<splitsArr.length; i++) {
      splitVal = String.format("%,f", splitsArr[i] * n);
      freqVal = String.format("%,d", (long)(histArr[i+1] * n));
      println(splitVal+TAB+freqVal);
    }
  }
  
  private static void parseFreq(String[] args) {
    FrequentItemsSketch<String> sketch;
    int defaultSize = 1 << 17; //128K
    int argsCase = parseArgsCase(args);
    switch (argsCase) {
      case 1:
        sketch = new FrequentItemsSketch<String>(defaultSize);
        doFreq(getBR(null), sketch); break; //[default k], [System.in]
      case 2:
        freqHelp(); break; //help
      case 3: //2nd arg not numeric, must be a filename
        sketch = new FrequentItemsSketch<String>(defaultSize);
        doFreq(getBR(args[1]), sketch); break; //[default k], file
      case 4: //2nd arg is numeric, no filename
        sketch = new FrequentItemsSketch<String>(Integer.parseInt(args[1])); //args[1] is numeric = k
        doFreq(getBR(null), sketch); //user k, [System.in]
        break;
      case 5: //3 valid args
        sketch = new FrequentItemsSketch<String>(Integer.parseInt(args[1])); //args[1] is numeric = k
        doFreq(getBR(args[2]), sketch);
    }
  }
  
  private static void doFreq(BufferedReader br, FrequentItemsSketch<String> sketch) {
    String itemStr = "";
    try {
      while ((itemStr = br.readLine()) != null) {
        sketch.update(itemStr);
      }
    } catch (IOException e ) {
      printlnErr("Read Error: Item: "+itemStr +", "+br.toString());
      System.exit(1);
    }
    //NFP is a subset of NFN
    FrequentItemsSketch<String>.Row[] rowArr = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    int len = rowArr.length;
    println("Qualifying Rows: "+len);
    println(Row.getRowHeader());
    for (int i=0; i<len; i++) {
      println((i+1) + rowArr[i].toString());
    }
  }
  
  private static double[] getEvenSplits(QuantilesSketch sketch, int splitPoints) {
    double min = sketch.getMinValue();
    double max = sketch.getMaxValue();
    return getSplits(min, max, splitPoints);
  }
  
  private static double[] getLogSplits(QuantilesSketch sketch, int splitPoints) {
    double min = sketch.getMinValue();
    double max = sketch.getMaxValue();
    double logMin = log10(min);
    double logMax = log10(max);
    double[] logArr = getSplits(logMin, logMax, splitPoints);
    double[] expArr = new double[logArr.length];
    for (int i= 0; i<logArr.length; i++) {
      expArr[i] = pow(10.0, logArr[i]);
    }
    return expArr;
  }
  
  private static double[] getSplits(double min, double max, int splitPoints) {
    double range = max - min;
    double delta = range/(splitPoints + 1);
    double[] splits = new double[splitPoints];
    for (int i = 0; i < splitPoints; i++) {
      splits[i] = delta * (i+1);
    }
    return splits;
  }
  
  private static boolean isNumeric(String token) {
    for (char c : token.toCharArray()) {
        if (!Character.isDigit(c)) return false;
    }
    return true;
  }
  
  private static BufferedReader getBR(String token) {
    BufferedReader br = null;
    try {
      if ((token == null) || (token.length() == 0)) {
        br = new BufferedReader(new InputStreamReader(System.in));
      } else {
        br = new BufferedReader(new InputStreamReader(new FileInputStream(token)));
      }
    } catch (FileNotFoundException e) {
      printlnErr("File Not Found: "+token);
      System.exit(1);
    }
    return br;
  }
  
  private static void uniqHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"UNIQ SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch uniq help").append(LS);
    sb.append("    sketch uniq [SIZE] [FILE]");
    println(sb.toString());
  }
  
  private static void rankHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"RANK SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch rank help").append(LS);
    sb.append("    sketch rank [SIZE] [FILE]");
    println(sb.toString());
  }
  
  private static void histHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"HIST SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch hist help").append(LS);
    sb.append("    sketch hist [SIZE] [FILE]");
    println(sb.toString());
  }
  
  private static void logHistHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"LOGHIST SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch loghist help").append(LS);
    sb.append("    sketch loghist [SIZE] [FILE]");
    println(sb.toString());
  }
  
  private static void freqHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"FREQ SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch freq help").append(LS);
    sb.append("    sketch freq [SIZE] [FILE]");
    println(sb.toString());
  }
  
  static void help() {
    StringBuilder sb = new StringBuilder();
    sb.append(BOLD+"NAME"+OFF).append(LS);
    sb.append("    sketch - sketch Uniques, Quantiles, Histograms, or Frequent Items.").append(LS);
    sb.append(BOLD+"SYNOPSIS"+OFF).append(LS);
    sb.append("    sketch (this help)").append(LS);
    sb.append("    sketch TYPE help").append(LS);
    sb.append("    sketch TYPE [SIZE] [FILE]").append(LS);
    sb.append(BOLD+"DESCRIPTION"+OFF).append(LS);
    sb.append("    Write a sketch(TYPE, SIZE) of FILE to standard output.").append(LS);
    sb.append("    TYPE is required.").append(LS);
    sb.append("    If SIZE is omitted, internal defaults are used.").append(LS);
    sb.append("    If FILE is omitted, Standard In is assumed.").append(LS);
    sb.append(BOLD+"TYPE DESCRIPTION"+OFF).append(LS);
    sb.append("    sketch uniq    : Sketch the unique string items of a stream.").append(LS);
    sb.append("    sketch rank    : Sketch the rank-value distribution of a numeric value stream.").
       append(LS);
    sb.append("    sketch hist    : "+
       "Sketch the linear-axis value-frequency distribution of numeric value stream.").append(LS);
    sb.append("    sketch loghist : "+
        "Sketch the log-axis value-frequency distribution of numeric value stream.").append(LS);
    sb.append("    sketch freq    : Sketch the Heavy Hitters of a string item stream.");
    println(sb.toString());
    uniqHelp();
    rankHelp();
    histHelp();
    logHistHelp();
    freqHelp();
  }
  
  private static void printlnErr(String s) { System.err.println(s); }
  
  private static void println(String s) { System.out. println(s); }
}
