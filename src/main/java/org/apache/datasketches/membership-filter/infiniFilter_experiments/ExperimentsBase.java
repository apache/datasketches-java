/*
 * Copyright 2024 Niv Dayan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package infiniFilter_experiments;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import filters.FingerprintSacrifice;
import filters.BloomFilter;
import filters.ChainedInfiniFilter;
import filters.CuckooFilter;
import filters.DuplicatingChainedInfiniFilter;
import filters.Filter;
import filters.FingerprintGrowthStrategy;
import filters.BasicInfiniFilter;
import filters.Chaining;
import filters.QuotientFilter;
import infiniFilter_experiments.ExperimentsBase.baseline;

public class ExperimentsBase {

	static int bits_per_entry = 16;
	static int num_entries_power = 12;	
	static int num_cycles = 27; // went up to 31
	
	static void parse_arguments(String[] args) {
		if (args != null) {
	        ArrayList<Integer> argsArr = new ArrayList<Integer>(args.length); // could be 9
	        for (String val : args) {
	            int temp = Integer.parseInt(val);
	            argsArr.add(temp);
	        }   
	        if (argsArr.size() > 0 && argsArr.get(0) > 0) {
	        	bits_per_entry = argsArr.get(0);
	        }
	        if (argsArr.size() > 1 && argsArr.get(1) > 0) {
	        	num_entries_power = argsArr.get(1);
	        }
	        if (argsArr.size() > 2 && argsArr.get(2) > 0) {
	        	num_cycles = argsArr.get(2);
	        }
		}
	}
	
	public static File create_file(String file_name) {
		try {
			File f = new File( file_name  );
			if (f.createNewFile()) {
				System.out.println("Results File created: " + f.getName());
			} else {
				System.out.println("Results file will be overwritten.");
			}
			return f;
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		return null;
	}
	
	public static class baseline implements Cloneable {
		public Map<String, ArrayList<Double>> metrics;
		public baseline() {
			init();
		}
		
		@Override
		public Object clone() {
			baseline f = null;
			try {
				f = (baseline) super.clone();
				f.init();
				f.metrics.get("num_entries").addAll(metrics.get("num_entries"));
				f.metrics.get("num_bits").addAll(metrics.get("num_entries"));

				f.metrics.get("insertion_time").addAll(metrics.get("insertion_time"));
				f.metrics.get("query_time").addAll(metrics.get("query_time"));
				f.metrics.get("delete_time").addAll(metrics.get("delete_time"));
				f.metrics.get("FPR").addAll(metrics.get("FPR"));
				f.metrics.get("memory").addAll(metrics.get("memory"));
				f.metrics.get("expansion").addAll(metrics.get("expansion"));
				f.metrics.get("background_deletes").addAll(metrics.get("background_deletes"));
				f.metrics.get("avg_run_length").addAll(metrics.get("avg_run_length"));
				f.metrics.get("avg_cluster_length").addAll(metrics.get("avg_cluster_length"));
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return f;
		}
		
		void init() {
			metrics = new TreeMap<String, ArrayList<Double>>();
			metrics.put("num_entries", new ArrayList<Double>());
			metrics.put("num_bits", new ArrayList<Double>());
			metrics.put("insertion_time", new ArrayList<Double>());
			metrics.put("query_time", new ArrayList<Double>());
			metrics.put("delete_time", new ArrayList<Double>());
			metrics.put("FPR", new ArrayList<Double>());
			metrics.put("memory", new ArrayList<Double>());
			metrics.put("expansion", new ArrayList<Double>());
			metrics.put("background_deletes", new ArrayList<Double>());
			metrics.put("avg_run_length", new ArrayList<Double>());
			metrics.put("avg_cluster_length", new ArrayList<Double>());
		}

		void print(String x_axis_name, String y_axis_name, int commas, int after_commas) {
			ArrayList<Double> x_axis = metrics.get(x_axis_name);
			ArrayList<Double> y_axis = metrics.get(y_axis_name);
			for (int i = 0; i < y_axis.size(); i++) {
				System.out.print(x_axis.get(i));	
				for (int c = 0; c < commas; c++) {
					System.out.print(",");
				}
				System.out.print(y_axis.get(i));	
				for (int c = 0; c < after_commas; c++) {
					System.out.print(",");
				}
				System.out.println();	
			}
		}
		
		void print(String y_axis_name, int commas, int after_commas) {
			ArrayList<Double> y_axis = metrics.get(y_axis_name);
			for (int i = 0; i < y_axis.size(); i++) {
				System.out.print(i + 1);	
				for (int c = 0; c < commas; c++) {
					System.out.print(",");
				}
				System.out.print(y_axis.get(i));	
				for (int c = 0; c < after_commas; c++) {
					System.out.print(",");
				}
				System.out.println();	
			}
		}
		
		void print_to_file(String x_axis_name, String y_axis_name, int commas, int after_commas, FileWriter file) throws IOException {
			ArrayList<Double> x_axis = metrics.get(x_axis_name);
			ArrayList<Double> y_axis = metrics.get(y_axis_name);
			for (int i = 0; i < x_axis.size(); i++) {
				file.write(x_axis.get(i).toString());
				for (int c = 0; c < commas; c++) {
					file.write(",");
				}
				file.write(y_axis.get(i).toString());
				for (int c = 0; c < after_commas; c++) {
					file.write(",");
				}
				file.write("\n");
			}
		}

		double average(String y_axis_name) {
			ArrayList<Double> y_axis = metrics.get(y_axis_name);
			double avg = 0;
			for (int i = 0; i < y_axis.size(); i++) {
				avg += y_axis.get(i);
			}
			return avg / y_axis.size();
		}
	}






	static public void experiment_false_positives() {
		int bits_per_entry = 10;
		int num_entries_power = 5;
		int seed = 5; 
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fingerprint_size = bits_per_entry - 3;
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
		HashSet<Integer> added = new HashSet<Integer>();
		Random rand = new Random(seed);
		double load_factor = 0.9;
		int num_queries = 20000;
		int num_false_positives = 0;

		for (int i = 0; i < qf.get_physcial_num_slots() * load_factor; i++) {
			int rand_num = rand.nextInt();
			boolean success = qf.insert(rand_num, false);
			if (success) {
				added.add(rand_num);
			}
			else {
				System.out.println("insertion failed");
			}

		}

		for (int i = 0; i < num_queries; i++) {
			int rand_num = rand.nextInt();
			if (!added.contains(rand_num)) {
				boolean found = qf.search(i);
				if (found) {
					//System.out.println("we seem to have a false positive");
					num_false_positives++;
				}
			}
		}
		double FPR = num_false_positives / (double)num_queries;
		System.out.println("measured FPR:\t" + FPR);
		double expected_FPR = Math.pow(2, - fingerprint_size);
		System.out.println("single fingerprint model:\t" + expected_FPR);
		double expected_FPR_bender = 1 - Math.exp(- load_factor / Math.pow(2, fingerprint_size));
		System.out.println("bender model:\t" + expected_FPR_bender);
	}

	static public void experiment_insertion_speed() {
		int bits_per_entry = 3;
		int num_entries_power = 12;
		int seed = 5; 
		//int num_entries = (int)Math.pow(2, num_entries_power);
		int fingerprint_size = bits_per_entry - 3;
		QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
		Random rand = new Random(seed);
		double load_factor = 0.1;
		int num_queries = 20000;
		int num_false_positives = 0;
		double num_insertions = qf.get_physcial_num_slots() * load_factor; 
		long start = System.nanoTime();
		long time_sum = 0;
		long time_sum_square = 0;
		for (int i = 0; i < num_insertions; i++) {
			long start1 = System.nanoTime();
			int rand_num = rand.nextInt();
			boolean success = qf.insert(rand_num, false);

			long end1 = System.nanoTime(); 
			//if (i > 5) {
			long time_diff = (end1 - start1);
			time_sum += time_diff;
			time_sum_square += time_diff * time_diff; 
			//}
			//System.out.println("execution time :\t" + ( end1 - start1) / (1000.0) + " mic s");	
		}
		long end = System.nanoTime(); 
		System.out.println("execution time :\t" + ( end - start) / (1000.0 * 1000.0) + " ms");
		System.out.println("execution time per entry :\t" + ( end - start) / (num_insertions * 1000.0) + " mic sec");

		double avg_nano = time_sum / num_insertions;
		System.out.println("avg :\t" + (avg_nano / 1000.0));

		double avg_normalized = avg_nano / 1000.0;
		double time_sum_square_normalized = time_sum_square / 1000000.0 ;
		double variance = (time_sum_square_normalized - avg_normalized * avg_normalized * num_insertions) / num_insertions;
		double std = Math.sqrt(variance);
		System.out.println("std :\t" + std);
	}
	
	
}

