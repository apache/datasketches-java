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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import filters.FingerprintSacrifice;
import filters.BloomFilter;
import filters.ChainedInfiniFilter;
import filters.CuckooFilter;
import filters.Filter;
import filters.BasicInfiniFilter;
import filters.Chaining;
import filters.QuotientFilter;

public class Experiment1 extends ExperimentsBase {
	
	public static void main(String[] args) {
		parse_arguments(args);
		
		System.gc();
		{
			QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
			scalability_experiment(qf, 0, qf.get_max_entries_before_expansion() - 1, new baseline());
		}
		{
			QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
			scalability_experiment(qf, 0, qf.get_max_entries_before_expansion() - 1, new baseline());
		}
		
		System.gc();
		
		baseline bloom_res = new baseline();
		{
			int num_entries = (int) Math.pow(2, (num_entries_power + num_cycles) / 2);
			Filter bloom = new BloomFilter(num_entries, bits_per_entry);
			long starting_index = 0;
			for (int i = num_entries_power; i < (num_entries_power + num_cycles) / 2 + 1; i++ ) {
				long end_key = (int)(Math.pow(2, i) ); // 
				scalability_experiment(bloom, starting_index, end_key, bloom_res);
				starting_index = end_key;
				//int num_insertions = original_qf_res.metrics.get("num_entries").get(i);
				//orig.expand();
				//System.out.println("# entries: " + qf.num_existing_entries + " new capacity: " + Math.pow(2, qf.power_of_two_size));
				System.out.println("bloom filter " + i);
			}
		}
		System.out.println("finished bloom");
		System.gc();
		
		baseline cuckoo_res = new baseline();
		{
			Filter cuckoo = new CuckooFilter((num_entries_power + num_cycles) / 2, bits_per_entry);
			long starting_index = 0;
			for (int i = num_entries_power; i < (num_entries_power + num_cycles) / 2 + 1; i++ ) {
				long end_key = (int)(Math.pow(2, i) * 0.95); // 
				scalability_experiment(cuckoo, starting_index, end_key, cuckoo_res);
				starting_index = end_key;
				System.out.println("cuckoo filter " + i);
			}
		}
		System.out.println("finished cuckoo");
		
		System.gc();
		
		baseline original_qf_res = new baseline();
		{
			QuotientFilter orig = new QuotientFilter((num_entries_power + num_cycles) / 2, bits_per_entry);
			orig.set_expand_autonomously(false); 
			long starting_index = 0;
			for (int i = num_entries_power; i < (num_entries_power + num_cycles) / 2 + 1; i++ ) {
				long end_key = (int)(Math.pow(2, i) * 0.90); // 
				scalability_experiment(orig, starting_index, end_key, original_qf_res);
				starting_index = end_key;
				System.out.println("static quotient filter " + i);
			}
		}

		System.gc();
		System.out.println("finished quotient");

		baseline chained_IF_res = new baseline();
		{
			BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
			qf.set_expand_autonomously(true); 
			long starting_index = 0;
			long end_key = qf.get_max_entries_before_expansion() - 1;
			for (int i = num_entries_power; i <= num_cycles; i++ ) {
				scalability_experiment(qf, starting_index, end_key,  chained_IF_res);
				starting_index = end_key;
				end_key = qf.get_max_entries_before_expansion() * 2 - 1;
				System.out.println("infinifilter " + i);
			}
		}	
		System.out.println("finished infinifilter");
		System.gc();
		
		baseline bit_sacrifice_res = new baseline();
		{
			FingerprintSacrifice qf2 = new FingerprintSacrifice(num_entries_power, bits_per_entry);
			qf2.set_expand_autonomously(true); 
			long starting_index = 0;
			long end_key = qf2.get_max_entries_before_expansion() - 1;
			for (int i = num_entries_power; i <= num_cycles && qf2.get_fingerprint_length() > 0; i++ ) {
				scalability_experiment(qf2, starting_index, end_key, bit_sacrifice_res);
				starting_index = end_key;
				end_key = qf2.get_max_entries_before_expansion() * 2 - 1;
				System.out.println("bit sacrifice " + i);
			}
		}
		System.out.println("finished bit sacrifice");
		
		System.gc();

		baseline geometric_expansion_res = new baseline();
		{
			Chaining qf3 = new Chaining(num_entries_power, bits_per_entry);
			qf3.set_expand_autonomously(true);
			long starting_index = 0;
			long end_key = qf3.get_max_entries_before_expansion() - 1;
			for (int i = num_entries_power; i <= num_cycles - 1; i++ ) {
				scalability_experiment(qf3, starting_index, end_key, geometric_expansion_res);
				starting_index = end_key + 1;
				end_key = (long)(qf3.get_max_entries_before_expansion() * 2 + starting_index - 1);
				//System.out.println("thresh  " + qf3.max_entries_before_expansion);
				
				//(long)(Math.pow(2, power_of_two_size) * expansion_threshold)
				System.out.println("geometric chaining " + i);
			}
		}
		System.out.println("finished geometric chaining");

		int commas_before = 1;
		int commas_after = 5;
		original_qf_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		chained_IF_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		bit_sacrifice_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		geometric_expansion_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		bloom_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		cuckoo_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		
		System.out.println();

		commas_before = 1;
		commas_after = 5;
		original_qf_res.print("num_entries", "query_time", commas_before++, commas_after--);
		chained_IF_res.print("num_entries", "query_time", commas_before++, commas_after--);
		bit_sacrifice_res.print("num_entries", "query_time", commas_before++, commas_after--);
		geometric_expansion_res.print("num_entries", "query_time", commas_before++, commas_after--);
		bloom_res.print("num_entries", "query_time", commas_before++, commas_after--);
		cuckoo_res.print("num_entries", "query_time", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 5;
		original_qf_res.print("num_entries", "FPR", commas_before++, commas_after--);
		chained_IF_res.print("num_entries", "FPR", commas_before++, commas_after--);
		bit_sacrifice_res.print("num_entries", "FPR", commas_before++, commas_after--);
		geometric_expansion_res.print("num_entries", "FPR", commas_before++, commas_after--);
		bloom_res.print("num_entries", "FPR", commas_before++, commas_after--);
		cuckoo_res.print("num_entries", "FPR", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 5;
		original_qf_res.print("num_entries", "memory", commas_before++, commas_after--);
		chained_IF_res.print("num_entries", "memory", commas_before++, commas_after--);
		bit_sacrifice_res.print("num_entries", "memory", commas_before++, commas_after--);
		geometric_expansion_res.print("num_entries", "memory", commas_before++, commas_after--);
		bloom_res.print("num_entries", "memory", commas_before++, commas_after--);
		cuckoo_res.print("num_entries", "memory", commas_before++, commas_after--);

		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Calendar.getInstance().getTime());

		LocalDate ld = java.time.LocalDate.now();
		String dir_name = "Exp1_" + bits_per_entry + "_bytes_" +  timeStamp.toString();
	    Path path = Paths.get(dir_name);

		try {
			Files.createDirectories(path);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		String write_latency_file_name = dir_name + "/writes_speed.txt";
		String read_latency_file_name  = dir_name + "/read_speed.txt";
		String FPR_file_name  = dir_name + "/false_positive_rate.txt";
		String memory_file_name  = dir_name + "/memory.txt";
		String all_file_name  = dir_name + "/all.txt";
		
		create_file(write_latency_file_name);
		create_file(read_latency_file_name);
		create_file(FPR_file_name);
		create_file(memory_file_name);
		create_file(all_file_name);
		
	    try {
	        FileWriter insertion_writer = new FileWriter(write_latency_file_name);
	        
			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			chained_IF_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			bit_sacrifice_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			geometric_expansion_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			bloom_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			cuckoo_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);

			//System.out.println();
			insertion_writer.close();
	        FileWriter reads_writer = new FileWriter(read_latency_file_name);

			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			chained_IF_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			bit_sacrifice_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			geometric_expansion_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			bloom_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			cuckoo_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);

			//System.out.println();
			reads_writer.close();
	        FileWriter FPR_writer = new FileWriter(FPR_file_name);

			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			chained_IF_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			bit_sacrifice_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			geometric_expansion_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			bloom_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			cuckoo_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);

			FPR_writer.close();
			FileWriter mem_writer = new FileWriter(memory_file_name);
			
			//System.out.println();

			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			chained_IF_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			bit_sacrifice_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			geometric_expansion_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			bloom_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			cuckoo_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
	        
			mem_writer.close();
						
	    	FileWriter all_writer = new FileWriter(all_file_name);

			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			chained_IF_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			bit_sacrifice_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			geometric_expansion_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			bloom_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			cuckoo_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			chained_IF_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			bit_sacrifice_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			geometric_expansion_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			bloom_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			cuckoo_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			chained_IF_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			bit_sacrifice_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			geometric_expansion_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			bloom_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			cuckoo_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			original_qf_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			chained_IF_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			bit_sacrifice_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			geometric_expansion_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			bloom_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			cuckoo_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
	    	
			all_writer.close();
	    	
	        System.out.println("Successfully wrote to the files.");
	      } catch (IOException e) {
	        System.out.println("An error occurred.");
	        e.printStackTrace();
	      }

	}
	
	
	static public void scalability_experiment(Filter qf, long initial_key, long end_key, baseline results) {

		int num_qeuries = 1000000;
		int query_index = Integer.MAX_VALUE;
		int num_false_positives = 0;

		//int num_entries_to_insert = (int) (Math.pow(2, power) * (qf.expansion_threshold )) - qf.num_existing_entries;
		//final int initial_num_entries = qf.get_num_entries(true);
		
		long initial_num_entries = initial_key;
		long insertion_index = initial_key;
		long start_insertions = System.nanoTime();

		
		//System.out.println("inserting: " + num_entries_to_insert + " to capacity " + Math.pow(2, qf.power_of_two_size));

		boolean successful_insert = false;
		do {
			successful_insert = qf.insert(insertion_index, false);
			insertion_index++;
		} while (insertion_index < end_key && successful_insert);
		
		if (!successful_insert) {
			System.out.println("an insertion failed");
			System.exit(1);
		}
		
		//qf.pretty_print();

		long end_insertions = System.nanoTime();
		long start_queries = System.nanoTime();

		for (int i = 0; i < num_qeuries || num_false_positives < 10; i++) {
			boolean found = qf.search(query_index--);
			if (found) {
				num_false_positives++;
			}
			if (i > num_qeuries * 10) {
				break;
			}
		}
		num_qeuries = Integer.MAX_VALUE - query_index;

		long end_queries = System.nanoTime();
		double avg_insertions = (end_insertions - start_insertions) / (double)(insertion_index - initial_num_entries);
		double avg_queries = (end_queries - start_queries) / (double)num_qeuries;
		double FPR = num_false_positives / (double)num_qeuries;
		//int num_slots = (1 << qf.power_of_two_size) - 1;
		//double utilization = qf.get_utilization();

		//double num_entries = qf.get_num_entries(true);
		double num_entries = qf.get_num_occupied_slots(true);

		results.metrics.get("num_entries").add(num_entries);
		results.metrics.get("insertion_time").add(avg_insertions);
		results.metrics.get("query_time").add(avg_queries);
		results.metrics.get("FPR").add(FPR);
		double bits_per_entry = qf.measure_num_bits_per_entry();
		//System.out.println(bits_per_entry);
		results.metrics.get("memory").add(bits_per_entry);

	}
	
}
