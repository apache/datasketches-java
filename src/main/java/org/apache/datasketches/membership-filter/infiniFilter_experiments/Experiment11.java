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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Queue;

import filters.BasicInfiniFilter;
import filters.ChainedInfiniFilter;
import filters.DuplicatingChainedInfiniFilter;
import filters.Filter;
import filters.FingerprintGrowthStrategy.FalsePositiveRateExpansion;
import infiniFilter_experiments.ExperimentsBase.baseline;
import filters.FingerprintSacrifice;
import filters.QuotientFilter;

public class Experiment11 extends ExperimentsBase {
	
	public static boolean condition_to_start_deleting(ChainedInfiniFilter qf) {
		//return !qf.is_chain_empty();
		return qf.num_expansions > 10;
	}
	
	/*public static boolean condition_to_start_deleting(ChainedInfiniFilter qf) {
		return qf.get_secondary() != null;
	}*/
	
	static void warmup(DuplicatingChainedInfiniFilter qf, int cycles) {
		cycles = Math.max(cycles, 25);
		baseline warmup_res = new baseline();
		{
			long starting_index = 0;
			Queue<Long> end_keys = new LinkedList<Long>();
			Queue<Long> start_keys = new LinkedList<Long>();
			for (int i = num_entries_power; i <= cycles; i++ ) {
				start_keys.add(starting_index);
				long end_index = scalability_experiment(qf, starting_index,  warmup_res);
				end_keys.add(end_index);
				starting_index = end_index;
				double percentage_full = qf.get_utilization();
				qf.get_num_physical_entries();
				System.out.println("warmup " + i + "\t" + qf.get_num_logical_entries() + "\t" + qf.get_num_physical_entries() + "\t" + qf.get_max_entries_before_expansion() + "\t" + percentage_full);
			}
			
		}
		warmup_res.init();
		System.out.println("finished warmup");
	}

	
	public static void main(String[] args) {
		parse_arguments(args);
		
		ExperimentsBase.bits_per_entry = 12;
		num_entries_power = 8;	
		num_cycles = 27;
		boolean perform_deletes = true;
		int num_generations_to_delete = 1000;
		boolean do_warmup = true;
		boolean do_fingerprint_sacrifice = true;
		boolean do_aleph_polynomial = true;
		boolean do_aleph_predictive = true;
		boolean do_infinifilter = true;
		
		if (do_warmup) {
			DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
			qf.set_expand_autonomously(true); 
			qf.set_fpr_style(FalsePositiveRateExpansion.UNIFORM);			
			warmup(qf, num_cycles - 4);
		}
		
		baseline uniform = new baseline();
		{
			DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
			qf.set_expand_autonomously(true); 
			qf.set_fpr_style(FalsePositiveRateExpansion.UNIFORM);	
			long starting_index = 0;
			Queue<Long> end_keys = new LinkedList<Long>();
			Queue<Long> start_keys = new LinkedList<Long>();
			int local_num_generations_to_delete = num_generations_to_delete;
			for (int i = num_entries_power; i <= num_cycles && do_aleph_polynomial; i++ ) {
				start_keys.add(starting_index);
				long end_index = scalability_experiment(qf, starting_index,  uniform);
				end_keys.add(end_index);
				if ( condition_to_start_deleting(qf) && perform_deletes && local_num_generations_to_delete > 0 ) {
					Long del_start_key = start_keys.remove();
					Long del_end_key = end_keys.remove();
					delete_oldest(qf, del_start_key, del_end_key, uniform);
					local_num_generations_to_delete--;
				}
				starting_index = end_index;
				double percentage_full = qf.get_utilization();
				qf.get_num_physical_entries();
				System.out.println("aleph fixed " + i + "\t" + qf.get_num_logical_entries() + "\t" + qf.get_num_physical_entries() + "\t" + qf.get_max_entries_before_expansion() + "\t" + percentage_full);

			}
			qf.print_filter_summary();
			qf.print_age_histogram();
		}
		//System.out.println("finished aleph predictive");
			
		System.gc();

		if (do_warmup) {
			DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
			qf.set_expand_autonomously(true); 
			qf.set_fpr_style(FalsePositiveRateExpansion.POLYNOMIAL);		
			warmup(qf, num_cycles - 4);
		}
		
		baseline polynomial = new baseline();
		{
			DuplicatingChainedInfiniFilter qf = new DuplicatingChainedInfiniFilter(num_entries_power, bits_per_entry, true, -1);
			qf.set_expand_autonomously(true); 
			qf.set_fpr_style(FalsePositiveRateExpansion.POLYNOMIAL);
			long starting_index = 0;
			Queue<Long> end_keys = new LinkedList<Long>();
			Queue<Long> start_keys = new LinkedList<Long>();
			int local_num_generations_to_delete = num_generations_to_delete;
			for (int i = num_entries_power; i <= num_cycles && do_aleph_polynomial; i++ ) {
				start_keys.add(starting_index);
				long end_index = scalability_experiment(qf, starting_index,  polynomial);
				end_keys.add(end_index);
				if ( condition_to_start_deleting(qf) && perform_deletes && local_num_generations_to_delete > 0 ) {
					Long del_start_key = start_keys.remove();
					Long del_end_key = end_keys.remove();
					delete_oldest(qf, del_start_key, del_end_key, polynomial);
					local_num_generations_to_delete--;
				}
				starting_index = end_index;
				double percentage_full = qf.get_utilization();
				qf.get_num_physical_entries();
				System.out.println("aleph poly " + i + "\t" + qf.get_num_logical_entries() + "\t" + qf.get_num_physical_entries() + "\t" + qf.get_max_entries_before_expansion() + "\t" + percentage_full);

			}
			qf.print_filter_summary();
			qf.print_age_histogram();
		}
		
		System.gc();

		int commas_before = 1;
		int commas_after = 5;
		//aleph_predictive.print("num_entries", "insertion_time", commas_before++, commas_after--);
		//polynomial.print("num_entries", "insertion_time", commas_before++, commas_after--);
		//fingerprint_sacrifice_res.print("num_entries", "insertion_time", commas_before++, commas_after--);
		//uniform.print("num_entries", "insertion_time", commas_before++, commas_after--);

		
		System.out.println();

		commas_before = 1;
		commas_after = 5;
		//aleph_predictive.print("num_entries", "query_time", commas_before++, commas_after--);
		//polynomial.print("num_entries", "query_time", commas_before++, commas_after--);
		//fingerprint_sacrifice_res.print("num_entries", "query_time", commas_before++, commas_after--);
		//uniform.print("num_entries", "query_time", commas_before++, commas_after--);

		
		System.out.println();

		commas_before = 1;
		commas_after = 5;
		//aleph_predictive.print("num_entries", "FPR", commas_before++, commas_after--);
		//polynomial.print("num_entries", "FPR", commas_before++, commas_after--);
		//fingerprint_sacrifice_res.print("num_entries", "FPR", commas_before++, commas_after--);
		//uniform.print("num_entries", "FPR", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 5;
		//aleph_predictive.print("num_entries", "memory", commas_before++, commas_after--);
		//polynomial.print("num_entries", "memory", commas_before++, commas_after--);
		//fingerprint_sacrifice_res.print("num_entries", "memory", commas_before++, commas_after--);
		//uniform.print("num_entries", "memory", commas_before++, commas_after--);
		
		//System.out.println();
		
		commas_before = 1;
		commas_after = 5;
		//aleph_predictive.print("num_entries", "delete_time", commas_before++, commas_after--);
		//polynomial.print("num_entries", "delete_time", commas_before++, commas_after--);
		//fingerprint_sacrifice_res.print("num_entries", "delete_time", commas_before++, commas_after--);
		//uniform.print("num_entries", "delete_time", commas_before++, commas_after--);

		//System.out.println();
		
		commas_before = 1;
		commas_after = 5;
		polynomial.print("num_entries", "expansion", commas_before++, commas_after--);
		uniform.print("num_entries", "expansion", commas_before++, commas_after--);
		polynomial.print("num_entries", "background_deletes", commas_before++, commas_after--);
		uniform.print("num_entries", "background_deletes", commas_before++, commas_after--);

		
		
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Calendar.getInstance().getTime());

		LocalDate ld = java.time.LocalDate.now();
		String dir_name = "Exp6_" + bits_per_entry + "_bytes_" +  timeStamp.toString();
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
		String delete_latency_file_name = dir_name + "/deletes_speed.txt";
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
			//aleph_predictive.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			polynomial.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			uniform.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);

			//System.out.println();
			insertion_writer.close();
	        FileWriter reads_writer = new FileWriter(read_latency_file_name);

			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			polynomial.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			uniform.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);

			//System.out.println();
			reads_writer.close();
	        FileWriter FPR_writer = new FileWriter(FPR_file_name);

			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			polynomial.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			uniform.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);

			FPR_writer.close();
			FileWriter mem_writer = new FileWriter(memory_file_name);
			
			//System.out.println();

			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			polynomial.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			uniform.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);

			mem_writer.close();
			FileWriter del_writer = new FileWriter(delete_latency_file_name);
						
			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, del_writer);
			polynomial.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, del_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, del_writer);
			uniform.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, del_writer);
			
			del_writer.close();
	    	FileWriter all_writer = new FileWriter(all_file_name);

			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			polynomial.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			uniform.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			polynomial.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			uniform.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			polynomial.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			uniform.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			polynomial.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			uniform.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");

			commas_before = 1;
			commas_after = 5;
			//aleph_predictive.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, all_writer);
			polynomial.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, all_writer);
			//fingerprint_sacrifice_res.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, all_writer);
			uniform.print_to_file("num_entries", "delete_time", commas_before++, commas_after--, all_writer);
			
			all_writer.close();
	    	
	        System.out.println("Successfully wrote to the files.");
	      } catch (IOException e) {
	        System.out.println("An error occurred.");
	        e.printStackTrace();
	      }
	}

	
	
	static public long scalability_experiment(DuplicatingChainedInfiniFilter qf, long initial_key, baseline results) {

		int num_qeuries = 1000000;
		int query_index = Integer.MAX_VALUE;
		int num_false_positives = 0;

		//int num_entries_to_insert = (int) (Math.pow(2, power) * (qf.expansion_threshold )) - qf.num_existing_entries;
		//final int initial_num_entries = qf.get_num_entries(true);
		
		long initial_num_entries = initial_key;
		long insertion_index = initial_key;
		
		
		
		

		
		//System.out.println("inserting: " + num_entries_to_insert + " to capacity " + Math.pow(2, qf.power_of_two_size));

		/*boolean successful_insert = false;
		do {
			successful_insert = qf.insert(insertion_index, false);
			insertion_index++;
		} while (insertion_index < end_key && successful_insert);*/
		
		
		long start_deletes = System.nanoTime();
		qf.resolve_pending_operations();
		long end_deletes = System.nanoTime();
		
		
		long start_expansion = System.nanoTime();
		if (qf.get_num_expansions() > 0) {
			qf.expand();
			qf.num_expansions++;
		}
		long end_expansion = System.nanoTime();
		
		
		long start_insertions = System.nanoTime();
		
		boolean successful_insert = false;
		long phys_entries = 0, max_entries = 0;
		do {
			successful_insert = qf.insert(insertion_index, false);
			insertion_index++;
			//System.out.println(qf.get_num_physical_entries() + "  " + qf.get_max_entries_before_expansion());
			 phys_entries = qf.get_num_physical_entries();
			 max_entries = qf.get_max_entries_before_expansion();
		} while (phys_entries <= max_entries - 2 && successful_insert);
				
		
		if (!successful_insert) {
			System.out.println("an insertion failed");
			System.exit(1);
		}
		
		//qf.pretty_print();

		long end_insertions = System.nanoTime();
		long start_queries = System.nanoTime();

		for (int i = 0; i < num_qeuries || num_false_positives < 10; i++) {
			boolean found = true;
			found = qf.search(query_index--);
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
		
		double expansion_time = (end_expansion - start_expansion) ;
		
		double background_delete_time = (end_deletes - start_deletes) ;
		
		double FPR = num_false_positives / (double)num_qeuries;
		//int num_slots = (1 << qf.power_of_two_size) - 1;
		//double utilization = qf.get_utilization();
		//System.out.println("utilization  " + utilization);
		//double num_entries = qf.get_num_entries(true);
		double num_entries = insertion_index; // qf.get_num_occupied_slots(true);

		results.metrics.get("num_entries").add(num_entries);
		results.metrics.get("insertion_time").add(avg_insertions);
		results.metrics.get("query_time").add(avg_queries);
		results.metrics.get("FPR").add(FPR);
		double bits_per_entry = qf.measure_num_bits_per_entry();
		//System.out.println(bits_per_entry);
		results.metrics.get("memory").add(bits_per_entry);
		results.metrics.get("delete_time").add(0.0);
		
		results.metrics.get("expansion").add(expansion_time);
		results.metrics.get("background_deletes").add(background_delete_time);	
				
		return insertion_index;
	}
	
	
	static public void delete_oldest(Filter qf, long initial_key, long end_key, baseline results) {
		
		//long initial_num_entries = initial_key;
		long delete_index = initial_key;
		long start_deletes = System.nanoTime();
		
		//System.out.println("inserting: " + num_entries_to_insert + " to capacity " + Math.pow(2, qf.power_of_two_size));

		long slot_of_deleted_key = -1;
		int num_deletes = 0;
		do {
			slot_of_deleted_key = qf.delete(delete_index);
			//boolean found = qf.search(delete_index);
			delete_index++;
			num_deletes++;
		} while (delete_index < end_key && slot_of_deleted_key > -1);
		
		if (slot_of_deleted_key == -1) {
			System.out.println("an delete failed");
			System.exit(1);
		}
		
		long end_deletes = System.nanoTime();
		
		double avg_deletes = (end_deletes - start_deletes) / (double)(num_deletes);
		//System.out.println("avg_deletes  " + avg_deletes);
		int last_index = results.metrics.get("delete_time").size() - 1;
		results.metrics.get("delete_time").set(last_index, avg_deletes);	
	}
	
}
