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
import java.util.Random;

import filters.ChainedInfiniFilter;
import filters.BasicInfiniFilter;
import filters.QuotientFilter;

public class Experiment2 extends ExperimentsBase {

	static public void rejuvenation_experiment(QuotientFilter qf, int power, baseline results, double fraction_queries) {
		int num_qeuries = 1000000;
		int query_index = Integer.MAX_VALUE;
		int num_false_positives = 0;

		//int num_entries_to_insert = (int) (Math.pow(2, power) * (qf.expansion_threshold )) - qf.num_existing_entries;
		final long initial_num_entries = qf.get_num_physical_entries();
		long insertion_index = initial_num_entries;
		Random gen = new Random(initial_num_entries);

		long start_insertions = System.nanoTime();

		//System.out.println("inserting: " + num_entries_to_insert + " to capacity " + Math.pow(2, qf.power_of_two_size));

		do {
			qf.insert(insertion_index, false);
			insertion_index++;
			if (gen.nextDouble() < fraction_queries) {
				for (int i = 0; i < fraction_queries; i++) {
					//long query_start = System.nanoTime();
					long random = Math.abs(gen.nextLong());
					long random_search_key = random % insertion_index; 
					boolean found;
					/*boolean found = qf.search(random_search_key);
					if(!found) {
						System.exit(1);
					}*/
					//long query_end = System.nanoTime();
					//query_tally += query_end - query_start;
					found = qf.rejuvenate(random_search_key);
					if(!found) {
						System.exit(1);
					}
				}
			}
		} while (qf.get_num_physical_entries() < qf.get_max_entries_before_expansion() - 1);
		//qf.pretty_print();

		long end_insertions = System.nanoTime();
		long start_queries = System.nanoTime();

		for (int i = 0; i < num_qeuries; i++) {
			boolean found = qf.search(query_index--);
			if (found) {
				num_false_positives++;
			}
		}

		long end_queries = System.nanoTime();
		double avg_insertions = (end_insertions - start_insertions) / (double)(insertion_index - initial_num_entries);
		double avg_queries = (end_queries - start_queries) / (double)num_qeuries;
		double FPR = num_false_positives / (double)num_qeuries;
		//int num_slots = (1 << qf.power_of_two_size) - 1;
		//double utilization = qf.get_utilization();
		double num_entries = qf.get_num_occupied_slots(true);

		//long totes_insertion = end_insertions - start_insertions;
		//System.out.println("insetion times: " + totes_insertion + "   query tally " + query_tally);
		results.metrics.get("num_entries").add(num_entries);
		results.metrics.get("insertion_time").add(avg_insertions);
		results.metrics.get("query_time").add(avg_queries);
		results.metrics.get("FPR").add(FPR);
		results.metrics.get("memory").add(qf.measure_num_bits_per_entry());

	}
	
	public static void main(String[] args) {
		parse_arguments(args);
		
		System.gc();
		{ QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
			Experiment1.scalability_experiment(qf, 0, qf.get_max_entries_before_expansion() - 1, new baseline());}
		{ QuotientFilter qf = new QuotientFilter(num_entries_power, bits_per_entry);
		Experiment1.scalability_experiment(qf, 0, qf.get_max_entries_before_expansion() - 1, new baseline());}

		System.gc();
		baseline chained_IF_1 = new baseline();
		{
			BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
			qf.set_expand_autonomously(true);
			for (int i = num_entries_power; i <= num_cycles; i++ ) {
				rejuvenation_experiment(qf, i, chained_IF_1, 0);
				System.out.println("infinifilter " + 0 + " rejuv  " + i);
			}
		}	

		System.gc();
		baseline chained_IF_2 = new baseline();
		{
			BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
			qf.set_expand_autonomously(true);
			for (int i = num_entries_power; i <= num_cycles; i++ ) {
				rejuvenation_experiment(qf, i, chained_IF_2, 0.2);
				System.out.println("infinifilter " + 0.2 + " rejuv  " + i);
			}
		}	

		System.gc();
		baseline chained_IF_3 = new baseline();
		{
			BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
			qf.set_expand_autonomously(true);
			for (int i = num_entries_power; i <= num_cycles; i++ ) {
				rejuvenation_experiment(qf, i, chained_IF_3, 1);
				System.out.println("infinifilter " + 1 + " rejuv  " + i);
			}
		}	

		System.gc();
		
		baseline chained_IF_4 = new baseline();
		{
			BasicInfiniFilter qf = new ChainedInfiniFilter(num_entries_power, bits_per_entry);
			qf.set_expand_autonomously(true);
			for (int i = num_entries_power; i <= num_cycles; i++ ) {
				rejuvenation_experiment(qf, i, chained_IF_4, 2);
				System.out.println("infinifilter " + 2 + " rejuv  " + i);
			}
		}	

		System.gc();


		int commas_before = 1;
		int commas_after = 3;
		chained_IF_1.print("num_entries", "insertion_time", commas_before++, commas_after--);
		chained_IF_2.print("num_entries", "insertion_time", commas_before++, commas_after--);
		chained_IF_3.print("num_entries", "insertion_time", commas_before++, commas_after--);
		chained_IF_4.print("num_entries", "insertion_time", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 3;
		chained_IF_1.print("num_entries", "query_time", commas_before++, commas_after--);
		chained_IF_2.print("num_entries", "query_time", commas_before++, commas_after--);
		chained_IF_3.print("num_entries", "query_time", commas_before++, commas_after--);
		chained_IF_4.print("num_entries", "query_time", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 3;
		chained_IF_1.print("num_entries", "FPR", commas_before++, commas_after--);
		chained_IF_2.print("num_entries", "FPR", commas_before++, commas_after--);
		chained_IF_3.print("num_entries", "FPR", commas_before++, commas_after--);
		chained_IF_4.print("num_entries", "FPR", commas_before++, commas_after--);

		System.out.println();

		commas_before = 1;
		commas_after = 3;
		chained_IF_1.print("num_entries", "memory", commas_before++, commas_after--);
		chained_IF_2.print("num_entries", "memory", commas_before++, commas_after--);
		chained_IF_3.print("num_entries", "memory", commas_before++, commas_after--);
		chained_IF_4.print("num_entries", "memory", commas_before++, commas_after--);


		
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(Calendar.getInstance().getTime());

		LocalDate ld = java.time.LocalDate.now();
		String dir_name = "Exp2_" + bits_per_entry + "_bytes_" +  timeStamp.toString();
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
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			chained_IF_2.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			chained_IF_3.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);
			chained_IF_4.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, insertion_writer);

			//System.out.println();
			insertion_writer.close();
	        FileWriter reads_writer = new FileWriter(read_latency_file_name);

			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			chained_IF_2.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			chained_IF_3.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);
			chained_IF_4.print_to_file("num_entries", "query_time", commas_before++, commas_after--, reads_writer);

			//System.out.println();
			reads_writer.close();
	        FileWriter FPR_writer = new FileWriter(FPR_file_name);

			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			chained_IF_2.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			chained_IF_3.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);
			chained_IF_4.print_to_file("num_entries", "FPR", commas_before++, commas_after--, FPR_writer);

			FPR_writer.close();
			FileWriter mem_writer = new FileWriter(memory_file_name);
			
			//System.out.println();

			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			chained_IF_2.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			chained_IF_3.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
			chained_IF_4.print_to_file("num_entries", "memory", commas_before++, commas_after--, mem_writer);
	        
			mem_writer.close();
						
	    	FileWriter all_writer = new FileWriter(all_file_name);

			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			chained_IF_2.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			chained_IF_3.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);
			chained_IF_4.print_to_file("num_entries", "insertion_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			chained_IF_2.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			chained_IF_3.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);
			chained_IF_4.print_to_file("num_entries", "query_time", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			chained_IF_2.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			chained_IF_3.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);
			chained_IF_4.print_to_file("num_entries", "FPR", commas_before++, commas_after--, all_writer);

			all_writer.write("\n");
			
			commas_before = 1;
			commas_after = 3;
			chained_IF_1.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			chained_IF_2.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			chained_IF_3.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
			chained_IF_4.print_to_file("num_entries", "memory", commas_before++, commas_after--, all_writer);
	    	
			all_writer.close();
	    	
	        System.out.println("Successfully wrote to the files.");
	      } catch (IOException e) {
	        System.out.println("An error occurred.");
	        e.printStackTrace();
	      }
		
	}
	
}
	

