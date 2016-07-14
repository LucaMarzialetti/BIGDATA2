package crimes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

public class StopSearchRace {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Stop&Search <input_dataset_txt> <output>");
			System.exit(1);
		}
		path_to_dataset=args[0];		//stop-and-search
		path_to_output_dir=args[1];		//racism
		String appName = "StopAndSearchRace_Job";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		stop_search_racism();
		sc.close();
	}//end main

	// Load the data from CSVs
	public static JavaRDD<String> loadData(String path, boolean header) { 
		// create spark configuration and spark context
		//conf.setMaster("local[*]");
		//sc.addJar("MBA.jar");
		JavaRDD<String> rdd = sc.textFile(path);
		if(header){
			rdd = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
					if(ind==0 && iterator.hasNext()){
						iterator.next();
						return iterator;
					}
					else
						return iterator;
				}
			},false);
		}
		return rdd;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//jobs on stop and search dataset

	@SuppressWarnings("serial")
	public static void stop_search_racism() {

		JavaRDD<String> stop_search_reports = loadData(path_to_dataset,true);

		// mappa il formato in < <SELF_DEF_ETHN,AGE,OBJ_OF_SEARCH, OUTCOME, OUTCOME_LINKED_TO_OBJ>, 1 >                                                       		input   output:K               output:V
		JavaPairRDD<Tuple5<String,String,String,String,String>, Integer> ethnicity_searches = 
				stop_search_reports.mapToPair(
						new PairFunction<String, Tuple5<String,String,String,String,String>, Integer>() {
							public Tuple2< Tuple5<String,String,String,String,String>,Integer > call(String report) {
								List<String> report_fields = new ArrayList<String>(Arrays.asList(report.split(",",-1)));
								String self_def_ethn=report_fields.get(8);
								//>>>>>>String officer_def_ethn=report_fields.get(9);
								String age_range=report_fields.get(7);
								String obj_of_search=report_fields.get(11);
								String outcome=report_fields.get(12);
								String outcome_linked=report_fields.get(13);
								return new Tuple2< Tuple5<String,String,String,String,String>, Integer>(new Tuple5<String,String,String,String,String>(self_def_ethn, age_range,obj_of_search, outcome,outcome_linked),1);
							}
						});

		//OUTDEBUG
		//ethnicity_searches.saveAsTextFile(path_to_output_dir1);

		//reduce in << <SELF_DEF_ETHN,AGE,OBJ_OF_SEARCH, OUTCOME, OUTCOME_LINKED_tO_OBJ>, TOT >  
		JavaPairRDD<Tuple5<String,String,String,String,String>, Integer> ethnicity_searches_grouped = 
				ethnicity_searches.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return new Integer(t1.intValue()+t2.intValue());}
				});

		//OUTDEBUG
		//ethnicity_searches_grouped.saveAsTextFile(path_to_output_dir1);

		//split in << <SELF_DEF_ETHN,AGE>,<OBJ_OF_SEARCH, OUTCOME, OUTCOME_LINKED_TO_OBJ,TOT> >  
		JavaPairRDD<Tuple2<String,String>, Tuple4<String,String,String,Integer> > ethnicity_searches_key_split = 
				ethnicity_searches_grouped.mapToPair(new PairFunction< Tuple2< Tuple5<String,String,String,String,String>,Integer> , Tuple2<String,String>, Tuple4<String,String,String,Integer> >() {
					public Tuple2< Tuple2<String,String>, Tuple4<String,String,String,Integer>> call(Tuple2<Tuple5<String,String,String,String, String>, Integer> arg0)	throws Exception {
						return new Tuple2<Tuple2<String,String>, Tuple4<String,String,String,Integer>>( new Tuple2<String,String>(arg0._1._1(),arg0._1._2()), new Tuple4<String,String,String,Integer>(arg0._1._3(),arg0._1._4(),arg0._1._5(),arg0._2));
					}
				});

		//OUTDEBUG
		//ethnicity_searches_key_split.saveAsTextFile(path_to_output_dir1);

		//group values
		JavaPairRDD<Tuple2<String,String>,Iterable<Tuple4<String,String,String,Integer>>> ethnicity_searches_frequencies =
				ethnicity_searches_key_split.groupByKey();

		//OUTDEBUG
		//ethnicity_searches_frequencies.saveAsTextFile(path_to_output_dir1);

		//orderd by frequency
		JavaPairRDD<Tuple2<String,String>,Iterable<Tuple4<String,String,String,Integer>>> ethnicity_searches_frequencies_ordered =
				ethnicity_searches_frequencies.mapValues(
						new Function<Iterable<Tuple4<String,String,String,Integer>>,Iterable<Tuple4<String,String,String,Integer>>>() {
							public Iterable<Tuple4<String,String,String,Integer>> call(Iterable<Tuple4<String,String,String,Integer>> arg0) throws Exception {
								List<Tuple4<String,String,String,Integer>> sortedList = new ArrayList<Tuple4<String,String,String,Integer>>();
								for (Tuple4<String,String,String,Integer> t : arg0) {sortedList.add(t);}
								Collections.sort(sortedList,Utils.ss_racism_frequency_comparator);
								return sortedList;
							}
						});

		/**flat finale delle tuple**/
		//1				2		3				4			5				6
		//ethnicity 	age		[obj_of_search	outcome		outcome_linked	freq]
		JavaRDD<String> flatted;
		flatted = ethnicity_searches_frequencies_ordered.flatMap(new FlatMapFunction<Tuple2<Tuple2<String,String>,Iterable<Tuple4<String,String,String,Integer>>>, String>() {

			@Override
			public Iterable<String> call(
					Tuple2<Tuple2<String, String>, Iterable<Tuple4<String, String, String, Integer>>> t)
							throws Exception {
				LinkedList<String> list = new LinkedList<String>();
				LinkedList<String> ans = new LinkedList<String>();
				String comp ="";
				list.addAll(Arrays.asList(t.toString().replaceAll("[()]", "").split(",",-1)));
				int i;
				//con virgolette
				//				for(i=0; i<list.size()-1; i++)
				//					comp+="\""+list.get(i)+"\", ";
				//				comp+="\""+list.get(i)+"\"";
				for(i=0; i<list.size()-1; i++)
					comp+=list.get(i)+",";
				comp+=list.get(i);
				ans.add(comp);
				return ans;
			}
		});
		flatted = flatted.coalesce(1);
		flatted.saveAsTextFile(path_to_output_dir);
	}
	//*******************end stop_search_racism****************//
}//end App