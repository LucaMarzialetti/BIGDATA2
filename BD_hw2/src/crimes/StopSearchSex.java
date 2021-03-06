package crimes;

import java.io.File;
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
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple7;

public class StopSearchSex {

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
		path_to_output_dir=args[1];		//sex
		String appName = "StopAndSearchSex_Job";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		stop_search_sexism();
		sc.close();
	}//end main

	public static JavaRDD<String> loadNestedData(String path, boolean header) {
		File file = new File(path);
		JavaRDD<String> merged = sc.emptyRDD();
		String[] names = file.list();
		for(String name : names){
			JavaRDD<String> tmp = loadData(path+"/"+name, header);
			merged = merged.union(tmp);
		}
		return merged;
	}


	// Load the data from CSVs
	public static JavaRDD<String> loadData(String path, boolean header) { 
		// create spark configuration and spark context
		//conf.setMaster("local[*]");
		//sc.addJar("MBA.jar");
		JavaRDD<String> rdd = sc.emptyRDD();
		File f = new File(path);
		if(f.isDirectory())
			rdd = loadNestedData(path, header);
		else
			if(f.isFile())
				rdd = sc.textFile(path);
			else 
				rdd = sc.emptyRDD();
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

	@SuppressWarnings("serial")
	public static void stop_search_sexism() {

		JavaRDD<String> stop_search_reports = loadData(path_to_dataset,true);

		// mappa il formato in < <GENDER, AGE, HOUR,OBJ_OF_SEARCH, OUTCOME, OUTCOME_LINKED_TO_OBJ, CLOTH_REMOVAL>, 1 >                                                       		input   output:K               output:V
		JavaPairRDD<Tuple7<String,String,String,String,String,String,String>, Integer> gender_searches = 
				stop_search_reports.mapToPair(
						new PairFunction<String, Tuple7<String,String,String,String,String,String,String>, Integer>() {
							public Tuple2< Tuple7<String,String,String,String,String,String,String>, Integer > call(String report) {
								List<String> report_fields = new ArrayList<String>(Arrays.asList(report.split(",",-1)));

								String gender=report_fields.get(6);
								String age_range=report_fields.get(7);
								//rimuove giorno, mantiene solo ora nel formato hh
								//minuti servono? [no]
								String[] date_tokenizer= report_fields.get(1).split("T",-1);
								String time=date_tokenizer[1];
								String[] time_tokenizer=time.split(":",-1);
								String hour=time_tokenizer[0];
								String obj_of_search=report_fields.get(11);
								String outcome=report_fields.get(12);
								String outcome_linked=report_fields.get(13);
								String cloth_removal=report_fields.get(14);
								return new Tuple2< Tuple7<String,String,String,String,String,String,String>, Integer >(new Tuple7<String,String,String,String,String,String,String>(gender, age_range, hour,obj_of_search, outcome, outcome_linked, cloth_removal),1);
							}
						});

		//OUTDEBUG
		//gender_searches.saveAsTextFile(path_to_output_dir);

		//reduce values  
		JavaPairRDD<Tuple7<String,String,String,String,String,String,String>, Integer> gender_searches_reduced = 
				gender_searches.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return t1+t2;}
				});
		//OUTDEBUG
		//gender_searches_reduced.saveAsTextFile(path_to_output_dir);

		//split key
		JavaPairRDD<Tuple3<String,String,String>,Tuple5<String,String,String,String,Integer>> gender_searches_split_key =
				gender_searches_reduced.mapToPair(new PairFunction< Tuple2< Tuple7<String,String,String,String,String,String,String>,Integer> , Tuple3<String,String,String>, Tuple5<String,String,String,String,Integer> >() {
					public Tuple2< Tuple3<String,String,String>, Tuple5<String,String,String,String,Integer>> call(Tuple2<Tuple7<String,String,String,String,String,String, String>, Integer> arg0)	throws Exception {
						return new Tuple2<Tuple3<String,String,String>, Tuple5<String,String,String,String,Integer>>( new Tuple3<String,String,String>(arg0._1._1(),arg0._1._2(),arg0._1._3()), new Tuple5<String,String,String,String,Integer>(arg0._1._4(),arg0._1._5(),arg0._1._6(),arg0._1._7(),arg0._2));
					}
				});

		//OUTDEBUG
		//gender_searches_split_key.saveAsTextFile(path_to_output_dir);

		//group
		JavaPairRDD<Tuple3<String,String,String>,Iterable<Tuple5<String,String,String,String,Integer>>> gender_searches_split_key_grouped=
				gender_searches_split_key.groupByKey();

		//OUTDEBUG
		//gender_searches_split_key_grouped.saveAsTextFile(path_to_output_dir);

		//order values by frequency
		JavaPairRDD<Tuple3<String,String,String>,Iterable<Tuple5<String,String,String,String,Integer>>> gender_searches_split_key_grouped_ordered=
				gender_searches_split_key_grouped.mapValues(
						new Function<Iterable<Tuple5<String,String,String,String,Integer>>,Iterable<Tuple5<String,String,String,String,Integer>>>() {
							public Iterable<Tuple5<String,String,String,String,Integer>> call(Iterable<Tuple5<String,String,String,String,Integer>> arg0) throws Exception {
								List<Tuple5<String, String, String, String, Integer>> sortedList = new ArrayList<Tuple5<String,String,String,String,Integer>>();
								for (Tuple5<String, String, String, String, Integer> t : arg0) {sortedList.add(t);}
								Collections.sort(sortedList,Utils.ss_sexism_frequency_comparator);
								return sortedList;
							}
						});

		//gender_searches_split_key_grouped_ordered.cache();
		//FINAL
		/**flat finale delle tuple**/
		//1			2		3		4				5			6				7				8
		//gender 	age		hour	[obj_of_search	outcome		outcome_linked	cloth_removal	freq]
		JavaRDD<String> flatted;
		flatted = gender_searches_split_key_grouped_ordered.flatMap(new FlatMapFunction<Tuple2<Tuple3<String,String,String>,Iterable<Tuple5<String,String,String,String,Integer>>>, String>() {

			@Override
			public Iterable<String> call(
					Tuple2<Tuple3<String, String, String>, Iterable<Tuple5<String, String, String, String, Integer>>> t)
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
	}//end stop_search_sexism

}//end App