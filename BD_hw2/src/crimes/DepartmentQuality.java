package crimes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

public class DepartmentQuality {
	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
		        System.err.println("Usage: Department <input_dataset_folder> <output>");
		        System.exit(1);
		}
		
		path_to_dataset=args[0];
		path_to_output_dir=args[1];
		//confrontare poi i due outcomes:come?
		stop_search_outcomes();
		street_outcomes();
	}//end main
	
	// immaginando di avere tutti gli ss di un dipartimento in una cartella
	// immaginando di avere tutti gli street di un dipartimento in una cartella
	// Load the data from the text file and return an RDD of stop_search_reports
	public static JavaRDD<String> load_ss_Data() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("Department SS");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("<name>.jar");
		JavaRDD<String> department_ss_reports = sc.textFile(path_to_dataset);
		return department_ss_reports;
	}
	
	public static JavaRDD<String> load_st_Data() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("Department ST");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("<name>.jar");
		JavaRDD<String> department_st_reports = sc.textFile(path_to_dataset);
		return department_st_reports;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//jobs 	
	@SuppressWarnings("serial")
	public static List<Tuple2<String,Iterable<Tuple3<String,String,Integer>>>> stop_search_outcomes() {

		JavaRDD<String> department_reports = load_ss_Data();
		//ss fields = 10,12,13 = legislation, outcome, outcome linked to subject
		// mappa il formato in < <LEGISLATION, OUTCOME, OUTCOME_LINKED_TO_OBJ>, 1 >                                                       		input   output:K               output:V
		JavaPairRDD<Tuple3<String,String,String>, Integer> department_searches = 
				department_reports.mapToPair(
						new PairFunction<String, Tuple3<String,String,String>, Integer>() {
							public Tuple2< Tuple3<String,String,String>,Integer > call(String report) {
								List<String> report_fields = new ArrayList<String>(Arrays.asList(report.split(",")));
								String legislation=report_fields.get(10);
								String outcome=report_fields.get(12);
								String outcome_linked=report_fields.get(13);
								return new Tuple2< Tuple3<String,String,String>, Integer>(new Tuple3<String,String,String>(legislation,outcome,outcome_linked),1);
							}
						});
		
		department_searches.saveAsTextFile(path_to_output_dir);
		
		//reduce in < <LEGISLATION, OUTCOME, OUTCOME_LINKED_TO_OBJ>, TOT >  
		JavaPairRDD<Tuple3<String,String,String>, Integer>department_searches_reduced = 
				department_searches.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return t1+t2;}
				});
		
		department_searches_reduced.saveAsTextFile(path_to_output_dir);

		//split in < LEGISLATION, <OUTCOME, OUTCOME_LINKED_TO_OBJ,TOT> >  
		JavaPairRDD<String, Tuple3<String,String,Integer> > department_searches_key_split = 
				department_searches_reduced.mapToPair(new PairFunction< Tuple2< Tuple3<String,String,String>, Integer> , String, Tuple3<String,String,Integer> >() {
					public Tuple2< String, Tuple3<String,String,Integer>> call(Tuple2<Tuple3<String,String, String>, Integer> arg0)	throws Exception {
						return new Tuple2<String, Tuple3<String,String,Integer>>( arg0._1._1(), new Tuple3<String,String,Integer>(arg0._1._2(),arg0._1._3(),arg0._2));
					}
				});
		
		department_searches_key_split.saveAsTextFile(path_to_output_dir);
		
		//group values
		JavaPairRDD<String, Iterable<Tuple3<String,String,Integer>>> department_searches_counts =
				department_searches_key_split.groupByKey();
		
		department_searches_counts.saveAsTextFile(path_to_output_dir);

		//orderd by frequency
		JavaPairRDD<String, Iterable<Tuple3<String,String,Integer>>> department_searches_counts_ordered =
				department_searches_counts.mapValues(
						new Function<Iterable<Tuple3<String,String,Integer>>,Iterable<Tuple3<String,String,Integer>>>() {
							public Iterable<Tuple3<String,String,Integer>> call(Iterable<Tuple3<String,String,Integer>> arg0) throws Exception {
								List<Tuple3<String,String,Integer>> sortedList = new ArrayList<Tuple3<String,String,Integer>>();
								for (Tuple3<String,String,Integer> t : arg0) {sortedList.add(t);}
								Collections.sort(sortedList,Utils.crime_frequency_comparator);
								return sortedList;
							}
						});

		//ethnicity_searches_frequencies_ordered.cache();
		department_searches_counts_ordered.saveAsTextFile(path_to_output_dir);
		List<Tuple2<String,Iterable<Tuple3<String,String,Integer>>>> result=department_searches_counts_ordered.collect();
		
		//street 10 last outcome
		//chiude il contesto
		sc.close();
		return result;
	}//end stop_search_department

	@SuppressWarnings("serial")
	public static List<Tuple2<String,Integer>> street_outcomes() {

		JavaRDD<String> department_reports = load_st_Data();
		//sT field = 10 =  outcome
		// mappa il formato in <  OUTCOME, 1 >                                                       		input   output:K               output:V
		JavaPairRDD<String, Integer> department_streets = 
				department_reports.mapToPair(
						new PairFunction<String, String, Integer>() {
							public Tuple2< String,Integer > call(String report) {
								List<String> report_fields = new ArrayList<String>(Arrays.asList(report.split(",")));
								String outcome=report_fields.get(10);
								return new Tuple2< String, Integer>(outcome,1);
							}
						});
		
		department_streets.saveAsTextFile(path_to_output_dir);
		
		//reduce in < OUTCOME, TOT >  
		JavaPairRDD<String, Integer>department_streets_reduced = 
				department_streets.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return t1+t2;}
				});
		
		department_streets_reduced.saveAsTextFile(path_to_output_dir);

		List<Tuple2<String,Integer>> result=department_streets_reduced.collect();
		
		//chiude il contesto
		sc.close();
		return result;
	}//end street_department

}
