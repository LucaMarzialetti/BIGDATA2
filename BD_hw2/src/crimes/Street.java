package crimes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
import scala.Tuple5;
import scala.Tuple7;

public class Street {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Street <input_dataset_txt> <output>");
			System.exit(1);}
		path_to_dataset=args[0];		//street
		path_to_output_dir=args[1];		//crime_freq
		String appName = "Street";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		crime_frequency_by_lsoa();
	}//end main

	// Load the data from CSVs
	public static JavaRDD<String> loadData(String path, boolean header) { 
		// create spark configuration and spark context
		//conf.setMaster("local[*]");
		//sc.addJar("MBA.jar");
		JavaRDD<String> rdd = sc.textFile(path);
		if(header){
			rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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

	//Street reports analysis : social groups-crime types relationships
	@SuppressWarnings("serial")
	public static List<Tuple2<Tuple5<String,String,String,String,String>,Iterable<Tuple3<String,String,Integer>>>> crime_frequency_by_lsoa() {

		//formato originale  
		JavaRDD<String> crime_reports_joined_fields = loadData(path_to_dataset,true);

		//mappa il formato in <<LSOA_CODE,LSOA_NAME,SUP_GROUP,GROUP,SUB_GROUP,CRIME,OUTCOME>,INTEGER>                                                       			 input   output:K               output:V
		JavaPairRDD<Tuple7<String,String,String,String,String,String,String>,Integer> crime_reports_mandatory_fields = 
				crime_reports_joined_fields.mapToPair(
						new PairFunction<String, Tuple7<String,String,String,String,String,String,String>, Integer>() {
							public Tuple2< Tuple7<String,String,String,String,String,String,String>, Integer > call(String report) {
								List<String> report_fields = new ArrayList<String>(Arrays.asList(report.split(",",-1)));

								String lsoa_code=report_fields.get(0);
								String lsoa_name=report_fields.get(1);
								String super_group=report_fields.get(2);
								String group=report_fields.get(3);
								String sub_group=report_fields.get(4);
								String crime_type=report_fields.get(5);
								String outcome=report_fields.get(6);

								return new Tuple2< Tuple7<String,String,String,String,String,String,String>, Integer >(new Tuple7<String,String,String,String,String,String,String>(lsoa_code, lsoa_name,super_group,group,sub_group,crime_type,outcome), 1);
							}
						});

		//couples.saveAsTextFile(path_to_output_dir+"/couples");

		//reduce in <<LSOA_CODE,LSOA_NAME,SUP_GROUP,GROUP,SUB_GROUP,CRIME,OUTCOME>,TOT>  
		JavaPairRDD<Tuple7<String,String,String,String,String,String,String>, Integer> crime_reports_mandatory_fields_reduced = 
				crime_reports_mandatory_fields.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return t1+t2;}
				});

		//couples_reduced.saveAsTextFile(path_to_output_dir+"/couples_reduced");

		//cambia formato da <<LSOA_CODE,LSOA_NAME,SUP_GROUP,GROUP,SUB_GROUP,CRIME,OUTCOME>,TOT> a <<LSOA_CODE,LSOA_NAME,SUP_GROUP,GROUP,SUB_GROUP>,<CRIME,OUTCOME,TOT> > 
		JavaPairRDD<Tuple5<String,String,String,String,String>,Tuple3<String,String,Integer>> lsoa_and_group_as_key =
				crime_reports_mandatory_fields_reduced.mapToPair(new PairFunction< Tuple2< Tuple7<String,String,String,String,String,String,String>,Integer> , Tuple5<String,String,String,String,String>, Tuple3<String,String,Integer> >() {
					public Tuple2<Tuple5<String,String,String,String,String>, Tuple3<String,String,Integer>> call(Tuple2<Tuple7<String,String,String,String,String,String, String>, Integer> arg0)	throws Exception {
						return new Tuple2<Tuple5<String,String,String,String,String>, Tuple3<String,String,Integer>>( new Tuple5<String,String,String,String,String>(arg0._1._1(),arg0._1._2(),arg0._1._3(),arg0._1._4(),arg0._1._5()), new Tuple3<String,String,Integer>(arg0._1._6(),arg0._1._7(),arg0._2));
					}
				});

		//item_as_key.saveAsTextFile(path_to_output_dir+"/item_as_key");

		//raggruppa: output = <<LSOA_CODE,LSOA_NAME,SUP_GROUP,GROUP,SUB_GROUP>,lista[<CRIME,OUTCOME,TOT>] >
		JavaPairRDD<Tuple5<String,String,String,String,String>,Iterable<Tuple3<String,String,Integer>>> lsoa_and_group_as_key_grouped = lsoa_and_group_as_key.groupByKey();
		//item_grouped.saveAsTextFile(path_to_output_dir+"/result");

		//ordinamento dei crime_types per frequenza
		JavaPairRDD<Tuple5<String,String,String,String,String>,Iterable<Tuple3<String,String,Integer>>> lsoa_and_group_as_key_grouped_ordered =
				lsoa_and_group_as_key_grouped.mapValues(
						new Function<Iterable<Tuple3<String,String,Integer>>,Iterable<Tuple3<String,String,Integer>>>() {
							public Iterable<Tuple3<String,String,Integer>> call(Iterable<Tuple3<String,String,Integer>> arg0)throws Exception {
								List<Tuple3<String,String,Integer>> sortedList = new ArrayList<Tuple3<String,String,Integer>>();
								for (Tuple3<String,String,Integer> t : arg0) {sortedList.add(t);}
								Collections.sort(sortedList,Utils.crime_frequency_comparator);
								return sortedList;
							}
						});
		/**flat finale delle tuple**/
		//1			2			3				4				5			6		7		8
		//
//		JavaRDD<String> flatted;
//		flatted = lsoa_and_group_as_key_grouped_ordered.flatMap(new FlatMapFunction<Tuple2<Tuple5<String,String,String,String,String>,Iterable<Tuple3<String,String,Integer>>>, String>() {
//
//			@Override
//			public Iterable<String> call(
//					Tuple2<Tuple5<String, String, String, String, String>, Iterable<Tuple3<String, String, Integer>>> t)
//					throws Exception {
//				LinkedList<String> list = new LinkedList<String>();
//				LinkedList<String> ans = new LinkedList<String>();
//				String comp ="";
//				list.addAll(Arrays.asList(t.toString().replaceAll("[()]", "").split(",",-1)));
//				int i;
//				//con virgolette
//				//				for(i=0; i<list.size()-1; i++)
//				//					comp+="\""+list.get(i)+"\", ";
//				//				comp+="\""+list.get(i)+"\"";
//				for(i=0; i<list.size()-1; i++)
//					comp+=list.get(i)+",";
//				comp+=list.get(i);
//				ans.add(comp);
//				return ans;
//			}
//		});
//		flatted = flatted.coalesce(1);
//		flatted.saveAsTextFile(path_to_output_dir);	
		lsoa_and_group_as_key_grouped_ordered = lsoa_and_group_as_key_grouped_ordered.coalesce(1);
		lsoa_and_group_as_key_grouped_ordered.saveAsTextFile(path_to_output_dir);
		List<Tuple2<Tuple5<String, String, String, String, String>, Iterable<Tuple3<String, String, Integer>>>> result= lsoa_and_group_as_key_grouped_ordered.collect();
		sc.close();
		return result;
	}//end class

}//end App