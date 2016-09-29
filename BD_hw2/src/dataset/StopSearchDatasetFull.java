package dataset;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

public class StopSearchDatasetFull {
	private static String path_to_dataset1;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		//check args
		//args[0]=input
		//args[1]=input
		//ags[2]=ouput
		path_to_dataset1=args[0];		//stop_search
		path_to_output_dir=args[1];
		String appName = "StopSearch_Dataset";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		Street_dataset_job();
		sc.close();
	}

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

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**map oa and social groups to soa**/
	@SuppressWarnings("serial")
	public static void Street_dataset_job() {
		//load data
		JavaRDD<String> stop_search = loadData(path_to_dataset1, true);
		/**MAPPING**/
		//mapping di stop_search
		JavaRDD<String> SS  = stop_search.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String t) throws Exception {
				List<String> report_fields = new ArrayList<String>(Arrays.asList(t.split(",",-1)));
				String type=report_fields.get(0);
				String date=report_fields.get(1);
				String latitude=report_fields.get(4);
				String longitude=report_fields.get(5);
				String gender=report_fields.get(6);
				String age=report_fields.get(7);
				String ethnicity=report_fields.get(9);
				String outcome=report_fields.get(12);
				String clothes_removal=report_fields.get(14);
				LinkedList<String> ans = new LinkedList<String>();
				String comp= type+","+date+","+longitude+","+latitude+","+gender+","+age+","+ethnicity+","+outcome+","+clothes_removal;
				ans.add(comp);
				return ans;
			}
		});
		
		//flatted 	1		2		3		4		5		6		7		8			9
		//			type 	date	long	lat		gender	age		ethn	outcome		clothes_r
		JavaRDD<String> flatted = SS.coalesce(1);
		flatted.saveAsTextFile(path_to_output_dir);
	}
}