package crimes;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class Header {

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
		String appName = "StopAndSearchSex";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		stop_search_sexism();
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

	public static void stop_search_sexism() {
		JavaRDD<String> loaded = loadData(path_to_dataset, true);
		loaded.saveAsTextFile(path_to_output_dir);
	}
}//end App