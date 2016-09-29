package dataset;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple7;

public class StopSearchDatasetFull {
	private static String path_to_dataset1;
	private static String path_to_dataset2;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		//check args
		//args[0]=input
		//args[1]=input
		//ags[2]=ouput
		path_to_dataset1=args[0];		//soag
		path_to_dataset2=args[1];		//street
		path_to_output_dir=args[2];
		String appName = "Street_Dataset";
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
		JavaRDD<String> SOA_sg = loadData(path_to_dataset1, false);
		JavaRDD<String> street = loadNestedData(path_to_dataset2, true);
		/**MAPPING**/
		//mapping di soa_sg
		JavaPairRDD<Tuple2<String, String>, Tuple3<String, String, String>> SOA_sg_couples;
		SOA_sg_couples = SOA_sg.mapToPair(new PairFunction<String, Tuple2<String, String>, Tuple3<String, String, String>>(){
			@Override
			public Tuple2<Tuple2<String, String>, Tuple3<String, String, String>> call(String line) throws Exception {
				Tuple2<Tuple2<String,String>, Tuple3<String,String,String>> tupla = new Tuple2<Tuple2<String,String>, Tuple3<String,String,String>>(new Tuple2<String, String>("", ""), new Tuple3<String, String, String>("", "", ""));
				if(!(line==null || line.isEmpty() || line.length()==0)) {
					//separatore tra double quotes
					//tutto quello che c'è tra due double quotes consecutive
					String[] tokens = line.split(",",-1);
					if(tokens.length>=6){
						String soa_code = tokens[0];
						String soa_name = tokens[1];
						String supg = tokens[4];
						String g = tokens[5];
						String subg = tokens[6];
						Tuple2<String,String> k = new Tuple2<String, String>(soa_code,soa_name);
						Tuple3<String, String, String> v = new Tuple3<String, String, String>(supg, g, subg);
						tupla = new Tuple2<Tuple2<String,String>, Tuple3<String,String,String>>(k, v);
					}
				}
				return tupla;
			}
		});
		//mapping di crimes
		JavaPairRDD<Tuple2<String, String>, Tuple4<String, String,String,String>> street_couples;
		street_couples = street.mapToPair(new PairFunction<String, Tuple2<String, String>, Tuple4<String, String,String,String>>(){
			@Override
			public Tuple2<Tuple2<String, String>, Tuple4<String, String,String,String>> call(String line) throws Exception {
				Tuple2<Tuple2<String,String>, Tuple4<String,String,String,String>> tupla = new Tuple2<Tuple2<String,String>, Tuple4<String,String,String,String>>(new Tuple2<String, String>("", ""), new Tuple4<String, String,String,String>("", "","",""));
				if(!(line==null || line.isEmpty() || line.length()==0)){
					String[] tokenizer = line.split(",",-1);
					if(tokenizer.length>=10){
						String longitude = tokenizer[4];
						String latitude = tokenizer[5];
						String soa_code = tokenizer[7];
						String soa_name = tokenizer[8];
						String crime = tokenizer[9];
						String outcome = tokenizer[10];
						Tuple2<String, String> k = new Tuple2<String, String>(soa_code, soa_name);
						Tuple4<String, String, String, String> v = new Tuple4<String, String, String, String>(longitude, latitude, crime, outcome);
						tupla = new Tuple2<Tuple2<String,String>, Tuple4<String,String,String,String>>(k,v); 
					}
				}
				return tupla;
			}
		});
		//join
		JavaPairRDD<Tuple2<String,String>, Tuple7<String,String,String,String,String,String,String>> joined;
		joined = SOA_sg_couples.join(street_couples).mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Tuple2<Tuple3<String,String,String>,Tuple4<String,String,String,String>>>, Tuple2<String,String>, Tuple7<String,String,String,String,String,String,String>>() {

			@Override
			public Tuple2<Tuple2<String, String>, Tuple7<String, String, String, String, String, String, String>> call(
					Tuple2<Tuple2<String, String>, Tuple2<Tuple3<String, String, String>, Tuple4<String, String, String, String>>> t)
					throws Exception {
				Tuple2<String,String> k = t._1;
				Tuple7<String,String,String,String,String,String,String> v = new Tuple7<String,String,String,String,String,String,String>(t._2._1._1(),t._2._1._2(),t._2._1._3(), t._2._2._1(), t._2._2._2(),t._2._2._3(),t._2._2._4());
				return new Tuple2<Tuple2<String,String>, Tuple7<String,String,String,String,String,String,String>> (k,v);
			}
		});
								
		//flatted 	1			2			3				4			5			6 		7		8		9
		//			lsoa_code 	lsoa_name 	supergroups		groups		subgroups	long	lat		crime	outcome
		JavaRDD<String> flatted;
		flatted = joined.flatMap(new FlatMapFunction<Tuple2<Tuple2<String,String>,Tuple7<String,String,String,String,String,String,String>>, String>() {
			@Override
			public Iterable<String> call(
					Tuple2<Tuple2<String, String>, Tuple7<String,String,String, String, String, String, String>> t) throws Exception {
				LinkedList<String> list = new LinkedList<String>();
				LinkedList<String> ans = new LinkedList<String>();
				String comp ="";
				list.addAll(Arrays.asList(t.toString().replaceAll("[()]", "").split(",",-1)));
				int i;
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
}