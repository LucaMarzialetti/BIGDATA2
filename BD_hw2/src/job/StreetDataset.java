package job;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class StreetDataset {
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
		String appName = "Dataset";
		conf = new SparkConf().setAppName(appName);
		sc = new JavaSparkContext(conf);
		OA_to_SOA_job();
	}

	// Load the data from CSVs
	public static JavaRDD<String> loadData(String path) { 
		// create spark configuration and spark context
		//conf.setMaster("local[*]");
		//sc.addJar("MBA.jar");
		JavaRDD<String> rdd = sc.textFile(path);
		return rdd;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/**map oa and social groups to soa**/
	@SuppressWarnings("serial")
	public static void OA_to_SOA_job() {
		//load data
		JavaRDD<String> SOA_sg = loadData(path_to_dataset1);
		JavaRDD<String> street = loadData(path_to_dataset2);
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
					Pattern p = Pattern.compile("\"([^\"]*)\"");
					Matcher m = p.matcher(line);
					int i = 1;
					String soa_code=null,soa_name=null, supg=null, g=null, subg=null;
					while(m.find()) {
						switch (i) {
						case 1 : {soa_code = m.group(1);break;}
						case 2 : {soa_name = m.group(1);break;}
						case 5 : {supg = m.group(1);break;}
						case 6 : {g = m.group(1);break;}
						case 7 : {subg = m.group(1);break;}
						default :break;
						}
						i++;
					}
					if(i>=7) {
						Tuple2<String,String> k = new Tuple2<String, String>(soa_code,soa_name);
						Tuple3<String, String, String> v = new Tuple3<String, String, String>(supg, g, subg);
						tupla = new Tuple2<Tuple2<String,String>, Tuple3<String,String,String>>(k, v);
					}
				}
				return tupla;
			}
		});
		//mapping di crimes
		JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> street_couples;
		street_couples = street.mapToPair(new PairFunction<String, Tuple2<String, String>, Tuple2<String, String>>(){
			@Override
			public Tuple2<Tuple2<String, String>, Tuple2<String, String>> call(String line) throws Exception {
				Tuple2<Tuple2<String,String>, Tuple2<String,String>> tupla = new Tuple2<Tuple2<String,String>, Tuple2<String,String>>(new Tuple2<String, String>("", ""), new Tuple2<String, String>("", ""));
				if(!(line==null || line.isEmpty() || line.length()==0)){
					String[] tokenizer = line.split(",");
					if(tokenizer.length>=10){
						String soa_code = tokenizer[7];
						String soa_name = tokenizer[8];
						String crime = tokenizer[9];
						String outcome = tokenizer[10];
						Tuple2<String, String> k = new Tuple2<String, String>(soa_code, soa_name);
						Tuple2<String, String> v = new Tuple2<String, String>(crime, outcome);
						tupla = new Tuple2<Tuple2<String,String>, Tuple2<String,String>>(k,v); 
					}
				}
				return tupla;
			}
		});
		//join
		JavaPairRDD<Tuple2<String,String>, Tuple5<String,String,String,String,String>> joined;
		joined = SOA_sg_couples.join(street_couples).mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Tuple2<Tuple3<String,String,String>,Tuple2<String,String>>>, Tuple2<String,String>, Tuple5<String,String,String,String,String>>() {
			@Override
			public Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> call(
					Tuple2<Tuple2<String, String>, Tuple2<Tuple3<String, String, String>, Tuple2<String, String>>> t)
					throws Exception {
				Tuple2<String, String> k = t._1;
				Tuple5<String,String,String,String,String> v = new Tuple5<String, String, String, String, String>(t._2._1._1(), t._2._1._2(), t._2._1._3(), t._2._2._1(), t._2._2._2());
				return new Tuple2<Tuple2<String,String>, Tuple5<String,String,String,String,String>>(k, v);
			}
		});
		//flatted
		JavaRDD<String> flatted;
		flatted = joined.flatMap(new FlatMapFunction<Tuple2<Tuple2<String,String>,Tuple5<String,String,String,String,String>>, String>() {
			@Override
			public Iterable<String> call(
					Tuple2<Tuple2<String, String>, Tuple5<String, String, String, String, String>> t) throws Exception {
				LinkedList<String> list = new LinkedList<String>();
				LinkedList<String> ans = new LinkedList<String>();
				String comp ="";
				list.addAll(Arrays.asList(t.toString().replaceAll("[()]", "").split(",")));
				int i;
				for(i=0; i<list.size()-1; i++)
					comp+=list.get(i)+", ";
				comp+=list.get(i);
				ans.add(comp);
				return ans;
			}
		});	
		flatted.saveAsTextFile(path_to_output_dir);
	}
}