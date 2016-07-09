package job;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple10;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple8;

public class OA_to_SOA {

	private static String path_to_dataset1;
	private static String path_to_dataset2;
	private static String path_to_output_dir1;
	private static String path_to_output_dir2;
	private static String path_to_output_dir3;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		//TODO: check args
		//args[0]=input
		//args[1]=input
		//ags[2]=ouput
		path_to_dataset1=args[0];
		path_to_dataset2=args[1];
		path_to_output_dir1=args[2];
		path_to_output_dir2=args[3];
		path_to_output_dir3=args[4];
		String appName = "OA_to_SOA";
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
		JavaRDD<String> OA_class_text = loadData(path_to_dataset1);
		JavaRDD<String> OA_to_SOA_text = loadData(path_to_dataset2);
		// mappa il OA_class in <String,String>
		JavaPairRDD<String,Tuple8<String, String, String, String, String, String, String, String>> OA_class_couples;
		OA_class_couples = OA_class_text.mapToPair(
				new PairFunction <String, String, Tuple8<String, String, String, String, String, String, String, String>>() {
					public Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>> call(String line) {
						Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>> tupla = new Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>>("", new Tuple8<String, String, String, String, String, String, String, String>("", "", "", "", "", "", "", ""));
						if(!(line==null || line.isEmpty() || line.length()==0)){
							String[] tokenizer = line.split(",");
							if(tokenizer.length>6){
								String oa = tokenizer[0];
								String country_code = tokenizer[3];
								String country_name = tokenizer[4];
								String sgroup_code = tokenizer[5];
								String sgroup_name = tokenizer[6];
								String group_code = tokenizer[7];
								String group_name = tokenizer[8];
								String lgroup_code = tokenizer[9];
								String lgroup_name = tokenizer[10];
								Tuple8<String, String, String, String, String, String, String, String> val = new Tuple8<String, String, String, String, String, String, String, String>(country_code, country_name, sgroup_code, sgroup_name, group_code, group_name, lgroup_code, lgroup_name);
								tupla = new Tuple2<String, Tuple8<String, String, String, String, String, String, String, String>>(oa, val); 
							}
						}
						return tupla;
					}
				});
		// mappa il OA_to_SOA in <String,String>
		JavaPairRDD<String,Tuple2<String, String>> OA_to_SOA_couples;
		OA_to_SOA_couples = OA_to_SOA_text.mapToPair(
				new PairFunction <String, String, Tuple2<String,String>>() {
					public Tuple2<String, Tuple2<String,String>> call(String line) {
						Tuple2<String,Tuple2<String,String>> tupla = new Tuple2<String,Tuple2<String,String>>("", new Tuple2<String, String>("", ""));
						if(!(line==null || line.isEmpty() || line.length()==0)){
							//separatore tra double quotes
							//tutto quello che c'è tra due double quotes consecutive
							Pattern p = Pattern.compile("\"([^\"]*)\"");
							Matcher m = p.matcher(line);
							int i = 1;
							String oa=null,soa_code=null,soa_name =null;
							while(m.find()){
								switch (i){
								case 3 : {oa = m.group(1);break;}
								case 4 : {soa_code = m.group(1);break;}
								case 5 : {soa_name = m.group(1);break;}
								default :break;
								}
								i++;
							}
							if(i>=5)
							{
								Tuple2<String,String> val = new Tuple2<String, String>(soa_code, soa_name);
								tupla = new Tuple2<String,Tuple2<String,String>>(oa, val);
							}
						}
						return tupla;
					}
				}).distinct();
		/**fa il join sulla chiave**/
		JavaPairRDD<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> joined;
		joined = OA_class_couples.join(OA_to_SOA_couples).mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple8<String,String,String,String,String,String,String,String>,Tuple2<String,String>>>, String, Tuple10<String, String, String, String, String, String, String, String, String, String>>() {
			@Override
			public Tuple2<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> call(
					Tuple2<String, Tuple2<Tuple8<String, String, String, String, String, String, String, String>, Tuple2<String, String>>> t)
							throws Exception {
				String k = t._1;
				Tuple8<String, String, String, String, String, String, String, String> eight = t._2._1;
				Tuple2<String, String> two = t._2._2;
				Tuple10<String, String, String, String, String, String, String, String, String, String> v = new Tuple10<String, String, String, String, String, String, String, String, String, String>(eight._1(), eight._2(), eight._3(), eight._4(), eight._5(), eight._6(), eight._7(), eight._8(), two._1(), two._2()); 
				return new Tuple2<String, Tuple10<String,String,String,String,String,String,String,String,String,String>>(k, v);
			}
		});
		//remove others
		JavaPairRDD<String, Tuple4<String,String,String,String>> invarianti;
		invarianti = joined.mapToPair(new PairFunction<Tuple2<String,Tuple10<String,String,String,String,String,String,String,String,String,String>>, String, Tuple4<String,String,String,String>>() {
			@Override
			public Tuple2<String, Tuple4<String, String, String, String>> call(
					Tuple2<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> t)
					throws Exception {
				String k = t._1;
				Tuple4<String, String, String, String> v = new Tuple4<String, String, String, String>(t._2._1(), t._2._2(), t._2._9(), t._2._10());
				return new Tuple2<String, Tuple4<String,String,String,String>>(k, v);
			}
		});

		/***CALCULTATING GROUPINGS***/
		//super groups for OA
		JavaPairRDD<Tuple2<String, String>, String> oa_SG;
		oa_SG = joined.mapToPair(new PairFunction<Tuple2<String,Tuple10<String,String,String,String,String,String,String,String,String,String>>, Tuple2<String, String>, String>() {
			@Override
			public Tuple2<Tuple2<String, String>, String> call(
					Tuple2<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> t)
							throws Exception {
				Tuple2<String, String> k = new Tuple2<String, String>(t._1(), t._2._3()); 
				String v = t._2._4();
				return new Tuple2<Tuple2<String,String>, String>(k,v);
			}
		});
		//groups for OA
		JavaPairRDD<Tuple2<String, String>, String> oa_G;
		oa_G = joined.mapToPair(new PairFunction<Tuple2<String,Tuple10<String,String,String,String,String,String,String,String,String,String>>, Tuple2<String, String>, String>() {
			@Override
			public Tuple2<Tuple2<String, String>, String> call(
					Tuple2<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> t)
							throws Exception {
				Tuple2<String, String> k = new Tuple2<String, String>(t._1(), t._2._5()); 
				String v = t._2._6();
				return new Tuple2<Tuple2<String,String>, String>(k,v);
			}
		});
		//sub groups for OA
		JavaPairRDD<Tuple2<String, String>, String> oa_SubG;
		oa_SubG = joined.mapToPair(new PairFunction<Tuple2<String,Tuple10<String,String,String,String,String,String,String,String,String,String>>, Tuple2<String, String>, String>() {
			@Override
			public Tuple2<Tuple2<String, String>, String> call(
					Tuple2<String, Tuple10<String, String, String, String, String, String, String, String, String, String>> t)
							throws Exception {
				Tuple2<String, String> k = new Tuple2<String, String>(t._1(), t._2._7()); 
				String v = t._2._8();
				return new Tuple2<Tuple2<String,String>, String>(k,v);
			}
		});
		/**PROJECTION**/
		//super groups
		JavaPairRDD<String, String> SG;
		SG = oa_SG.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, String> t) throws Exception {
				return new Tuple2<String, String>(t._1._1, t._2);
			}
		});
		//groups
		JavaPairRDD<String, String> G;
		G = oa_G.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, String> t) throws Exception {
				return new Tuple2<String, String>(t._1._1, t._2);
			}
		});
		//sub groups
		JavaPairRDD<String, String> SubG;
		SubG = oa_SubG.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<Tuple2<String, String>, String> t) throws Exception {
				return new Tuple2<String, String>(t._1._1, t._2);
			}
		});
		/***JOIN BACK***/
		JavaPairRDD<String, Tuple5<String, String, String, String, String>> back;
		


		/**flat finale delle tuple**/
		//		JavaRDD<String> flatted;
		//		flatted = joined.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<Tuple8<String, String, String, String, String, String, String, String>,Tuple2<String,String>>>, String>() {
		//			@Override
		//			public Iterable<String> call(Tuple2<String, Tuple2<Tuple8<String, String, String, String, String, String, String, String>, Tuple2<String, String>>> t)
		//					throws Exception {
		//				LinkedList<String> list = new LinkedList<String>();
		//				LinkedList<String> ans = new LinkedList<String>();
		//				String comp ="";
		//				list.addAll(Arrays.asList(t.toString().replaceAll("[()]", "").split(",")));
		//				int i;
		//				for(i=0; i<list.size()-1; i++)
		//					comp+="\""+list.get(i)+"\", ";
		//				comp+="\""+list.get(i)+"\"";
		//				ans.add(comp);
		//				return ans;
		//			}
		//		});

		//OUTPUT oa_categorie
		OA_class_couples.saveAsTextFile(path_to_output_dir1);
		//OUTPUT OA ed SOA
		OA_to_SOA_couples.saveAsTextFile(path_to_output_dir2);
		//OUTPUT join flatted, unique, LISTA
		//		flatted.saveAsTextFile(path_to_output_dir3);

		//		OA_to_SOA_couples.saveAsTextFile(path_to_output_dir);
		//		OA_class_couples.saveAsTextFile(path_to_output_dir);

		//		//reduce in <<DATA,ITEM>,COUNT_PER_ITEM>  
		//		JavaPairRDD<Tuple2<String,String>, Integer> date_item_couples_reduced = 
		//				date_item_couples.reduceByKey(new Function2<Integer, Integer, Integer>() {
		//					public Integer call(Integer t1, Integer t2) {return t1+t2;}
		//				});
		//
		//
		//		//cambia formato da <<DATA,ITEM>,COUNT_PER_ITEM> a <DATA, <ITEM,COUNT_PER_ITEM>> 
		//		JavaPairRDD<String,Tuple2<String,Integer>> date_as_key =
		//				date_item_couples_reduced.mapToPair(new PairFunction< Tuple2< Tuple2<String,String>,Integer>, String, Tuple2<String,Integer>>() {
		//					public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> arg0)	throws Exception {
		//						return new Tuple2<String, Tuple2<String, Integer>>( arg0._1._1, new Tuple2<String,Integer>(arg0._1._2,arg0._2));
		//					}
		//				});
		//
		//		//raggruppa per chiave in <DATA, LIST[<ITEM,COUNT_PER_ITEM>]>
		//		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> data_as_key_grouped = date_as_key.groupByKey();
		//		data_as_key_grouped.cache();
		//
		//		//ordinamento item
		//		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> ordered_items =
		//				data_as_key_grouped.mapValues(
		//						new Function<Iterable<Tuple2<String,Integer>>,Iterable<Tuple2<String,Integer>>>() {
		//							public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> arg0)throws Exception {
		//								List<Tuple2<String, Integer>> sortedList = new ArrayList<Tuple2<String, Integer>>();
		//								int cont=0;
		//								for (Tuple2<String, Integer> t : arg0) {
		//									if(cont==5)
		//										break;
		//									sortedList.add(t);
		//									cont=cont+1;
		//								}
		//								Collections.sort(sortedList,Utils.item_number_comparator);
		//								return sortedList;
		//							}
		//						});
		//
		//		ordered_items.cache();
		//		ordered_items.saveAsTextFile(path_to_output_dir);
		//		List<Tuple2<String,Iterable<Tuple2<String, Integer>>>> result=ordered_items.collect();
		//		//chiude il contesto
		sc.close();
	}
}//end App