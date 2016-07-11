package crimes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

// Import classes for MLLib
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

@SuppressWarnings("unused")
public class MachineLearning {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Machine Learning <input_dataset_txt> <output>");
			System.exit(1);
		}

		path_to_dataset=args[0];
		path_to_output_dir=args[1];
		decision_tree();
	}//end main

	// Load the data from the csv file and return an RDD 
	public static JavaRDD<String> loadData() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("MLearning");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("MBA.jar");
		JavaRDD<String>  one_year_input_csv = sc.textFile(path_to_dataset);
		return  one_year_input_csv;
	}

	@SuppressWarnings("serial")
	public static void decision_tree() {

		//carica il file csv
		JavaRDD<String> input_csv = loadData();
		
		//crea un RDD di oggetti di tipo Flight
		JavaRDD<Crime> crimes = input_csv.map(
				new Function<String, Crime>() {
					public Crime call(String line) throws Exception {
						StringTokenizer st=new StringTokenizer(line,",");
						Crime c = new Crime();
						c.setCrime_type(st.nextToken());
						c.setGroup(st.nextToken());
						c.setLat(st.nextToken());
						c.setLocation(st.nextToken());
						c.setLon(st.nextToken());
						c.setLsoa_code(st.nextToken());
						c.setMonth(st.nextToken());
						c.setOutcome(st.nextToken());
						c.setSub_group(st.nextToken());
						c.setSuper_group(st.nextToken());
						return c;
					}
				});
		
		//conversione della feature non numerica "carrier" in numerica
		Map<String,Integer> crime_type_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.crime_type)
		.collect()
		.forEach( 
				crime_type -> { 	Integer int_code=0; 
									if(!crime_type_to_int_map.containsKey(crime_type)){
										crime_type_to_int_map.put(crime_type, int_code); int_code=int_code+1;} });
		
		Map<String,Integer> group_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.group)
		.collect()
		.forEach( 
				group -> { 	Integer int_code=0; 
									if(!group_to_int_map.containsKey(group)){
										group_to_int_map.put(group, int_code); int_code=int_code+1;} });
		
		Map<String,Integer> lat_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.lat)
		.collect()
		.forEach( 
				lat -> { 	Integer int_code=0; 
									if(!lat_to_int_map.containsKey(lat)){
										lat_to_int_map.put(lat, int_code); int_code=int_code+1;} });
		
		Map<String,Integer> loc_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.location)
		.collect()
		.forEach( 
				loc -> { 	Integer int_code=0; 
									if(!loc_to_int_map.containsKey(loc)){
										loc_to_int_map.put(loc, int_code); int_code=int_code+1;} });

		Map<String,Integer> lon_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.lon)
		.collect()
		.forEach( 
				lon -> { 	Integer int_code=0; 
									if(!lon_to_int_map.containsKey(lon)){
										lon_to_int_map.put(lon, int_code); int_code=int_code+1;} });

		Map<String,Integer> lsoa_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.lsoa_code)
		.collect()
		.forEach( 
				lsoa -> { 	Integer int_code=0; 
									if(!lsoa_to_int_map.containsKey(lsoa)){
										lsoa_to_int_map.put(lsoa, int_code); int_code=int_code+1;} });

		Map<String,Integer> month_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.month)
		.collect()
		.forEach( 
				month -> { 	Integer int_code=0; 
									if(!month_to_int_map.containsKey(month)){
										month_to_int_map.put(month, int_code); int_code=int_code+1;} });

		Map<String,Integer> outcome_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.outcome)
		.collect()
		.forEach( 
				outcome -> { 	Integer int_code=0; 
									if(!outcome_to_int_map.containsKey(outcome)){
										outcome_to_int_map.put(outcome, int_code); int_code=int_code+1;} });

		Map<String,Integer> subgroup_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.sub_group)
		.collect()
		.forEach( 
				sgroup -> { 	Integer int_code=0; 
									if(!subgroup_to_int_map.containsKey(sgroup)){
										subgroup_to_int_map.put(sgroup, int_code); int_code=int_code+1;} });

		Map<String,Integer> supgroup_to_int_map= new HashMap<String, Integer>();
		crimes.map(crime -> crime.super_group)
		.collect()
		.forEach( 
				supgroup -> { 	Integer int_code=0; 
									if(!supgroup_to_int_map.containsKey(supgroup)){
										supgroup_to_int_map.put(supgroup, int_code); int_code=int_code+1;} });

		//creazione RDD di LabeledPoint = etichetta + feature vector
		JavaRDD<LabeledPoint> labeled_points = crimes.map(
				new Function<Crime, LabeledPoint>() {
					public LabeledPoint call(Crime crime) throws Exception {
								double outcome_label;
								if(crime.getOutcome().equals("Investigation complete; no suspect identified")) {outcome_label=0.0;} 
								else {outcome_label=1.0;}
								return new LabeledPoint(outcome_label,
												Vectors.dense(	(double)crime_type_to_int_map.get(crime.crime_type),
																(double)group_to_int_map.get(crime.group),
																(double)lat_to_int_map.get(crime.lat),
																(double)loc_to_int_map.get(crime.location),
																(double)lon_to_int_map.get(crime.lon),
																(double)lsoa_to_int_map.get(crime.lsoa_code),
																(double)month_to_int_map.get(crime.month),
																(double)subgroup_to_int_map.get(crime.sub_group),
																(double)supgroup_to_int_map.get(crime.super_group))
								);}//end call
				});//end map
		
		
		/*
		 * a cosa serve questo passaggio?
		 */
		double[] weights={0.85, 0.15};
		//splitta l'rdd, in modo da ottenerne due: il primo contiene 85% dei valori che soddisfano la condizione ,il secondo la restante percentuale
		JavaRDD<LabeledPoint> not_solved=labeled_points.filter(labeled_point -> labeled_point.label () == 0.0).randomSplit(weights)[0];
		JavaRDD<LabeledPoint> solved=labeled_points.filter(labeled_point -> labeled_point.label () != 0.0);
		JavaRDD<LabeledPoint> full_set=not_solved.union(solved);
		
		/*
		 * Nota: usare un anno per fare training e un anno successivo per fare testing
		 */
		//crea test set e training set randomicamente
		double[] test_train_weights={0.7, 0.3};
		JavaRDD<LabeledPoint>[] splits = full_set.randomSplit(test_train_weights);
		JavaRDD<LabeledPoint> training_set = splits[0];
		JavaRDD<LabeledPoint> test_set = splits[1];

		//setta parametri dell'albero di decisione
		//classi possibili per l'etichetta_ solved o not_solved
		Integer num_classes = 2;
		
		//range dei valori della feature alla posizione indicata dalla chiave
		Map<Integer, Integer> categorical_features_info = new HashMap<Integer, Integer>();
		//categorical_features_info.put(0, 31);								//feature in posizione 0=dayofmonth, valori da 0 a 30 
		//categorical_features_info.put(1, 7);								//feature in posizione 1=dayofweek, valori da 0 a 30
		categorical_features_info.put(0, group_to_int_map.size());
		categorical_features_info.put(1, lat_to_int_map.size());
		categorical_features_info.put(2, loc_to_int_map.size());
		categorical_features_info.put(3, lon_to_int_map.size());
		categorical_features_info.put(4, lsoa_to_int_map.size());
		categorical_features_info.put(5, month_to_int_map.size());
		categorical_features_info.put(6, subgroup_to_int_map.size());
		categorical_features_info.put(7, supgroup_to_int_map.size());

		//algoritmo per il calcolo dell impurità accettabile per una partizione delle features, quando si deve costruire l'albero
		String impurity_metric = "gini";
		//+ profondo->più preciso MA rischio overfitting
		Integer max_depth = 9;
		//numero massimo di intervalli in cui dividere (discretizzare) i dati continui
		Integer max_discretization_bins = 7000;
		
		//costruisce l'albero di decisione con i parametri settati
		DecisionTreeModel decision_tree_model = DecisionTree.trainClassifier(training_set, num_classes, categorical_features_info, impurity_metric, max_depth, max_discretization_bins);
		
		//applica l'albero costruito al test set, per assegnare un valore alla label
		JavaPairRDD<Double, Double> predictions_on_test_set =
				  test_set.mapToPair(p -> new Tuple2<Double, Double> (decision_tree_model.predict(p.features()), p.label()));

		//calcola l'errore di predizione, come numero di predizioni corrette sul totale (percentuale)
		Double prediction_error_value =
				  1.0 * predictions_on_test_set.filter(
						  new Function<Tuple2<Double, Double>, Boolean>() {
							  @Override
							  public Boolean call(Tuple2<Double, Double> t) {
								  return !t._1().equals(t._2());
							  }
				  }).count() / test_set.count();

		// Save model
		decision_tree_model.save(sc.sc(), path_to_output_dir);
				
	
	}//end decision_tree
}