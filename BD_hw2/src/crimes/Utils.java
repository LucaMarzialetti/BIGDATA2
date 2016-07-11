package crimes;

import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.Comparator;

public class Utils {

	//tuple comparator for crimes dataset
	public static Comparator<Tuple3<String,String,Integer>> crime_frequency_comparator = 
			new Comparator<Tuple3<String,String,Integer>>() {
				public int compare(Tuple3<String,String,Integer> tupleA, Tuple3<String,String,Integer> tupleB) {
			return tupleB._3().compareTo(tupleA._3());
		}
	};

	//tuple comparator for stop&search dataset (racism analysis)
	public static Comparator<Tuple4<String,String,String,Integer>> ss_racism_frequency_comparator =
			new Comparator<Tuple4<String,String,String,Integer>>() {
				public int compare(Tuple4<String,String,String,Integer> tupleA, Tuple4<String,String,String,Integer> tupleB) {
			return tupleB._4().compareTo(tupleA._4());
		}
	};

	//tuple comparator for stop&search dataset (sexism analysis)
	public static Comparator<Tuple5<String, String, String, String, Integer>> ss_sexism_frequency_comparator =
			new Comparator<Tuple5<String, String, String, String, Integer>>() {
				public int compare(Tuple5<String, String, String, String, Integer> tupleA, Tuple5<String, String, String, String, Integer> tupleB) {
			return tupleB._5().compareTo(tupleA._5());
		}
	};
}//end class


