package mongo;

public class BuilderMongoDB {
	public static final String mongoHost = "localhost";
	public static final int mongoPort = 27017;
	public static final String mongoClint = "bigdata";
	/*collections*/
	public static final String ssSexCollection = "stop_search_sexism";
	public static final String ssRaceCollection = "stop_search_racism";
	public static final String streetCollection = "street";
	/*paths*/
	public static final String ssSexResultPath = "results/StopSearchSexResults/part-00000";
	public static final String ssRaceResultPath = "results/StopSearchRaceResults/part-00000";
	public static final String streetResultPath = "results/StreetResults/part-00000";
	
	public static void main(String[] args) {
		MongoConnector connector = new MongoConnector(mongoHost, mongoPort, mongoClint);
//		connector.showCollection(ssSexCollection);
		connector.showCollection(ssRaceCollection);
//		connector.showCollection(streetCollection);
//		System.out.println("ADDING STOP_SEARCH SEX RESULT");
//		connector.bulkAdd(JSONBuilder.buildStopSearchSex(ssSexResultPath), ssSexCollection);
		System.out.println("ADDING STOP_SEARCH RACE RESULT");
		connector.bulkAdd(JSONBuilder.buildStopSearchRace(ssRaceResultPath), ssRaceCollection);
//		System.out.println("ADDING STREET RESULT");
//		connector.bulkAdd(JSONBuilder.buildStreet(streetResultPath), streetCollection);
	}
}
