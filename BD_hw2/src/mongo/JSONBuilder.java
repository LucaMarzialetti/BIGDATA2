package mongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class JSONBuilder {

	//1			2		3		4				5			6				7				8
	//gender 	age		hour	[obj_of_search	outcome		outcome_linked	cloth_removal	freq]
	public static JSONArray buildStopSearchSex(String path){
		JSONArray ja = new JSONArray();
		BufferedReader br = null;
		String line = null;
		try{
			br = new BufferedReader(new FileReader(path));
			while ((line = br.readLine()) != null) {
				JSONObject o = new JSONObject();
				try {
					String[] fields = line.split(",",-1);
					o.put("gender", fields[0]);
					o.put("age", fields[1]);
					o.put("hour", fields[2]);
					String recombine ="";
					int i;
					for(i=3; i<fields.length-1; i++)
						recombine+=fields[i]+",";
					recombine+=fields[i];
					recombine = recombine.replaceAll("[\\[\\]]", "");
					String[] reports = recombine.split(", ",-1);
					JSONArray report_array = new JSONArray();
					for(i=0; i<reports.length; i++){
						JSONObject report = new JSONObject();
						String[] report_line = reports[i].split(",",-1);
						report.put("obj_of_search", report_line[0]);
						report.put("outcome", report_line[1]);
						report.put("outcome_linked", report_line[2]);
						report.put("cloath_removal", report_line[3]);
						report.put("freq", report_line[4]);
						report_array.put(report);
					}
					o.put("reports", report_array);
					ja.put(o);
				} 
				catch (JSONException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (br != null) {
				try {
					br.close();
				}
				catch (IOException e) {
					System.out.println(e.getMessage());		
				}
			}
		}
		return ja;
	}

	//1				2		3				4			5				6
	//ethnicity 	age		[obj_of_search	outcome		outcome_linked	freq]
	public static JSONArray buildStopSearchRace(String path){
		JSONArray ja = new JSONArray();
		BufferedReader br = null;
		String line = null;
		try{
			br = new BufferedReader(new FileReader(path));
			while ((line = br.readLine()) != null) {
				JSONObject o = new JSONObject();
				try {
					String[] fields = line.split(",",-1);
					o.put("ethnicity", fields[0]);
					o.put("age", fields[1]);
					String recombine ="";
					int i;
					for(i=2; i<fields.length-1; i++)
						recombine+=fields[i]+",";
					recombine+=fields[i];
					recombine = recombine.replaceAll("[\\[\\]]", "");
					String[] reports = recombine.split(", ",-1);
					JSONArray report_array = new JSONArray();
					for(i=0; i<reports.length; i++){
						JSONObject report = new JSONObject();
						String[] report_line = reports[i].split(",",-1);
						report.put("obj_of_search", report_line[0]);
						report.put("outcome", report_line[1]);
						report.put("outcome_linked", report_line[2]);
						report.put("freq", report_line[3]);
						report_array.put(report);
					}
					o.put("reports", report_array);
					ja.put(o);
				} 
				catch (JSONException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (br != null) {
				try {
					br.close();
				}
				catch (IOException e) {
					System.out.println(e.getMessage());		
				}
			}
		}
		return ja;
	}

	//1				2			3				4		5			6				7			8
	//lsoa_code		lsoa_name	supergroup		group	subgroup	[crime_type 	outcome 	freq]
	public static JSONArray buildStreet(String path){
		JSONArray ja = new JSONArray();
		BufferedReader br = null;
		String line = null;
		try{
			br = new BufferedReader(new FileReader(path));
			while ((line = br.readLine()) != null) {
				JSONObject o = new JSONObject();
				try {
					String[] fields = line.split(",",-1);
					o.put("lsoa_code", fields[0]);
					o.put("lsoa_name", fields[1]);
					o.put("super_group", fields[2]);
					o.put("group", fields[3]);
					o.put("sub_group", fields[4]);
					String recombine ="";
					int i;
					for(i=5; i<fields.length-1; i++)
						recombine+=fields[i]+",";
					recombine+=fields[i];
					recombine = recombine.replaceAll("[\\[\\]]", "");
					String[] reports = recombine.split(", ",-1);
					JSONArray report_array = new JSONArray();
					for(i=0; i<reports.length; i++){
						JSONObject report = new JSONObject();
						String[] report_line = reports[i].split(",",-1);
						report.put("crime_type", report_line[0]);
						report.put("outcome", report_line[1]);
						report.put("freq", report_line[2]);
						report_array.put(report);
					}
					o.put("reports", report_array);
					ja.put(o);
				} 
				catch (JSONException e) {
					System.out.println(e.getMessage());
				}
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (br != null) {
				try {
					br.close();
				}
				catch (IOException e) {
					System.out.println(e.getMessage());		
				}
			}
		}
		return ja;
	}
}
