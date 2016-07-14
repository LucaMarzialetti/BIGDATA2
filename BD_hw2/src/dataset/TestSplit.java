package dataset;

import java.util.regex.Pattern;

public class TestSplit {
	public static void main(String[] args) {
//		String str = "2016-01-02T16:45:00+00:00";
//		//		Pattern p = Pattern.compile("([^,]*)");
//		//		Matcher m = p.matcher(str);
//		//		int i=1;
//		//		while(m.find()){
//		//			System.out.println(i+":\""+m.group(1)+"\"");
//		//			i++;
//		//		}
//		String[] y_a_h = str.split("T", -1);
//		for(int i=0; i<y_a_h.length; i++){
//			String[] m_a_s = y_a_h[i].split(":", -1);
//			for(int y=0; y<m_a_s.length; y++)
//				System.out.println(m_a_s[y]);
//		}
		
		String toreplace = "[Controlled drugs,Nothing found - no further action,,False,4, Offensive weapons,Suspect arrested,,False,2, Offensive weapons,Nothing found - no further action,,False,1, Controlled drugs,Local resolution,,False,1, Offensive weapons,Article found - Detailed outcome unavailable,,False,1, Article for use in theft,Nothing found - no further action,,False,1]";
		String replaced = toreplace.replaceAll("[\\[\\]]", "");
		String[]tokens = replaced.split(", ",-1);
		for(int i=0; i<tokens.length; i++)
			System.out.println(tokens[i]);

	}
}
