package job;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestSplit {
	public static void main(String[] args) {
		String line = "\"pippo\",\"pippa\"";
		Pattern p = Pattern.compile("\"([^\"]*)\"");
		Matcher m = p.matcher(line);
		int i = 1;
		while(m.find()){
			System.out.println(i+" "+m.group(1));
			i++;
		}
	}
}
