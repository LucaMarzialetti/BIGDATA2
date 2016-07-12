package dataset;

public class TestSplit {
	public static void main(String[] args) {
		String str = ",2010-12,Avon and Somerset Constabulary,Avon and Somerset Constabulary,-0.805600,51.819143,On or near Norfolk Terrace,E01017662,Aylesbury Vale 015C,Other crime,,";
//		Pattern p = Pattern.compile("([^,]*)");
//		Matcher m = p.matcher(str);
//		int i=1;
//		while(m.find()){
//			System.out.println(i+":\""+m.group(1)+"\"");
//			i++;
//		}
		String[] tok = str.split(",", -1);
		System.out.println(tok.length);
	}
}
