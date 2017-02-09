import java.io.FileWriter;
import java.util.Random;

public class CreateCustomers {
	private static void CreateDataSet() {
		int ID=0;
		String name=null;
		int age=0;
		int coutryCode=0;
		float salary=0;
		int nameLength=10;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("customers.txt"); 
			 while(ID<50000){
				 ID++;
				 nameLength=new Random().nextInt(10)+10;
				 name=getRandomString(nameLength);
				 age=new Random().nextInt(60)+10;
				 coutryCode=new Random().nextInt(9)+1;
				 salary=new Random().nextFloat()*9900+100;
				 
				 lineRecordString=String.valueOf(ID)+","+name+","+String.valueOf(age)+","+String.valueOf(coutryCode)+","+String.valueOf(salary)+"\r\n";
				 fw.write(lineRecordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }
		
	public static String getRandomString(int length) { 
	    String base = "abcdefghijklmnopqrstuvwxyz";   
	    Random random = new Random();   
	    StringBuffer sb = new StringBuffer();   
	    for (int i = 0; i < length; i++) {   
	        int number = random.nextInt(base.length());   
	        sb.append(base.charAt(number));   
	    }   
	    return sb.toString();   
	 }  

	/**
	 * @param args
	 */
	/*public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateDataSet();
	}
*/

}
