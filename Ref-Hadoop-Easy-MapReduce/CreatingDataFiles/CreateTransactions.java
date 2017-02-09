import java.io.FileWriter;
import java.util.Random;

public class CreateTransactions {
	private static void CreateDataSet() {
		long transID=0;
		int custID=0;
		float transTotal=0;		
		int transNumItems=0;
		String transDesc=null;
		int transDescLength=10;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("transactions.txt"); 
			 while(transID<5000000){
				 transID++;
				 custID=new Random().nextInt(49999)+1;
				 transTotal=new Random().nextFloat()*990+10;
				 transNumItems=new Random().nextInt(9)+1;
				 transDescLength=new Random().nextInt(30)+20;
				 transDesc=getRandomString(transDescLength);
				 
				 lineRecordString=String.valueOf(transID)+","+String.valueOf(custID)+","+String.valueOf(transTotal)+","+String.valueOf(transNumItems)+","+transDesc+"\r\n";
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
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateDataSet();
	}

}
