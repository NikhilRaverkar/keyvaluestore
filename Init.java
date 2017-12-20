

public class Init {
	static Boolean mechanism;

	public static void main(String args[]) {
		if(args[3].contains("readrepair")|| args[3].replaceAll("\\s+","").equalsIgnoreCase("readrepair")||args[3].contains("read")) {
			mechanism=true;
			System.out.println("Read Repair");
		}else {
			mechanism=false;
		} 
		InitializeNode init= new InitializeNode(args[0],Integer.valueOf(args[1]),args[2],mechanism);
		init.start();
		
	}
	
	
	
}
