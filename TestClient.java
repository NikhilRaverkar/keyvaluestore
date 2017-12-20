

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;



public class TestClient {
	
	static NodeData coordinator_node;;
	static int key;
	static String value;
	static String consistency;
	
	public static void main(String args[]) {
		try {
		Scanner sc = new Scanner(new File(args[0]));
		int i=1;
		Map<Integer, NodeData>available_nodes = new HashMap<>();
		while(sc.hasNext()){
			String[] data= sc.nextLine().split(" ");
			NodeData node = new NodeData(data[0],data[1],Integer.valueOf(data[2]),null);
			available_nodes.put(i,node);
			i++;
			
			 
		} 
		Socket sock = null;
		while(null==sock) {
		int nodeid=ThreadLocalRandom.current().nextInt(1, 5);
		coordinator_node = available_nodes.get(nodeid);
		//System.out.println(nodeid);
		try {
		sock = new Socket(coordinator_node.getIp(),coordinator_node.getPort());
		}catch(Exception e) {
			continue;
		}
		coordinator_node.setSocket(sock);
		System.out.println("Seleted Random:"+coordinator_node.getName());
			
		}
			

String choice = null;
do {
	System.out.println("Select an Operation");
	System.out.println("1. Put Key");
	System.out.println("2. Get Key");
	System.out.println("3. Exit");
	Scanner scan = new Scanner(System.in);
	choice= scan.nextLine();
	switch(choice) {
	
	case "1":
		key=0;
		value="";
		consistency="";
		System.out.println("Insert a key");
		try {
		key= Integer.valueOf(scan.nextLine());
		while(key <0 || key >255) {
			System.out.println("Insert a correct key");
			key= Integer.valueOf(scan.nextLine());
		}
		}catch(NumberFormatException e) {
			System.out.println("Incorrect type key insterted... Exiting a program");
			System.exit(0);
		}
		System.out.println("insert a value");
		value=scan.nextLine();
		
		do {
			System.out.println("Select a Consistency Level");
			System.out.println("1. One");
			System.out.println("2. Two");
			Scanner sc2= new Scanner(System.in);
			consistency = sc2.nextLine();
			switch(consistency) {
			case "1" :
				putkey(key, value, consistency);
			break;
			case "2":
				putkey(key, value, consistency);
				break;
				
			}
			
		}while(consistency.compareTo("1")!=0 && consistency.compareTo("2")!=0 );
		
		break;
	case "2":
		key=0;
		value="";
		consistency="";
		
		System.out.println("Insert a key");
		try {
		key= Integer.valueOf(scan.nextLine());
		while(key <0 || key >255) {
			System.out.println("Insert a correct key");
			key= Integer.valueOf(scan.nextLine());
		}
		}catch(NumberFormatException e) {
			System.out.println("Incorrect type key insterted... Exiting a program");
			System.exit(0);
		}

		
		do {
			System.out.println("Select a Consistency Level");
			System.out.println("1. One");
			System.out.println("2. Two");
			Scanner sc2= new Scanner(System.in);
			consistency = sc2.nextLine();
			switch(consistency) {
			case "1" :
				getkey(key,consistency);
			break;
			case "2":
				getkey(key,consistency);
				break;
				
			}
			
		}while(consistency.compareTo("1")!=0 && consistency.compareTo("2")!=0);
		
		break;
		
	case "3" :
		System.out.println("Closing a client");
		System.exit(0);
		break;
	
	default:
		System.out.println("Incorrect option selected");
		System.exit(0);
		break;
	}
	
	
	
}while (choice !="3");
			
		
		
		}catch(IOException e) {
			
		}
	}

	private static void getkey(int key2, String consistency2) {
		// TODO Auto-generated method stub
		
		try {
		Keyval.Getkey.Builder gettkey = Keyval.Getkey.newBuilder();
		Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
		gettkey.setKey(key2);
		
		if(consistency2.equals("1")) {
			gettkey.setConsistency(1);
		}else if(consistency2.equals("2")) {
			gettkey.setConsistency(2);
		}
		message.setGetKey(gettkey);
		Keyval.NodeMessage msg= message.build();
		msg.writeDelimitedTo(new DataOutputStream(coordinator_node.getSocket().getOutputStream()));
		
		DataInputStream input;
		while(true) {
			 input= new DataInputStream( coordinator_node.getSocket().getInputStream());
			if(input.available()>0) {
				break;
			}
		}
		
		Keyval.NodeMessage resp =  Keyval.NodeMessage.parseDelimitedFrom(input);
		Keyval.GetKeyResponse get_response = resp.getGetKeyResponse();
		System.out.println("KEY:"+get_response.getKey());
		System.out.println("VALUE:"+get_response.getValue());
		
		
		
		}catch(IOException e) {
			
		}
		
	}

	private static void putkey(int key2, String value2, String consistency2) {
		// TODO Auto-generated method stub
		try {
		Keyval.PutKey.Builder putkey = Keyval.PutKey.newBuilder();
		Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
		putkey.setKey(key2);
		putkey.setValue(value2);
		if(consistency2.equals("1")) {
		putkey.setConsistency(1);
		}else if(consistency2.equals("2")) {
			putkey.setConsistency(2);
		}
		message.setPutKey(putkey);
		Keyval.NodeMessage msg= message.build();
		msg.writeDelimitedTo(new DataOutputStream(coordinator_node.getSocket().getOutputStream()));
		
		DataInputStream input;
		while(true) {
			 input= new DataInputStream( coordinator_node.getSocket().getInputStream());
			if(input.available()>0) {
				break;
			}
		}
		
		Keyval.NodeMessage resp =  Keyval.NodeMessage.parseDelimitedFrom(input);
		Keyval.PutKeyResponse put_response = resp.getPutKeyResponse();
		
		if(put_response.getResponse()) {
			System.out.println("Key has been inserted");
		}else {
			System.out.println("Data insertion failed");
		}
		
		}catch(IOException e) {
			
		}
		
	}
	
	

}
