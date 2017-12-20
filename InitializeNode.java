

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

public class InitializeNode {

	private String fileName;
	private String nodeName;
	private int port;
	private String ip;
	//AtomicInteger reqCount = new AtomicInteger(0);
	Map<Integer,Integer> reqCount = Collections.synchronizedMap(new HashMap<>());
//	AtomicInteger respCount =new AtomicInteger(0);
	Map<Integer,Integer> respCount = Collections.synchronizedMap(new HashMap<>());
	Map<Integer,Integer> consistency_count = Collections.synchronizedMap(new HashMap<>());
	//AtomicInteger consistency_count=new AtomicInteger(0);
	AtomicInteger reqId = new AtomicInteger(0);
	Object lock = new Object();
	DateFormat dateModified;
	File file;
	BufferedWriter writer;
	Map<Integer, Data> memData = Collections.synchronizedMap(new HashMap<>());
	Map<String, Data> getRequest = Collections.synchronizedMap(new HashMap<>());
	Map<String, List<Integer>> hint = new HashMap<>();
	private Boolean mechanism;
	
	public String getIp() {
		return ip;
	}


	public void setIp(String ip) {
		this.ip = ip;
	}


	Map<String ,  NodeData> nodeMapping = new HashMap<>();
	public String getNodeName() {
		return nodeName;
	}


	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}


	public int getPort() {
		return port;
	}


	public void setPort(int port) {
		this.port = port;
	}



	public String getFileName() {
		return fileName;
	}


	public void setFileName(String fileName) {
		this.fileName = fileName;
	}


	public InitializeNode(String nodeName, int port, String fileName,Boolean mechanism) {
		this.setNodeName(nodeName);
		this.setPort(port);
		this.setFileName(fileName);  
		this.mechanism=mechanism;
		dateModified= new  SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z ");
		dateModified.setTimeZone(TimeZone.getTimeZone("GMT"));
		try {
			
			
			file = new File(nodeName+".txt");
			rebuildmap(file.getCanonicalPath());
			Scanner sc = new Scanner(new File(this.getFileName()));
			while(sc.hasNext()) {
				String line =sc.nextLine();
				if(null!=line.split(" ")[0]) {
					NodeData node_data= new NodeData(line.split(" ")[0],"",0,null);
					nodeMapping.put(line.split(" ")[0], null);
				}
				
			}
			
			
			
			nodeMapping.remove(nodeName);
			//FileHandler handler = new  FileHandler(nodeName+".txt",true);
			
			//SimpleFormatter format = new SimpleFormatter();
			//handler.setFormatter(new CustomFromatter());
			
		} catch (SecurityException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			this.setIp(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread th = new Thread() {
			public void run() {
				startserver(new InetSocketAddress(port));

			}
		};
		th.start();
		
		
		
		
		/*Thread hinted = new Thread() {
			
			public void run() {
				hintedHandoff();
				
			}
		};
		hinted.setDaemon(true);
		hinted.start();*/


	}
    protected synchronized void readrepair(Socket socket, Data data) {
    	System.out.println("sending read repair request to a server");
    	Keyval.WriteKeyRequest.Builder writekey = Keyval.WriteKeyRequest.newBuilder();
		Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
		writekey.setKey(data.getKey());
		writekey.setValue(memData.get(data.getKey()).getValue());
		writekey.setTimestamp(memData.get(data.getKey()).getTime());
		message.setWriteKeyRequest(writekey);
		Keyval.NodeMessage writeRreq= message.build();
		try {
			writeRreq.writeDelimitedTo(new DataOutputStream(socket.getOutputStream()));
		}catch(IOException e) {
			
		}
    	
    	
    	
    	
    	
    	
    }
	protected synchronized void hintedHandoff(Socket sock ,String nodeName) {
		
		System.out.println("Sending a hint using hinted handoff");
		if(hint.size()>0) {
		for(Entry data:hint.entrySet()) {
			
			if(data.getKey().equals(nodeName)) {
				List<Integer> keys = (List<Integer>) data.getValue();
			for(int hintData : keys) {
				Keyval.WriteKeyRequest.Builder writekey = Keyval.WriteKeyRequest.newBuilder();
				Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
				if(null!=memData.get(hintData).getValue()  && null!= memData.get(hintData).getTime()) {
				writekey.setKey(hintData);
				writekey.setValue(memData.get(hintData).getValue());
				writekey.setTimestamp(memData.get(hintData).getTime());
				message.setWriteKeyRequest(writekey);
				Keyval.NodeMessage writeRreq= message.build();
				try {
					writeRreq.writeDelimitedTo(new DataOutputStream(sock.getOutputStream()));
				}catch(IOException e) {
					
				}
				}
				
			}
		}
	  }
		
	}

	}
	private void rebuildmap(String FileName) {
		try {
	 FileReader fileread = new FileReader(FileName);
	 BufferedReader br = new BufferedReader(fileread);
	 String data,value = null,time = null;
	Integer key = null;
	 while((data = br.readLine())!=null ) {
		 try {
		 if(data.trim().length()>0) {
		 String parser[] =data.split(";");
		 Data dat= new Data();
		 if(null!= parser[1].split(":")[1]) {
		  key= Integer.valueOf(parser[1].split(":")[1]);
		 }
		 if(null!=parser[2].split(":")[1]) {
		  value= parser[2].split(":")[1];
		 }if(null!=parser[0].split(":")[1]) {
		  time= parser[0].split(":")[1];
		 }
		 if(null!=key && null!=value &&null!=time) {
			dat.setKey(Integer.valueOf(parser[1].split(":")[1]));
			dat.setValue(value);
			dat.setTime(time);
			System.out.println("REBUILDING MEM DATA:"+key+" "+value);
		 memData.put(key, dat); 
		 }
		 }
		 }catch(Exception e) {
				
				continue;
			}
	 }
		}catch(IOException e) {
			
		}
		
	}


	//Accept a new connection and add in  a map
	private void startserver(InetSocketAddress inetSocketAddress) {
		try {
			ServerSocket server= new ServerSocket(port);
			while(true) {

				Socket sock = server.accept();
				client(sock,lock,null);
				System.out.println("started a connection");
			}




		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void client(Socket sock, Object lock, String name) {
		
		Thread th  = new Thread (() ->{ 
			try {
				String nodName = name;
				if( nodName!=null) {
				
				}
				while(true) {
					InputStream input = new DataInputStream(sock.getInputStream());
					Keyval.NodeMessage msg =  Keyval.NodeMessage.parseDelimitedFrom(input);
					if(msg.hasInitNode()) {
						Keyval.InitNode init = msg.getInitNode();
						Keyval.InitNode.Node node = init.getNode();
						NodeData node_data= new NodeData(node.getName(),node.getIp(),node.getPort(),sock);
						nodeMapping.put(node.getName(), node_data);
						System.out.println(node.getName()+"Has Connected");
						nodName = node.getName();
						//hintedHandoff(sock,nodName);
					}
					
					if(msg.hasPutKey()) {
						String time=dateModified.format(new Date());
						int requestId =reqId.incrementAndGet();
						
							Keyval.PutKey put = msg.getPutKey();
							int key = put.getKey();
							String data  = put.getValue();
							int consisency= put.getConsistency();
							
							
							consistency_count.put(requestId,1);
							Set<String> nodes;
							Keyval.WriteKeyRequest.Builder writekey = Keyval.WriteKeyRequest.newBuilder();
							Keyval.NodeMessage.Builder mesg= Keyval.NodeMessage.newBuilder();
							writekey.setKey(key);
							writekey.setValue(data);
							writekey.setTimestamp(time);
							writekey.setId(requestId);
							respCount.put(requestId,1);
							reqCount.put(requestId,0);
							mesg.setWriteKeyRequest(writekey);
							Keyval.NodeMessage writeRreq= mesg.build();
							System.out.println("RECEIVED A WRITE REQUEST FOR DATA:"+data);
							synchronized(lock) {
								nodes = new HashSet<>( nodeMapping.keySet());
							int request=0;
							for(String node:nodes) {
								
								try {
								writeRreq.writeDelimitedTo(new DataOutputStream(nodeMapping.get(node).getSocket().getOutputStream()));
								reqCount.put(requestId,++request);
								}catch(Exception e) {
									//reqCount.put(requestId,++request);
									List<Integer> hintList = new ArrayList<>();
									//List<Integer> hintList= hint.get(node);
									if(hint.get(node)!=null){
										hintList.addAll(hint.get(node));
									}
									hintList.add(key);
									hint.put(node,hintList );
									synchronized(lock) {
									nodeMapping.put(node,null);
									}
									continue;
								}
								 
								
								
								
							}
							}
							while(true) {
							int req = reqCount.get(requestId);
							int resp = respCount.get(requestId);
							if(resp >= consisency || req<1) {
								
								Keyval.PutKeyResponse.Builder put_response = Keyval.PutKeyResponse.newBuilder();
								Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
								put_response.setKey(key);
								System.out.println("CONSISTENCY Received IS:"+ consistency_count.get(requestId));
								System.out.println("CONSISTENCY IS:"+ consisency);
								if(consistency_count.get(requestId)>= consisency) {
									synchronized(lock) {
										Data dat= new Data();
										dat.setKey(key);
										dat.setValue(data);
										dat.setTime(time);
										memData.put(key, dat);
										System.out.println("Written data:"+data +" At key:"+key);
									}
									put_response.setResponse(true);
									
									synchronized(lock){
										//LOGGER.log(Level.INFO,dateModified.format(new Date())+ " "+ "KEY:"+key +" "+"VALUE:"+data );
										writer = new BufferedWriter(new FileWriter(file,true));
										
										
										writer.write("time:"+time+ ";"+ "key:"+key +";"+"value:"+data);
										writer.newLine();
										writer.close();
										
										}
									
								}else {  
									Thread th2 = new Thread() {
										public void run() {
											Set<String> nodes;
									nodes = new HashSet<>( nodeMapping.keySet());
									
									for(String node:nodes) {
										List<Integer> hintList = new ArrayList<>();
										//List<Integer> hintList= hint.get(node);
										if(hint.get(node)!=null){
											hintList.addAll(hint.get(node));
										}
										for(int i=0;i<hintList.size();i++) {
											if(hintList.get(i)==key) {
												hintList.remove(i);
											}
										}
										
									hint.put(node,hintList);
									}
										}
									};
									th2.start();
									put_response.setResponse(false);
								}
								message.setPutKeyResponse(put_response);
								Keyval.NodeMessage response= message.build();
								response.writeDelimitedTo(new DataOutputStream(sock.getOutputStream()));
								
								
								reqCount.remove(requestId);
								respCount.remove(requestId);
								break;
							}
							
							}
							
							
							
							
							
						

					}
					
					if(msg.hasWriteKeyRequest()) {
						Thread th2 = new Thread() {
							String nodename = name;
							public void run() {
						if(!mechanism) {
						hintedHandoff(sock,name);
						}
							}
		};
		th2.start();
						Keyval.WriteKeyRequest write = msg.getWriteKeyRequest();
						int key = write.getKey();
						String data  = write.getValue();
						boolean isInserted= false;
						try{
							synchronized(lock){
								Data dat = new Data();
								dat.setKey(key);
								dat.setValue(data);
								dat.setTime(write.getTimestamp());
							memData.put(key, dat);
							isInserted=true;
							writer = new BufferedWriter(new FileWriter(file,true));
							//LOGGER.log(Level.INFO,dateModified.format(new Date())+ " "+ "KEY:"+key +" "+"VALUE:"+data );
							
							writer.write("time:"+write.getTimestamp()+ ";"+ "key:"+key +";"+"value:"+data);
							writer.newLine();
							writer.close();
							}
						}catch(Exception e) {
							isInserted=false;
						}
						System.out.println("Written data:"+data +" At key:"+key);
						Keyval.WriteKeyResponse.Builder write_response = Keyval.WriteKeyResponse.newBuilder();
						Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
						write_response.setKey(key);
						write_response.setResponse(isInserted);
						write_response.setId(write.getId());
						message.setWriteKeyResponse(write_response);
						Keyval.NodeMessage response= message.build();
						response.writeDelimitedTo(new DataOutputStream(sock.getOutputStream()));
						
					}
					if(msg.hasWriteKeyResponse()) {
						
						
						Keyval.WriteKeyResponse write_response = msg.getWriteKeyResponse();
						
						if(write_response.getResponse()) {
							System.out.println("Key has been inserted at other server:");
							if(null==consistency_count.get(write_response.getId())) {
								consistency_count.put(write_response.getId(), 1);
							}else {
							int consis = consistency_count.get(write_response.getId());
							consistency_count.put(write_response.getId(),++consis);
							}
							System.out.println("Consistency after resp:"+consistency_count.get(write_response.getId()));
						}else {
							System.out.println("Node failed to insert a key");
						}   
						if(null == respCount.get(write_response.getId())) {
							respCount.put(write_response.getId(),1);
							}else {
								int count = respCount.get(write_response.getId());
								count++;
								respCount.put(write_response.getId(), count);
							}
					}
					if(msg.hasGetKey()) {
						
						//respCount.set(0);
						 readkey(sock ,msg);
						
					}if(msg.hasReadKeyRequest()) {
						Thread th2 = new Thread() {
							String nodename = name;
							public void run() {
						if(!mechanism) {
						hintedHandoff(sock,name);
						}
							}
		};
		th2.start();
						Keyval.ReadKeyRequest get = msg.getReadKeyRequest();
						int key = get.getKey();
						//String data  = write.getValue();
						String data = null;
						String timestamp = null;
						boolean isInserted= false;
						try{
							synchronized(lock){
								
								if(null!=memData.get(key)) {
								Data dat = memData.get(key);
								key= dat.getKey();
								data=dat.getValue();
								timestamp= dat.getTime();
								}else {
									key= get.getKey();
									data="";
									timestamp= "";
								}
							
							
							}
						}catch(Exception e) {
							isInserted=false;
						}
						
						Keyval.ReadKeyResponse.Builder read_response = Keyval.ReadKeyResponse.newBuilder();
						Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
						read_response.setKey(key);
						read_response.setValue(data);
						read_response.setTimestamp(timestamp);
						read_response.setId(get.getId());
						message.setReadKeyResponse(read_response);
						Keyval.NodeMessage response= message.build();
						response.writeDelimitedTo(new DataOutputStream(sock.getOutputStream()));
						
					}
					if(msg.hasReadKeyResponse()) {
						
						
						Keyval.ReadKeyResponse read_response = msg.getReadKeyResponse();
						int consis = consistency_count.get(read_response.getId());
						consistency_count.put(read_response.getId(),++consis);
						if(null == respCount.get(read_response.getId())) {
							respCount.put(read_response.getId(),1);
							}else {
								int count = respCount.get(read_response.getId());
								count++;
								respCount.put(read_response.getId(), count);
							}
						Data dat = new Data();
						dat.setKey(read_response.getKey());
						dat.setValue(read_response.getValue());
						dat.setTime(read_response.getTimestamp());
						getRequest.put(nodName+"|"+read_response.getKey(), dat);
						if(null==memData.get(read_response.getKey())) {
							memData.put(read_response.getKey(), dat);
							writer = new BufferedWriter(new FileWriter(file,true));
							//LOGGER.log(Level.INFO,dateModified.format(new Date())+ " "+ "KEY:"+key +" "+"VALUE:"+data );
							if(null!=dat.getValue()) {
								if(dat.getValue().trim().length()>0) {
							writer.write("time:"+dat.getTime()+ ";"+ "key:"+dat.getKey() +";"+"value:"+dat.getValue());
							writer.newLine();
								}
							}
							
							writer.close();
						}else if((memData.get(read_response.getKey()).getTime().compareTo(read_response.getTimestamp()))<=-1) {
							memData.put(read_response.getKey(), dat);
							System.out.println("Difference is:"+memData.get(read_response.getKey()).getTime().compareTo(read_response.getTimestamp()));
						System.out.println(memData.get(read_response.getKey()).getTime());
						System.out.println(read_response.getTimestamp());
							writer = new BufferedWriter(new FileWriter(file,true));
							//LOGGER.log(Level.INFO,dateModified.format(new Date())+ " "+ "KEY:"+key +" "+"VALUE:"+data );
							if(null!=dat.getValue() && memData.get(read_response.getKey()).getTime().compareTo(read_response.getTimestamp()) >0) {
								if(dat.getValue().trim().length()>0) {
							writer.write("time:"+dat.getTime()+ ";"+ "key:"+dat.getKey() +";"+"value:"+dat.getValue());
							writer.newLine();
								}
							}
							
							writer.close();
						}
						
						
						
					}



				}

			}catch(IOException e) {
				Set<Integer> requestIds = new HashSet<>(reqCount.keySet());
				for(int req : requestIds) {
					synchronized(lock){
						
						if(null!= respCount.get(req)) {
							int count = respCount.get(req);
							respCount.put(req, ++count);
						}else {
							respCount.put(req, 1);
						}
						
					}
				}

			}

		});
		th.start();






	}

	private synchronized void  readkey(Socket sock, Keyval.NodeMessage msg) {
	
		try {
			
		synchronized(lock){
			Keyval.Getkey get = msg.getGetKey();
			int key = get.getKey();
			int requestId =reqId.incrementAndGet();
			reqCount.put(requestId,0);
			respCount.put(requestId,1);
			String data = null;
			List<String> nodes= new ArrayList<>();
			if(null!=memData.get(key)) {
			Data dat  =memData.get(key);
			data = dat.getValue();
			}
			int consisency= get.getConsistency();
			
				
			System.out.println("Got Data:"+data +" At key:"+key);
			consistency_count.put(requestId,1);
			int reqests=0;
			Set<String >nodesList  =   new HashSet<>( nodeMapping.keySet());
			for(String node: nodesList) {
				
				
				
			
				Keyval.ReadKeyRequest.Builder getkey = Keyval.ReadKeyRequest.newBuilder();
				Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
				getkey.setKey(key);
				getkey.setId(requestId);
				
				nodes.add(node);
				message.setReadKeyRequest(getkey);
				System.out.println("SENDING A REQUEST TO: "+ node);
				Keyval.NodeMessage writeRreq= message.build();
				try {
				writeRreq.writeDelimitedTo(new DataOutputStream(nodeMapping.get(node).getSocket().getOutputStream()));
				reqCount.put(requestId,++reqests);
				}catch(Exception e) {
					continue;
				}
				 
				
				
				
			}
			while(true) {
			int req = reqCount.get(requestId);
			int resp = respCount.get(requestId);
			
			if(resp >= consisency || req<1 ) {
				
				Keyval.GetKeyResponse.Builder get_response = Keyval.GetKeyResponse.newBuilder();
				Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();
				get_response.setKey(key);
				String time = "";
				String value ="";
				if(null!= memData.get(key)) {
				if(time.trim().length()<=0 || value.trim().length()<=0) { 
					time= memData.get(key).getTime();
					value= memData.get(key).getValue();
					}
				}else {
					value="No data found for the key";
				}
				
				if(consistency_count.get(requestId)>= consisency) {
					if(null!=value) {
						if(value.trim().length()>0) {
							get_response.setValue(value);
						}else {
							get_response.setValue("No data found for the key");
						}
					}else {
						get_response.setValue("No data found for the key");
					}
			
					
				}else {
					get_response.setValue("Consistency Failed");
				}
				message.setGetKeyResponse(get_response);
				Keyval.NodeMessage response= message.build();
				response.writeDelimitedTo(new DataOutputStream(sock.getOutputStream()));
/*				synchronized(lock){
					//LOGGER.log(Level.INFO,dateModified.format(new Date())+ " "+ "KEY:"+key +" "+"VALUE:"+data );
					writer = new BufferedWriter(new FileWriter(file,true));
					
					writer.write(dateModified.format("time:"+new Date())+ ";"+ "key:"+key +";"+"value:"+data);
					writer.newLine();
					writer.close();
					}*/
				reqCount.remove(requestId);
				respCount.remove(requestId);
				if(mechanism) {
				for(String node:nodes) {
					if(null!=getRequest.get(node+"|"+key)) {
						
							
						
						if(time.compareTo(getRequest.get(node+"|"+key).getTime())>0) {
							System.out.println("Starting read repair for:"+node+"|"+key);
							Thread th = new Thread () {
								public void run() {
							readrepair(nodeMapping.get(node).getSocket(), memData.get(key));
								}
							};
							
							th.start();
						}
					}
				}
				}
				break;
			}
			
			}
			
			
			
			
			
		}
		}catch(IOException e) {
			
			
			
		}
	}

	
	

	public void start() {

		try {
			Scanner sc = new Scanner (new File(getFileName()));

			while(true) {

				if(!sc.hasNext()) {
					break;
				}
				String[] nodeData = sc.nextLine().split(" ");

				if(getNodeName().equals(nodeData[0])) {
					this.setIp(nodeData[1]);
				}else {
					try {
					Socket sock = new Socket ();
					//System.out.println("Connecting"+ nodeData[0] + " " +nodeData[1]+Integer.valueOf(nodeData[2] ));
					sock.connect(new InetSocketAddress(nodeData[1],Integer.valueOf(nodeData[2])));
					NodeData node= new NodeData(nodeData[0],nodeData[1],Integer.valueOf(nodeData[2]),sock);
					nodeMapping.put(nodeData[0], node);
					client(sock,lock,nodeData[0]);
					}catch(Exception e) {
						continue;
					}
				}

			}
				Keyval.InitNode.Builder init = Keyval.InitNode.newBuilder();
				Keyval.InitNode.Node.Builder nodeinit = Keyval.InitNode.Node.newBuilder();
				Keyval.NodeMessage.Builder message= Keyval.NodeMessage.newBuilder();

				for(String key: nodeMapping.keySet()) {
					if(null!=nodeMapping.get(key)) {
					nodeinit.setName(getNodeName());
					nodeinit.setIp(getIp());
					nodeinit.setPort(getPort());
					init.setNode(nodeinit);
					message.setInitNode(init); 
					Keyval.NodeMessage msg= message.build();
					System.out.println("Connecting:"+key);
					msg.writeDelimitedTo(new DataOutputStream(nodeMapping.get(key).getSocket().getOutputStream()));
					}
				}

			

		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
