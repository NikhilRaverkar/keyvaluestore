

import java.net.Socket;

public class NodeData {
	
	String name;
	String ip;
	int port;
	Socket socket;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public NodeData(String name, String ip, int port, Socket socket) {
		super();
		this.name = name;
		this.ip = ip;
		this.port = port;
		this.socket = socket;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public Socket getSocket() {
		return socket;
	}
	public void setSocket(Socket socket) {
		this.socket = socket;
	}
	

}
