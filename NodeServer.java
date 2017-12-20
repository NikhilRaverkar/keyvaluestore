

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NodeServer {
	
	InetSocketAddress inetSocketAddress;
	Selector selector;
	ServerSocketChannel socketChannel;
	ByteBuffer bytebuff;

	public InetSocketAddress getInetSocketAddress() {
		return inetSocketAddress;
	}

	public void setInetSocketAddress(InetSocketAddress inetSocketAddress) {
		this.inetSocketAddress = inetSocketAddress;
	}

	public NodeServer(InetSocketAddress inetSocketAddress) {
		super();
		this.inetSocketAddress = inetSocketAddress;
		try {
			selector= Selector.open();
			socketChannel = ServerSocketChannel.open();
			socketChannel.bind(inetSocketAddress);
			socketChannel.configureBlocking(false);
			socketChannel.register(selector, SelectionKey.OP_ACCEPT);
			bytebuff = ByteBuffer.allocate(512);
			Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() ->{
				
				try {
					connect();
				}catch(Exception ex) {
					
				}  
				
			},0,50,TimeUnit.MILLISECONDS);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
	
	private void connect() throws IOException {
		selector.selectNow();
		for(SelectionKey key : selector.selectedKeys()) {
			
			
			if(key.isAcceptable()) {
			listen(selector,socketChannel);
			}
			if(key.isReadable()) {
				read(bytebuff,key.channel());
			}
		}
		
		selector.keys().clear();
		
		
		
	}

	
	
	
	
	private void read(ByteBuffer bytebuff2, SelectableChannel channel) {
		try {
		SocketChannel sock = (SocketChannel)channel;
		//InputStream input = new DataInputStream(sock.read(bytebuff2));
		sock.read(bytebuff2);
		bytebuff2.flip();
		Keyval.NodeMessage message = Keyval.NodeMessage.parseFrom(bytebuff2);
		if(message.hasInitNode()) {
			System.out.println("YAYAYAYYAs");
		}
		bytebuff2.clear();
		
		}catch(IOException e) {
			
		}
		
	}

	private void listen(Selector selector, ServerSocketChannel socketChannel) {
		try {
		SocketChannel serverSock = SocketChannel.open();
		serverSock.configureBlocking(false);
		serverSock.register(selector,SelectionKey.OP_READ);
		}catch(IOException ex) {
			
		}
	}
}
