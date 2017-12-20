LIB_PATH=/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar
all: clean 



	javac -cp $(LIB_PATH) -d .  Data.java Init.java InitializeNode.java Keyval.java NodeData.java NodeServer.java TestClient.java
	






	
clean:
	
