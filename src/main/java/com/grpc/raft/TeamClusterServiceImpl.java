package com.grpc.raft;

import org.apache.log4j.Logger;

import grpc.Team;
import grpc.TeamClusterServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * This class communicates the the RAFT Servers
 * @author Sricheta's computer
 *
 */
public class TeamClusterServiceImpl extends TeamClusterServiceGrpc.TeamClusterServiceImplBase{

	private RaftServer server;
	final static Logger logger = Logger.getLogger(TeamClusterServiceImpl.class);

	public TeamClusterServiceImpl(RaftServer serv){
		super();
		server = serv;
	}

	/**
	 * This method updates the RAFT HashMap with the DB address where it stored the chunk
	 * @param request
	 * @param responseObserver
	 */
	public void UpdateChunkLocations(Team.ChunkLocations request, StreamObserver<Team.Ack> responseObserver) {

		String key = request.getFileName()+"_"+ request.getChunkId();
		String value = server.data.get(key);
		
		//MaxChunks$IP1,IP2,IP3
		// If file is getting uploaded for the first time.
		if(value == null) {
			value = request.getMaxChunks()+"$";
			StringBuilder builder = new StringBuilder();
			builder.append(value);
			for(int i =0;i<request.getDbAddressesCount();i++) {
				builder.append(request.getDbAddresses(i));
				builder.append(",");
			}
			value = value.substring(0, value.length() - 1); 
			server.data.put(key, value);
		}else {
			//If key is already there, only update the db addresses.
			String[] valArr = value.split("\\$");
			StringBuilder builder = new StringBuilder();
			for(int i =0;i<request.getDbAddressesCount();i++) {
				builder.append(request.getDbAddresses(i));
				builder.append(",");
			}
			valArr[1] = builder.toString().substring(0, builder.length() - 1);
			String newValue = valArr[0] + "$"+ valArr[1];
			server.data.put(key, newValue);
			
		}
		
		Team.Ack response = Team.Ack.newBuilder()
				.setMessageId(request.getMessageId())
				.setIsAck(true)
				.build();

		// Use responseObserver to send a single response back
		responseObserver.onNext(response);

		// When you are done, you must call onCompleted.
		responseObserver.onCompleted();

	}


}
