package com.grpc.raft;

import java.util.List;

import org.apache.log4j.Logger;

import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * This class is a client to Raft Server.
 * @author Sricheta's computer
 *
 */
public class RaftClient {

	final static Logger logger = Logger.getLogger(RaftClient.class);
	
	/**
	 * This function gets the addresses of global nodes and calls the RPC getFileLocation to each of them
	 * If FileFound == true then add to a list of FileTransfer.FileLocationInfo.
	 * @param globalNodes
	 * @param request
	 * @return
	 */
	
	
	public FileTransfer.FileLocationInfo getFileFromOtherTeam(List<Connection> globalNodes, FileInfo request) {
		
		logger.debug("Getting File from all the clusters..");
		ManagedChannel channel = null;
		String addressString = null;
		for(Connection connection : globalNodes) {

			addressString = connection.getIP() +":" +  connection.getPort();
			channel = ManagedChannelBuilder.forTarget(addressString)
					.usePlaintext(true)
					.build();

			DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(channel);
			FileTransfer.FileLocationInfo response =  stub.getFileLocation(request);
			
			if(response.getIsFileFound()) {
				logger.debug("File Found in "+ addressString +"!! ");
				channel.shutdownNow();
				return response;

			}
			
			channel.shutdownNow();
		}
		
		return null;
	}
}
