package com.grpc.raft;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.log4j.Logger;

import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.RequestFileList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

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


	public FileTransfer.FileLocationInfo getFileLocationFromOtherTeam(List<Connection> globalNodes, FileInfo request) {

		logger.debug("Getting File locations from all the clusters..");
		ManagedChannel channel = null;
		String addressString = null;
		for(Connection connection : globalNodes) {

			addressString = connection.getIP() +":" +  connection.getPort();
			channel = ManagedChannelBuilder.forTarget(addressString)
					.usePlaintext(true)
					.build();

			DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(channel);
			FileTransfer.FileLocationInfo response  = null;
			try {
				response =  stub.withDeadlineAfter(10000, TimeUnit.MILLISECONDS).getFileLocation(request);
			}
			catch(Exception e) {
				channel.shutdownNow();
				continue;
			}
			if(response.getIsFileFound()) {
				logger.debug("File Found in "+ addressString +"!! ");
				channel.shutdownNow();
				return response;

			}

			channel.shutdownNow();
		}

		return null;
	}

	/**
	 * This method calls the list File rpc of all the others teams. Adds to the Arraylist and returns to Rft Server
	 * @param globalNodes
	 * @param fileList
	 * @param request 
	 */
	public void listFilesFromOtherTeams(List<Connection> globalNodes, List<String> fileList, RequestFileList request) {

		logger.debug("List Files from all other clusters..");
		ManagedChannel channel = null;
		String addressString = null;
		for(Connection connection : globalNodes) {
  
			addressString = connection.getIP() +":" +  connection.getPort();
			channel = ManagedChannelBuilder.forTarget(addressString)
					.usePlaintext(true)
					.build();

			System.out.println("Retrieving file list from "+addressString);
			DataTransferServiceGrpc.DataTransferServiceBlockingStub stub = DataTransferServiceGrpc.newBlockingStub(channel);
			try {
				RequestFileList requestNew = RequestFileList.newBuilder().setIsClient(false).build();
				FileTransfer.FileList response =  stub.listFiles(requestNew);
				if(response.getLstFileNamesCount() != 0) {
					for(int i = 0;i< response.getLstFileNamesCount();i++) {
						fileList.add(response.getLstFileNames(i));
					}
				}
			}catch (Exception e){
				System.out.println("Unable to reach this global node from "+addressString+"! Skipping");
				e.printStackTrace();
			}
		}

	}

	public void listFilesFromOtherTeamsOptimized(List<Connection> globalNodes, List<String> fileList, RequestFileList request){
		ConcurrentLinkedQueue<String> mylist = new ConcurrentLinkedQueue<String>();
		AtomicInteger completedRequests = new AtomicInteger(0);

		for(Connection conn : globalNodes){
			String addressString = conn.getIP() +":" +  conn.getPort();
			ManagedChannel channel = ManagedChannelBuilder.forTarget(addressString)
					.usePlaintext(true)
					.build();

			DataTransferServiceGrpc.DataTransferServiceFutureStub stub = DataTransferServiceGrpc.newFutureStub(channel);
			RequestFileList requestNew = RequestFileList.newBuilder().setIsClient(false).build();
			ListenableFuture<FileTransfer.FileList> future =  stub.withDeadlineAfter(10000, TimeUnit.MILLISECONDS).listFiles(requestNew);
			Futures.addCallback(future, new FutureCallback<FileTransfer.FileList>() {
				@Override
				public void onSuccess(@Nullable FileTransfer.FileList fileList) {
					for(int i = 0; i < fileList.getLstFileNamesCount(); i++){
						mylist.add(fileList.getLstFileNamesList().get(i));
					}
					completedRequests.incrementAndGet();
				}

				@Override
				public void onFailure(Throwable throwable) {
					completedRequests.incrementAndGet();
				}
			});

		}

		while(completedRequests.intValue() < globalNodes.size()){
			//Wait until it completes
			//Thread.sleep(50);
			logger.debug("Waiting for responses. Received "+completedRequests.intValue()+" / "+globalNodes.size());
		}

		for(String s : mylist){
			fileList.add(s);
		}
	}
}
