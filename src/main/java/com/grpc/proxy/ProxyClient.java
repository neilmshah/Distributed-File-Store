package com.grpc.proxy;

import java.util.Iterator;
import java.util.List;
import java.lang.Math;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.ChunkInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import grpc.Team.ChunkLocations;
import grpc.Team.FileData;
import grpc.TeamClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * This class acts a Client to DB Server
 * @author Sricheta's computer
 *
 */
public class ProxyClient {

	final static Logger logger = Logger.getLogger(ProxyClient.class);
	
	/**
	 * This method calls the db node to upload File chunk
	 * 
	 * rpc UploadFile (stream FileUploadData) returns (FileInfo); 
	 * 
	 * TODO Update target address with Db Server IP
	 */
	public void uploadDataToDB(FileUploadData fileUploadData) {
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:9000")
				.usePlaintext(true)
				.build();

		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);	

		StreamObserver<FileTransfer.FileInfo> responseObserver = new StreamObserver<FileTransfer.FileInfo>() {
			public void onNext(FileTransfer.FileInfo fileInfo) {
				//send to DB
				logger.debug("Successfully written chunk: "+fileInfo.getFileName());
			}

			public void onError(Throwable t) {
				t.printStackTrace();
				logger.debug("Upload File Chunk failed:");
			}

			public void onCompleted() {
				logger.debug("Upload File Chunk completed.");
			}
		};

		StreamObserver<FileTransfer.FileUploadData> requestObserver = stub.uploadFile(responseObserver);
		try{
			requestObserver.onNext(fileUploadData);
			Thread.sleep(5000);

		} catch(RuntimeException e){
			requestObserver.onError(e);
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		requestObserver.onCompleted();
		channel.shutdown();
	}
	
	/**
	 * Initiate downloadChunk request from proxy to db
	 * 
	 * rpc DownloadChunk (ChunkInfo) returns (stream FileMetaData);
	 * 
	 * TODO  Update target address with Db Server IP
	 */
	public Iterator<FileMetaData> downloadChunk(ChunkInfo request, List<String> li) {
		Iterator<FileMetaData> fileMetaDataList = null;
		for (String addr : li) {
			final ManagedChannel channel = ManagedChannelBuilder.forTarget(addr)
					.usePlaintext(true)
					.build();
			DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);	
			try {
				fileMetaDataList = blockingStub.downloadChunk(request);
				} catch (StatusRuntimeException ex) {
				  logger.log(Level.WARN, "RPC failed: {0}");
				}
			channel.shutdown();
			if(fileMetaDataList != null) {
				break;
			}
		}
		return fileMetaDataList;
	}
	
	
	/**
	 * Proxy to get File Locations from RAFT
	 * 
	 * rpc getChunkLocations (FileData) returns (ChunkLocations)
	 * 
	 * TODO Update target address with RAFT Server IP
	 * 
	 */
	public ChunkLocations GetChunkLocations(ChunkInfo ch) {
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:3000")
				.usePlaintext(true)
				.build();
		TeamClusterServiceGrpc.TeamClusterServiceBlockingStub blockingStub = TeamClusterServiceGrpc.newBlockingStub(channel);
		
		FileData fd = FileData.newBuilder().setChunkId(ch.getChunkId()).setFilename(ch.getFileName()).setMessageId((long)Math.random()).build();
		
		return blockingStub.getChunkLocations(fd);

	}
}
