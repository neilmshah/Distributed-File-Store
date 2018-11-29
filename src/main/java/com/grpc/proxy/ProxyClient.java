package com.grpc.proxy;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileUploadData;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import grpc.FileTransfer.ChunkInfo;
import grpc.FileTransfer.FileMetaData;

/**
 * This class acts a Client to DB Server
 * @author Sricheta's computer
 *
 */
public class ProxyClient {

	final static Logger logger = Logger.getLogger(ProxyClient.class);
	
	/**
	 * This method calls the db node to upload File chunk
	 * TODO Update IP Address of Db
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
	 * TODO Update IP Address of Db
	 */
	public Iterator<FileMetaData> downloadChunk(ChunkInfo request) {
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:3000")
				.usePlaintext(true)
				.build();
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);	
		Iterator<FileMetaData> fileMetaDataList = null;
		try {
			fileMetaDataList = blockingStub.downloadChunk(request);
			} catch (StatusRuntimeException ex) {
			  logger.log(Level.WARN, "RPC failed: {0}");
			}
		channel.shutdown();
		return fileMetaDataList;
	}
}
