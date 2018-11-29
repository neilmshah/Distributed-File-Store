package com.grpc.proxy;

import org.apache.log4j.Logger;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileUploadData;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * This class acts a Client to DB Server
 * @author Sricheta's computer
 *
 */
public class DBClient {

	final static Logger logger = Logger.getLogger(DBClient.class);
	
	/**
	 * This method calls the db node to upload File chunk
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

}
