package com.grpc.proxy;

import org.apache.log4j.Logger;

import com.util.ConfigUtil;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Use this class to test your Proxt RPC's
 * @author Sricheta's computer
 *
 */
public class TestProxyClient {
	
	final static Logger logger = Logger.getLogger(TestProxyClient.class); 

	public static void main(String[] args) {

		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:3000")
				.usePlaintext(true)
				.build();

		FileTransfer.FileUploadData fileUploadData = FileTransfer.FileUploadData.newBuilder()
				.setFileName("Gello.txts")
				.setChunkId(123)
				.setMaxChunks(10)
				.setSeqNum(0)
				.setSeqMax(0)
				.build();
		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);	

		StreamObserver<FileTransfer.FileInfo> responseObserver = new StreamObserver<FileTransfer.FileInfo>() {
			public void onNext(FileTransfer.FileInfo fileInfo) {
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


	private static void loadConfig() {
		ConfigUtil c = new ConfigUtil();
	}


}
