package com.grpc.client;

import com.util.ConfigUtil;
import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

/**
 * Client for file transfer
 */
public class Client {
	final static Logger logger = Logger.getLogger(Client.class);
	private static ConfigUtil config;
	private static Long rpcCount;
	private static Semaphore limiter;

	Client(){
		config = new ConfigUtil();
		rpcCount = 0l;
		limiter = new Semaphore(100);
		logger.info("Client initialized.");
	}

	/**
	 * Gets the file catalog form local Raft servers
	 */
	public void listFiles(){
		logger.info("Initiating request for file catalog.");

		try{
			limiter.acquire();
		} catch (InterruptedException e){
			e.printStackTrace();
			logger.error("Could not initiate ListFiles - maximum limit reached.");
		}
		//TODO: Randomly select proxy
		ManagedChannel ch = ManagedChannelBuilder
				.forAddress(config.raftNodes.get(0).getIP(), config.raftNodes.get(0).getPort())
				.build();
		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(ch);
		//create the request
		FileTransfer.RequestFileList req = FileTransfer.RequestFileList.newBuilder().setIsClient(true).build();
		stub.listFiles(req, new StreamObserver<FileTransfer.FileList>() {
			@Override
			public void onNext(FileTransfer.FileList fileList) {
				printFileList(fileList);
			}

			@Override
			public void onError(Throwable throwable) {
				logger.error("*** LIST FILES FAILED! "+throwable.getMessage());
			}

			@Override
			public void onCompleted() {
				logger.info("Completed request for file catalog.");
				shutdownChannel(ch);
			}
		});

	}

	public void requestFileInfo(FileTransfer.FileInfo req){
		logger.info("Initiating request for RequestFileInfo.");

		try{
			limiter.acquire();
		} catch (InterruptedException e){
			e.printStackTrace();
			logger.error("Could not initiate RequestFileInfo - maximum limit reached.");
		}
		//TODO: Randomly select proxy
		ManagedChannel ch = ManagedChannelBuilder
				.forAddress(config.raftNodes.get(0).getIP(), config.raftNodes.get(0).getPort())
				.build();
		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(ch);
		stub.requestFileInfo(req, new StreamObserver<FileTransfer.FileLocationInfo>() {
			@Override
			public void onNext(FileTransfer.FileLocationInfo fileLocationInfo) {

			}

			@Override
			public void onError(Throwable throwable) {

			}

			@Override
			public void onCompleted() {

			}
		});
	}

	private void shutdownChannel(ManagedChannel ch) {
		ch.shutdown();
	}

	private void printFileList(FileTransfer.FileList fileList) {
		logger.info("Printing file list.");
		for(int i=0; i<fileList.getLstFileNamesCount(); ++i){
			System.out.println(fileList.getLstFileNames(i));
		}
	}

	public static void main(String[] args) {
	
	}

}
