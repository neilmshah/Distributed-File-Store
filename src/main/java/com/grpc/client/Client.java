package com.grpc.client;

import java.util.stream.Stream;

import com.util.ConfigUtil;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class Client {


	public static void main(String[] args) {

		loadConfig();
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:3000")
				.usePlaintext(true)
				.build();

		DataTransferServiceGrpc.DataTransferServiceStub stub = DataTransferServiceGrpc.newStub(channel);
		StreamObserver<FileTransfer.FileUploadData> uploadFile = stub.uploadFile(new StreamObserver<FileTransfer.FileInfo>() {
			@Override
			public void onNext(FileTransfer.FileInfo value) {
				System.out.println("file: " + value.getFileName());
			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onCompleted() {

			}
		});


	}

	private static void loadConfig() {
		ConfigUtil c = new ConfigUtil();



	}

}
