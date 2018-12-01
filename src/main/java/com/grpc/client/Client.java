package com.grpc.client;

import java.io.File;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;


import com.util.ConfigUtil;
import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileList;
import grpc.FileTransfer.FileLocationInfo;

import com.google.protobuf.ByteString;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileList;
import grpc.FileTransfer.FileLocationInfo;
import grpc.FileTransfer.FileUploadData;

import grpc.FileTransfer.FileUploadInfo;
import grpc.FileTransfer.ProxyInfo;
import grpc.FileTransfer.RequestFileList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class Client {

	private static ManagedChannel getChannel(String address) {
		return ManagedChannelBuilder.forTarget(address)
				.usePlaintext(true)
				.build();
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		new ConfigUtil();
		Scanner scan = new Scanner(System.in);
		showMenu();
		String readFile = "";
		List<ProxyInfo> proxyList = new ArrayList<ProxyInfo>();
		File f = null;

		while(scan.hasNext()) {
			String s1 = scan.nextLine();
			if(s1.equals("Over")) {
				break;
			}
			switch(s1) {
			case "0":
				showMenu();
				break;
			case "1":
				listFiles();
				break;
			case "2":
				System.out.println("Enter the File Name: \n");
				readFile = scan.nextLine();
				requestFileInfo(readFile);
				System.out.println("Press 3 to download file. \n");
				break;
			case "3":
				if(readFile != "") {
					downloadFile(readFile);
				} else {
					System.out.println("Press 1 to select a file. \n");
				}
				readFile = "";
				break;
			case "4":
				System.out.println("Enter File Path");
				f = new File(scan.nextLine());
				proxyList = requestFileUpload(f, 10);
				break;
			case "5":
				uploadFile(f, proxyList, 10);
				break;
			default:
				System.out.println("Invalid option. Press 0 to see Menu\n");
				break;
			}

		}
		scan.close();
	}

	private static void uploadFile(File f, List<ProxyInfo> proxyList, int maxChunks) throws IOException, InterruptedException {
		int proxyNum = proxyList.size();

		ArrayList<ManagedChannel> chList = new ArrayList<ManagedChannel>();
		for(ProxyInfo pr: proxyList) {
			chList.add(getChannel(pr.getIp() + pr.getPort()));
		}

		DataTransferServiceGrpc.DataTransferServiceStub asyncStub = DataTransferServiceGrpc.newStub(chList.get(0));
		int seqSize = 1024 * 1024; // 1MB
		long seqMax = f.length()/seqSize;
		byte[] buffer = new byte[seqSize];
		// TODO Split file into Chunks

		FileInputStream fis = new FileInputStream(f);
		BufferedInputStream bis = new BufferedInputStream(fis);

		int bytesAmount = 0, seqNum = 0;

		ByteArrayOutputStream out = null;



		StreamObserver<FileTransfer.FileInfo> responseObserver = new StreamObserver<FileTransfer.FileInfo>() {
			@Override
			public void onNext(FileTransfer.FileInfo fileInfo) {

			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();

			}

			@Override
			public void onCompleted() {
				System.out.println("On completed");

			}
		};

		StreamObserver<FileTransfer.FileUploadData> requestObserver = asyncStub.uploadFile(responseObserver);

		while ((bytesAmount = bis.read(buffer)) > 0) {
			seqNum++;
			out.write(buffer, 0, bytesAmount);	
			byte[] contents = out.toByteArray();

			FileUploadData uploadData = FileUploadData.newBuilder().setFileName(f.getName())
					.setChunkId(seqNum).setMaxChunks(maxChunks)
					.setSeqNum(seqNum).setSeqMax(seqMax).setData(ByteString.copyFrom(contents)).build();

			requestObserver.onNext(uploadData);
			Thread.sleep(new Random().nextInt(1000) + 500);
		}

		requestObserver.onCompleted();

		//TODO Create stub per each channel
		//		DataTransferServiceGrpc.DataTransferServiceStub ayncStub = DataTransferServiceGrpc.newStub(chList.get(1));
	}

	private static boolean checkFileUploadInfo(String line) {
		// TODO Check each element of words
		String [] words = line.split(",");
		if(words.length != 3)
			return false;
		return true;

	}

	private static List <ProxyInfo> requestFileUpload(File f, long maxChunks) {
		System.out.println("requestFileUpload called ");

		ManagedChannel ch = getChannel(getRandomAddress(ConfigUtil.raftNodes));
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(ch);

		int chunkSize = 1024 * 1024;// 1MB

		FileUploadInfo request = FileUploadInfo.newBuilder()
				.setFileName(f.getName())
				.setFileSize((float)f.length())
				.setMaxChunks(maxChunks)
				.build();

		return blockingStub.requestFileUpload(request).getLstProxyList();
	}

	private static void showMenu() {
		System.out.println("Select an option and press Enter: \n"
				+ "1) List Files\n" 
				+ "2) Read a particular file\n"
				+ "3) Download File Chunk\n"
				+ "4) Request File Upload\n"
				+ "5) Upload File\n"
				);

	}

	private static void downloadFile(String readFile) {
		// TODO Auto-generated method stub
		System.out.println("downloadFile called with" + readFile);

	}

	private static void requestFileInfo(String fileName) {
		// TODO Auto-generated method stub
		System.out.println("requestFileInfo called with " + fileName);

		//TODO Update RAFT Node address
		ManagedChannel channel = getChannel(getRandomAddress(ConfigUtil.raftNodes));
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);

		FileInfo request = FileInfo.newBuilder().setFileName(fileName).build();

		FileLocationInfo fileLocations = blockingStub.requestFileInfo(request);

		System.out.println("Locations: \n" + fileLocations.getLstProxyList());
	}

	private static void listFiles() {
		System.out.println("listFiles called");

		//TODO Update RAFT Node address
		ManagedChannel channel = getChannel(getRandomAddress(ConfigUtil.raftNodes));

		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);

		RequestFileList request = RequestFileList.newBuilder().setIsClient(true).build();
		FileList li = blockingStub.listFiles(request);

		System.out.println("File List: \n" + li);
	}

	private static String getRandomAddress(List<Connection> nodes) {
		//int index = new Random().nextInt(nodes.size()); ucomment in final demo
		int index = new Random().nextInt(0);
		return nodes.get(index).getIP()+":"+ nodes.get(index).getPort();
	}

}