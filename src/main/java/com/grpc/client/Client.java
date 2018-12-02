package com.grpc.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import com.google.protobuf.ByteString;
import com.util.ConfigUtil;
import com.util.Connection;

import grpc.DataTransferServiceGrpc;
import grpc.FileTransfer;
import grpc.FileTransfer.ChunkInfo;
import grpc.FileTransfer.FileInfo;
import grpc.FileTransfer.FileList;
import grpc.FileTransfer.FileLocationInfo;
import grpc.FileTransfer.FileMetaData;
import grpc.FileTransfer.FileUploadData;
import grpc.FileTransfer.FileUploadInfo;
import grpc.FileTransfer.ProxyInfo;
import grpc.FileTransfer.RequestFileList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import grpc.DataTransferServiceGrpc.DataTransferServiceStub;

/**
 * Client to initiate all requests
 * @author Manogna
 */

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
		String fileName = "";
		File f = null;
		int maxChunks = 0;
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
				fileName = scan.nextLine();
				OutputStream out = new FileOutputStream(fileName);
				downloadFile(fileName, out);
				break;
			case "3":
				System.out.println("Enter File Path: ");
				f = new File(scan.nextLine());
				System.out.println("Enter maximum chunks: ");
				maxChunks = scan.nextInt();
				scan.nextLine();
				uploadFile(f, maxChunks);
				break;
			default:
				System.out.println("Invalid option. Press 0 to see Menu.");
				break;
			}

		}
		scan.close();
	}

	@SuppressWarnings("null")
	private static void uploadFile(File f, int maxChunks) throws IOException, InterruptedException {
		
		List<ProxyInfo> proxyList = requestFileUpload(f, maxChunks);
		int proxyNum = proxyList.size();
		//TODO Alter the fixed seq size
		int seqSize = 1024 * 1024; // 1MB
		long totalSeq = f.length()/seqSize;
		
		int seqMax;
		if(totalSeq % maxChunks == 0) {
			seqMax = (int) (totalSeq/maxChunks);
		} else {
			seqMax = (int) (totalSeq/(maxChunks-1));
		}
		
		/**
		 *  Allocating chunks to each Proxy
		 */
//		HashMap<Integer, ProxyInfo> proxyMap = new HashMap<Integer, ProxyInfo>();
//		if(maxChunks % proxyNum  == 0) {
//			int allottedChunks = maxChunks/proxyNum, start = 1, pr = 0;
//			for(int i=1; i <= maxChunks; i++) {
//				if(i < start+allottedChunks) {
//					proxyMap.put(i, proxyList.get(pr));
//					if (start+allottedChunks-i == 1) {
//						start = i+1;
//						pr++;
//					}
//				}
//			}
//		} else {
//		    int allottedChunks = maxChunks/(proxyNum-1), start=1, pr=0;
//			for(int i=1; i <= maxChunks-1; i++) {
//				if(i < start+allottedChunks) {
//					proxyMap.put(i, proxyList.get(pr));
//					if (start+allottedChunks-i == 1) {
//						start = i+1;
//						pr++;
//					}
//				}
//			}
//			proxyMap.put(maxChunks, proxyList.get(pr));
//		}
		
		
		
		/**
		 *  Allocating each chunk to stubs
		 */
		HashMap<Integer, DataTransferServiceStub> stubMap = new HashMap<Integer, DataTransferServiceStub>();
		if(maxChunks % proxyNum  == 0) {
			int allottedChunks = maxChunks/proxyNum, start = 1, pr = 0;
			for(int i=1; i <= maxChunks; i++) {
				if(i < start+allottedChunks) {
					ProxyInfo proxy = proxyList.get(pr);
					ManagedChannel channel = getChannel(proxy.getIp()+":"+proxy.getPort());
					DataTransferServiceStub asyncStub = DataTransferServiceGrpc.newStub(channel);
					stubMap.put(i, asyncStub);
					if (start+allottedChunks-i == 1) {
						start = i+1;
						pr++;
					}
				}
			}
		} else {
		    int allottedChunks = maxChunks/(proxyNum-1), start=1, pr=0;
			for(int i=1; i <= maxChunks-1; i++) {
				if(i < start+allottedChunks) {
					ProxyInfo proxy = proxyList.get(pr);
					ManagedChannel channel = getChannel(proxy.getIp()+":"+proxy.getPort());
					DataTransferServiceStub asyncStub = DataTransferServiceGrpc.newStub(channel);
					stubMap.put(i, asyncStub);
					if (start+allottedChunks-i == 1) {
						start = i+1;
						pr++;
					}
				}
			}
			ProxyInfo proxy = proxyList.get(pr);
			ManagedChannel channel = getChannel(proxy.getIp()+":"+proxy.getPort());
			DataTransferServiceStub asyncStub = DataTransferServiceGrpc.newStub(channel);
			stubMap.put(maxChunks, asyncStub);
		}
		
		

		StreamObserver<FileInfo> responseObserver = new StreamObserver<FileInfo>() {
			@Override
			public void onNext(FileInfo fileInfo) {

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
		
		byte[] buffer = new byte[seqSize];

		FileInputStream fis = new FileInputStream(f);
		@SuppressWarnings("resource")
		BufferedInputStream bis = new BufferedInputStream(fis);

		int bytesAmount = 0, seqNum = 0, chunkId = 1;

		while ((bytesAmount = bis.read(buffer)) > 0) {
			StreamObserver<FileTransfer.FileUploadData> requestObserver = stubMap.get(chunkId).uploadFile(responseObserver);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			seqNum++;
			out.write(buffer, 0, bytesAmount);	
			byte[] contents = out.toByteArray();

			FileUploadData uploadData = FileUploadData.newBuilder().setFileName(f.getName())
					.setChunkId(chunkId).setMaxChunks(maxChunks)
					.setSeqNum(seqNum).setSeqMax(seqMax).setData(ByteString.copyFrom(contents)).build();
			
			requestObserver.onNext(uploadData);
			Thread.sleep(new Random().nextInt(1000) + 500);
			
			if(seqNum == seqMax && chunkId < maxChunks) {
				requestObserver.onCompleted();
				((ManagedChannel) stubMap.get(chunkId).getChannel()).shutdown();
				seqNum = 0;
				chunkId++;
			} else if(seqNum == (totalSeq % (maxChunks-1)) && chunkId == maxChunks){
				requestObserver.onCompleted();
				((ManagedChannel) stubMap.get(chunkId).getChannel()).shutdown();
			}
		}
	}

	private static List <ProxyInfo> requestFileUpload(File f, long maxChunks) {
		System.out.println("requestFileUpload called ");

		ManagedChannel ch = getChannel(getRandomAddress(ConfigUtil.raftNodes));
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(ch);

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
				+ "2) Download File Chunk\n"
				+ "3) Upload File\n"
				);
	}

	private static void downloadFile(String fileName, OutputStream out) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("downloadFile called with" + fileName);
		FileLocationInfo fileDetails = requestFileInfo(fileName);
		
		List<ProxyInfo> readProxies = fileDetails.getLstProxyList();
		int proxyNum = readProxies.size();
		long maxChunks = fileDetails.getMaxChunks();
		
		for(int i=1; i <= maxChunks; i++) {
			ProxyInfo proxy = readProxies.get((i-1) % proxyNum);
			
			ManagedChannel channel = getChannel(proxy.getIp() + ":" + proxy.getPort());
			
			DataTransferServiceGrpc.DataTransferServiceStub asyncStub 
				= DataTransferServiceGrpc.newStub(channel);
			
			ChunkInfo downloadRequest = ChunkInfo.newBuilder()
					.setFileName(fileDetails.getFileName()).setChunkId(i).setStartSeqNum(1).build();
			
			asyncStub.downloadChunk(downloadRequest, new StreamObserver<FileMetaData>() {

				@Override
				public void onNext(FileMetaData fileData) {
					byte [] data = fileData.getData().toByteArray();
					try {
						out.write(data);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}

				@Override
				public void onError(Throwable t) {
					// TODO Auto-generated method stub
					t.printStackTrace();
				}

				@Override
				public void onCompleted() {
					// TODO Auto-generated method stub
					channel.shutdown();
				}
				
			});
		}

	}

	private static FileLocationInfo requestFileInfo(String fileName) {
		// TODO Auto-generated method stub
		System.out.println("requestFileInfo called with " + fileName);

		//TODO Update RAFT Node address
		ManagedChannel channel = getChannel(getRandomAddress(ConfigUtil.raftNodes));
		DataTransferServiceGrpc.DataTransferServiceBlockingStub blockingStub = DataTransferServiceGrpc.newBlockingStub(channel);
		FileInfo request = FileInfo.newBuilder().setFileName(fileName).build();
		FileLocationInfo fileLocations = blockingStub.requestFileInfo(request);
		System.out.println("Locations: \n" + fileLocations.getLstProxyList());
		return fileLocations;
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
		int index = new Random().nextInt(1);
		return nodes.get(index).getIP()+":"+ nodes.get(index).getPort();
	}

}