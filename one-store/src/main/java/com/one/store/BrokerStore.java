/**
 * 
 */
package com.one.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.one.remote.cmd.OneMessage;
import org.tio.utils.json.Json;

import com.one.store.common.MsgPosition;
import com.one.store.file.MappedFile;

/**
 * @author yangkunguo
 *
 */
public class BrokerStore {

	static String s=File.separator;
	private String path=s+"onemq"+s+"store"+s+"data"+s;
	
	private RandomAccessFile afile=null;
	FileChannel fchan=null;
	//消息存储
	private Map<String, MappedFile> mappedFile=new HashMap<>();
	
	//消息索引位置信息
	private Map<String,LinkedBlockingQueue<MsgPosition>> msgMappedInfo=new ConcurrentHashMap<>(100000);
	/**
	 * @param args
	 */
	/*public static void main(String[] args) {
		long s=System.currentTimeMillis();
		BrokerStore store=new BrokerStore();
		String str="{\"clientName\":\"测试-1\",\"clientUrl\":\"127.0.0.1:6634\",\"queueName\":\"topic-1\"}";
		
		for(int i=0;i<1000000;i++) {
//			long ops=store.saveMsg("topic", str);
//			System.out.println(ops);
		}
		System.out.println(System.currentTimeMillis()-s);
	}*/

	public long testsaveMsg(String topic,String msg)  {
		File file=new File(path);
		if(!file.exists()) {
			file.mkdirs();
		}
		file=new File(path+topic+".data");
		FileChannel fchan=null;
		FileLock lock=null;
		long length=0;
		try {
//			if(afile==null)
				afile=new RandomAccessFile(file, "rw");
			length=afile.length();
//			System.out.println(afile.getFilePointer());
			afile.seek(length);
//			if(fchan==null)
			fchan = afile.getChannel();
			msg=msg+"\n";
			byte[] bt=msg.getBytes(Charset.forName("UTF-8"));
//			System.out.println(msg.length()+"=="+bt.length);
			lock=fchan.tryLock(length, bt.length, false);
//			System.out.println(lock.isShared());
//			lock.acquiredBy().
			fchan.write(ByteBuffer.wrap(bt));
//			afile.write(bt);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			
			if(lock!=null) try {
				lock.release();
				if(fchan!=null)fchan.close();
				if(afile!=null)afile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return length;
	}
	
	public void load() {
		
	}
	
	public void start() {
		File file=new File(path);
		if(!file.exists()) {
			file.mkdirs();
		}
//		file=new File(path+topic+".data");
		
		load();
	}
	
	public void stop() {
		for(Entry<String, MappedFile> ent:mappedFile.entrySet()) {
			ent.getValue().cleanup(0);
		}
	}
	public boolean putMessage(OneMessage msg) {
		
		if(!mappedFile.containsKey(msg.getTopic())) {
			File file=new File(path+msg.getTopic()+".data");
			mappedFile.put(msg.getTopic(), new MappedFile(file,1024*1024*100));
		}
		MappedFile mf=mappedFile.get(msg.getTopic());
		Long position=mf.appand(msg);
		if(position!=-1) {
			LinkedBlockingQueue<MsgPosition> indexs=msgMappedInfo.get(msg.getTopic());
			if(indexs==null) {
				indexs=new LinkedBlockingQueue<>();
				msgMappedInfo.put(msg.getTopic(), indexs);
			}
			
			try {
				indexs.put(new MsgPosition(msg.get_id(),position, mf.enCode(msg).length()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
//		System.out.println(Json.toJson(msgMappedInfo));
		mf.flush(mf.getWrotePosition());
		return true;
	}

	private OneMessage readMessage(String topic,MsgPosition mp) {
		MappedFile mf=mappedFile.get(topic);
		//
		if((mp.getPosition()+mp.getLength())>mf.getFileSize()) {
			
		}
		OneMessage msg=mf.read(mp);
//		if(msg!=null) {
//			msgMappedInfo.get(topic).remove(id);
//		}
		return msg;
	}
	
	public List<OneMessage> readMessage(String topic,int readSize) {
		List<OneMessage> msgs=new ArrayList<>(readSize);
		LinkedBlockingQueue<MsgPosition> indexs=msgMappedInfo.get(topic);
		int i=0;
		if(indexs==null)return msgs;
		Iterator<MsgPosition> iterator=indexs.iterator();
		while(iterator.hasNext()) {
			OneMessage msg= readMessage( topic, iterator.next());
			 if(msg!=null) {
				 msgs.add(msg);
			 }
			i++;
			if(i==readSize)break;
		}

		return msgs;
	}
//	public Map<String, MsgPosition> getMsgMappedInfo() {
//		return msgMappedInfo;
//	}
	
	public void removeMessage(String topic,List<OneMessage> ids) {
		LinkedBlockingQueue<MsgPosition> indexs=msgMappedInfo.get(topic);
		System.out.println("MsgPosition.size : "+indexs.size());
		for(OneMessage id:ids) {
			indexs.remove(new MsgPosition(id.get_id(),0L,0));
		}
		System.out.println("MsgPosition.size : "+indexs.size());
	}
}
