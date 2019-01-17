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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.one.remote.cmd.OneMessage;

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
	
	private Map<String, MappedFile> mappedFile=new HashMap<>();
	
	private Map<String/*msgID*/, MsgPosition> msgMappedInfo=new ConcurrentHashMap<>(100000);
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long s=System.currentTimeMillis();
		BrokerStore store=new BrokerStore();
		String str="{\"clientName\":\"测试-1\",\"clientUrl\":\"127.0.0.1:6634\",\"queueName\":\"topic-1\"}";
		
		for(int i=0;i<1000000;i++) {
//			long ops=store.saveMsg("topic", str);
//			System.out.println(ops);
		}
		System.out.println(System.currentTimeMillis()-s);
	}

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
		if(mf.appand(msg)) {
			msgMappedInfo.put(msg.get_id(), new MsgPosition(mf.getWrotePosition(), mf.enCode(msg).length()));
		}
		mf.flush(mf.getWrotePosition());
		return true;
	}

	public OneMessage readMessage(String topic,String id) {
		MappedFile mf=mappedFile.get(topic);
		OneMessage msg=mf.read(msgMappedInfo.get(id));
		if(msg!=null) {
			msgMappedInfo.remove(msg.get_id());
		}
		return msg;
	}
	public Map<String, MsgPosition> getMsgMappedInfo() {
		return msgMappedInfo;
	}
}
