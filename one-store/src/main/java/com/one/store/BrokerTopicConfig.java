/**
 * 
 */
package com.one.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**队列名的文件保存
 * @author yangkunguo
 *
 */
public class BrokerTopicConfig {

	private static String encoding="UTF-8";
	private static String s=File.separator;
	private static String path=s+"onemq"+s+"store"+s+"topic"+s;
	
	/**
	 * @param topic
	 * @throws IOException
	 */
	public static void addTopic(String topic) throws IOException {
		Properties pro=new Properties();
		File dir=new File(path);
		if(!dir.exists()) {
			dir.mkdir();
		}
		File file=new File(path+"topic.config");
		if(!file.exists())file.createNewFile();
		
		InputStream in =new FileInputStream(file);
		InputStreamReader reader=new InputStreamReader(in, encoding);
		pro.load(reader);
		in.close();
		reader.close();
		//
		OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file,true), Charset.forName(encoding));
//		out = new FileOutputStream(file,false);//true表示追加打开
		pro.put(topic, "1");
		pro.store(out, "add key  "+topic+"=1");
		out.close();
	}
	
	/**
	 * @return
	 * @throws IOException
	 */
	public static Map<String, String> loadTopic() throws IOException{
		
		Properties pro=new Properties();
		File dir=new File(path);
		if(!dir.exists()) {
			dir.mkdir();
		}
		File file=new File(path+"topic.config");
		if(!file.exists())file.createNewFile();
		
		InputStream in =new FileInputStream(file);
		InputStreamReader reader=new InputStreamReader(in, encoding);
		pro.load(reader);
		in.close();
		reader.close();
		//
		
		Map<String,String> kvs=new HashMap<String,String>((Map)pro);
		
		return kvs;
		
	}
	
	/**
	 * @param topic
	 * @throws IOException
	 */
	public static void removeTopic(String topic) throws IOException{
		
		Properties pro=new Properties();
		File dir=new File(path);
		if(!dir.exists()) {
			dir.mkdir();
		}
		File file=new File(path+"topic.config");
		if(!file.exists())file.createNewFile();
		
		InputStream in =new FileInputStream(file);
		InputStreamReader reader=new InputStreamReader(in, encoding);
		pro.load(reader);
		in.close();
		reader.close();
		//
		OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file,false), Charset.forName(encoding));
		pro.remove(topic);
		
		pro.store(out, "remove key  "+topic+"=1");
		out.close();
	}
}
