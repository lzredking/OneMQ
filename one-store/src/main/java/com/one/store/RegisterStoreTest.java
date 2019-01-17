/**
 * 
 */
package com.one.store;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.one.remote.common.Const;
import org.one.remote.common.OneBroker;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class RegisterStoreTest {

	static String s=File.separator;
	private static File regFile=new File(s+"onemq"+s+"store"+s+"regster.cf");
	public static String brokerTitle="[broker]";
	public static String producerTitle="[producer]";
	public static String consumerTitle="[consumer]";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String data="{\"brokerName\":\"def-master1\",\"brokerUrl\":\"127.0.0.1:6656\",\"groupName\":\"group1\",\"isLive\":1,\"lastTime\":0,\"master\":true,\"role\":\"MASTER\"}";
		List<String> lines=new ArrayList<>();
//		lines.add("[broker]");
//		for(int i=0;i<10;i++) {
//			lines.add(data);
//		}
//		lines.add("[producer]");
//		for(int i=0;i<10;i++) {
//			lines.add(data);
//		}
//		try {
//			
////			FileUtils.writeStringToFile(file, data, Charset.forName("UTF-8"));
////			FileUtils.writeLines(file, "UTF-8", lines, true);
//			lines=FileUtils.readLines(regFile, "UTF-8");
//			boolean isread=false;
//			for(String line:lines) {
//				if(isread) {
//					System.out.println(line);
//				}
//				if(line.equals("[producer]")) {
//					isread=true;
//				}
//				if(StringUtils.isBlank(line)) {
//					isread=false;
//					continue;
//				}
//			}
//			
//			FileUtils.writeLines(regFile, lines, "123", false);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//改成写Josn数组
//		OneBroker bk=Json.toBean(data, OneBroker.class);
		addRegisterInfo(brokerTitle,data);
		addRegisterInfo(producerTitle,data);
		addRegisterInfo(consumerTitle,data);
	}

	/**添加Broker
	 * @param broker
	 */
	public static void addRegisterInfo(String title,OneBroker broker) {
		
		String info=Json.toJson(broker);
		try {
			Map<String,List<String>> map=readStore(title);
			List<String> lines=map.get("center");
			if(lines.isEmpty()) {
//				lines.add(title);
				lines.add(info);
			}else {
				//去重
//				boolean isExist=false;
				Map<String,String> datas=new HashMap<>();
				lines.add(info);
				for(String json:lines) {
//					if(StringUtils.isNotBlank(json)) {
//						System.out.println(json);
//						OneBroker bk=Json.toBean(json, OneBroker.class);
//						System.out.println(bk.getGroupName()
//								.equals(broker.getGroupName())
//								&& bk.getBrokerName()
//								.equals(broker.getBrokerName()));
//						if(bk.getGroupName().equals(broker.getGroupName())
//								&& bk.getBrokerName().equals(broker.getBrokerName())) {
//							isExist=true;
//						}
//					}
					if(datas.get(json)==null) {
						datas.put(json, json);
					}else {
						if(!json.equals(datas.get(json)))
							datas.put(json, json);
					}
				}
//				datas.put(info,info);
				lines=new LinkedList<>();
				for(String json:datas.keySet()) {
					lines.add(json);
				}
//				if(!isExist) {
//					lines.add(info);
//				}
			}
			//重新写入
			reWriteConf( map, lines,title);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**删除Broker
	 * @param broker
	 */
	public void removeBrokerInfo(OneBroker broker) {
		try {
			Map<String,List<String>> map=readStore(brokerTitle);
			List<String> lines=map.get("center");
			if(lines.isEmpty()) {
				return;
			}else {
				//去掉
//				boolean isExist=false;
				for(String json:lines) {
					OneBroker bk=Json.toBean(json, OneBroker.class);
					if(bk.getGroupName().equals(broker.getGroupName())
							&& bk.getBrokerName().equals(broker.getBrokerName())) {
						lines.remove(json);
						break;
					}
				}
				
			}
			//重新写入
			reWriteConf( map, lines,brokerTitle);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private static synchronized void reWriteConf(Map<String,List<String>> map,List<String> lines,String title) throws IOException {
		List<String> infos=new LinkedList<>();
		infos.addAll(map.get("before"));
		if(!map.get("before").isEmpty()) {
			infos.add("---");
		}
		infos.add(title);
		infos.addAll(lines);
		infos.add("---");
		infos.addAll(map.get("after"));
		infos.add("---");
		List<String> datas=new LinkedList<>();
		for(String data:infos) {
			System.out.println("--"+data);
			if(StringUtils.isNotBlank(data)) {
				datas.add(data);
			}
//			if(data.equals(brokerTitle) || data.equals(producerTitle) 
//					|| data.equals(consumerTitle)) {
//				datas.add("---");
//			}
		}
//		if(StringUtils.isNotBlank(datas.get(datas.size()-1))) {
//			datas.add("\r\n");
//		}
		FileUtils.writeLines(regFile, "UTF-8", datas, false);
	}
	/**读所有配置
	 * @return
	 * @throws IOException
	 */
	public static List<String> readStore() throws IOException{
		List<String> lines=FileUtils.readLines(regFile, "UTF-8");
//		if(lines.isEmpty()) {
//			lines.add(RegisterStore.brokerTitle);
////			lines.add("\r\n");
//			lines.add(RegisterStore.producerTitle);
////			lines.add("\r\n");
//			lines.add(RegisterStore.consumerTitle);
////			lines.add("\r\n");
//		}
		return lines;
	}
	
	/**读取指定配置
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static Map<String,List<String>> readStore(String title) throws IOException{
		Map<String,List<String>> map=new HashMap<>();
		List<String> infos=new LinkedList<>();
		List<String> beforeInfos=new LinkedList<>();
		List<String> afterInfos=new LinkedList<>();
		List<String> lines=readStore();
		
		boolean isbefore=true;
		boolean isread=false;
		boolean isafter=false;
		for(String line:lines) {
			if(isbefore) {
				beforeInfos.add(line);
			}
			if(isafter) {
				if(!line.equals(title)) {
					afterInfos.add(line);
				}
			}
			if(isread) {
				System.out.println(line);
				if(!line.equals(brokerTitle) && !line.equals(producerTitle) 
						&& !line.equals(consumerTitle)) {
					infos.add(line);
				}
			}
			if(line.equals(title)) {
				beforeInfos.remove(title);
				isread=true;
				isbefore=false;
				isafter=false;
			}
			if(line.equals("---")) {
				isread=false;
				isbefore=false;
				isafter=true;
				continue;
			}
		}
		map.put("before", beforeInfos);
		map.put("center", infos);
		map.put("after", afterInfos);
		return map;
	}
}
