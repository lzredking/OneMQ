/**
 * 
 */
package com.one.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.one.remote.common.OneBroker;
import org.tio.utils.json.Json;

import com.alibaba.fastjson.JSONArray;
import com.one.store.enums.RegisterType;

/**
 * @author yangkunguo
 *
 */
public class RegisterStore {

	static String s=File.separator;
	private static File regFile=new File(s+"onemq"+s+"store"+s+"regster.cf");
	
	
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
		OneBroker bk=Json.toBean(data, OneBroker.class);
//		addRegisterInfo(brokerTitle,bk);
//		addRegisterInfo(producerTitle,bk);
//		addRegisterInfo(consumerTitle,bk);
		removeRegisterInfo(RegisterType.broker.name(),bk);
	}

	/**添加配置
	 * @param title 配置行标题
	 * @param obj 配置行对象
	 */
	public static void addRegisterInfo(String title, Object obj) {
		List<Object> objs=new ArrayList<>();
		
		try {
			List<String> lines=readStore();//new LinkedList<>();
			String datas=readStore(title);
			if(StringUtils.isBlank(datas)) {
				objs.add(obj);
			}else {
				//去重
				objs=(List<Object>) JSONArray.parseArray(datas, obj.getClass());
				
				Set<Object> set=new HashSet<>();
				objs.add(obj);
				for(Object json:objs) {
					set.add(json);
				}
				
				objs.clear();
				for(Object json:set) {
					objs.add(json);
				}
			}
			List<String> nlines=new LinkedList<>();
			for(String line:lines) {
				if(StringUtils.isNotBlank(line)
						&& !line.split("=")[0].equals(title)) {
					nlines.add(line);
				}
			}
			//重新写入
			String info=Json.toJson(objs);
			nlines.add(title+"="+info);
			reWriteConf(nlines);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**删除Broker
	 * @param broker
	 */
	public static void removeRegisterInfo(String title, Object obj) {
		try {
			List<String> lines=readStore();
			List<String> nlines=new LinkedList<>();
			if(lines.isEmpty()) {
				return;
			}else {
				//去掉
				for(String line:lines) {
					if(StringUtils.isNotBlank(line)
							&& line.split("=")[0].equals(title)) {
						List<Object> objs=(List<Object>) JSONArray.parseArray(line.split("=")[1], obj.getClass());
						objs.remove(obj);
						
						String info=Json.toJson(objs);
						nlines.add(title+"="+info);
						break;
					}
				}
				for(String line:lines) {
					if(StringUtils.isNotBlank(line)
							&& !line.split("=")[0].equals(title)) {
						nlines.add(line);
					}
				}
				//重新写入
				reWriteConf(nlines);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**重写配置
	 * @param datas
	 * @throws IOException
	 */
	private static synchronized void reWriteConf(List<String> datas) throws IOException {
		FileUtils.writeLines(regFile, "UTF-8", datas, false);
	}
	/**读所有配置
	 * @return
	 * @throws IOException
	 */
	public static List<String> readStore() throws IOException{
		List<String> lines=FileUtils.readLines(regFile, "UTF-8");
		return lines;
	}
	
	/**读取指定配置
	 * @param title
	 * @return 对象数据Json
	 * @throws IOException
	 */
	public static String readStore(String title) throws IOException{
		List<String> lines=readStore();
		
		for(String line:lines) {
			if(StringUtils.isNotBlank(line)
					&& line.split("=")[0].equals(title)) {
				return line.split("=")[1];
			}
		}
		return null;
	}

	public static File getRegFile() {
		return regFile;
	}

	public static void setRegFile(File regFile) {
		RegisterStore.regFile = regFile;
	}
}
