/**
 * 
 */
package org.one.broker.message;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;

/**
 * @author yangkunguo
 *
 */
public class CacheMsg {

	/**
	 * 缓存消息
	 */
	private static Map<String, Integer> cacheTopic = new ConcurrentHashMap<>(100);

	/**
	 * topic , id,msg
	 */
	private static Map<String, Map<String,OneMessage>> cacheTopicMsg = new ConcurrentHashMap<>(100);
	
	private static AtomicLong size=new AtomicLong(0);
	private CacheMsg() {}
	
	
	/**通过Key查询消息
	 * @param topic
	 * @param id
	 * @return
	 */
	public static OneMessage getCacheMsg(String topic,String id) {
		Map<String,OneMessage> msgs=cacheTopicMsg.get(topic);
		return msgs.get(id);
	}
	
	/**删除已经消费的消息
	 * @param topic
	 * @param id
	 * @return
	 */
	public static OneMessage removeCacheMsg(OneMessage msg) {
		Map<String,OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		size.decrementAndGet();
		return msgs.remove(msg.get_id());
	}

	/**读取次数+1
	 * @param topic
	 * @param id
	 * @return
	 */
	public static OneMessage addCacheMsgNumber(String topic,String id) {
		Map<String,OneMessage> msgs=cacheTopicMsg.get(topic);
		OneMessage msg=msgs.get(id);
		msg.setReadNumber(msg.getReadNumber()+1);
//		size.addAndGet(1);
		return msg;
	}

	/**查询队列是否已经存在
	 * @param topic
	 * @return
	 */
	public static boolean getCacheTopic(String topic) {
		if(cacheTopic.get(topic)==null) {
			return false;
		}
		return cacheTopic.get(topic)!=null?true:false;
	}

	public static void setCacheTopic(String topic) {
		CacheMsg.cacheTopic.put(topic, 1);
	}

	/**返回队列下的消息
	 * @param topic
	 * @return
	 */
	public static List<OneMessage> getCacheTopicMsg(String topic) {
		List<OneMessage> msgs=new ArrayList<>(10000);
		if(cacheTopicMsg.get(topic)!=null) {
			msgs.addAll(cacheTopicMsg.get(topic).values());
		}
		return msgs;
	}

	/**缓存消息记录
	 * @param msg
	 */
	public static void addCacheTopicMsg(OneMessage msg) {
		Map<String,OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		if(msgs==null) {
			msgs=new ConcurrentHashMap<>(10000);
		}
		
		msgs.put(msg.get_id(), msg);
		CacheMsg.cacheTopicMsg.put(msg.getTopic(), msgs);
		size.addAndGet(1);
	}


	public static Map<String, Map<String, OneMessage>> getCacheTopicMsg() {
		return cacheTopicMsg;
	}


	public static long getSize() {
		return size.get();
	}

	
	
}
