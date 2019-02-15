/**
 * 
 */
package org.one.broker.message;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
//	private static Map<String, Map<String,OneMessage>> cacheTopicMsg = new ConcurrentHashMap<>(100);
	private static Map<String, LinkedBlockingQueue<OneMessage>> cacheTopicMsg = new ConcurrentHashMap<>(100);
	
	private static AtomicLong size=new AtomicLong(0);
	private CacheMsg() {}
	
	
	/**通过Key查询消息
	 * @param topic
	 * @param id
	 * @return
	 */
//	public static OneMessage getCacheMsg(String topic,String id) {
//		Map<String,OneMessage> msgs=cacheTopicMsg.get(topic);
//		
//		return msgs.get(id);
//	}
//	
	/**删除已经消费的消息
	 * @param topic
	 * @param id
	 * @return
	 */
	public static OneMessage removeCacheMsg(OneMessage msg) {
//		Map<String,OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		LinkedBlockingQueue<OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		size.decrementAndGet();
		msgs.remove(msg);
		return msg;//msgs.remove(msg.get_id());
	}
	
	public static void removeCacheMsg(List<OneMessage> msgs) {
		for(OneMessage msg:msgs) {
			removeCacheMsg(msg);
		}
	}

	/**读取次数+1
	 * @param topic
	 * @param id
	 * @return
	 */
//	public static OneMessage addCacheMsgNumber(String topic,String id) {
//		LinkedBlockingQueue<OneMessage> msgs=cacheTopicMsg.get(topic);
//		OneMessage msg=msgs.get(id);
//		msg.setReadNumber(msg.getReadNumber()+1);
//		return msg;
//	}

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
	public static List<OneMessage> getCacheTopicMsg(String topic,int size) {
		List<OneMessage> msgs=new ArrayList<>(10000);
		if(cacheTopicMsg.get(topic)!=null) {
			LinkedBlockingQueue<OneMessage> bq=cacheTopicMsg.get(topic);
			int i=0;
			Iterator<OneMessage> iterator=bq.iterator();
			while(iterator.hasNext()) {
				msgs.add(iterator.next());
				i++;
				if(i==size)break;
			}
			
		}
		return msgs;
	}

	/**缓存消息记录
	 * @param msg
	 * @throws InterruptedException 
	 */
	public static void addCacheTopicMsg(OneMessage msg) throws InterruptedException {
//		Map<String,OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		LinkedBlockingQueue<OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		if(msgs==null) {
//			msgs=new ConcurrentHashMap<>(10000);
			msgs=new LinkedBlockingQueue<>();
		}
		
//		msgs.put(msg.get_id(), msg);
		msgs.put(msg);
		CacheMsg.cacheTopicMsg.put(msg.getTopic(), msgs);
		size.addAndGet(1);
	}


	public static Map<String, LinkedBlockingQueue<OneMessage>> getCacheTopicMsg() {
		return cacheTopicMsg;
	}


	public static long getSize(OneMessage msg) {
		LinkedBlockingQueue<OneMessage> msgs=cacheTopicMsg.get(msg.getTopic());
		if(msgs!=null) {
			return msgs.size();
		}
		return 0;
	}

	
	
}
