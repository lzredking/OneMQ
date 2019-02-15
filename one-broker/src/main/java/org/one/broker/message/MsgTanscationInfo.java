/**
 * 
 */
package org.one.broker.message;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.one.remote.common.broker.MsgInfo;

/**队列对应消费客户端信息
 * @author yangkunguo
 *
 */
public class MsgTanscationInfo {

	/**
	 * msgid,channel
	 */
//	private static Map<String, ChannelContext> msgIdChannels=new ConcurrentHashMap<>(100000);
	
	/**如果消息要消费结果，暂存在此
	 * msgid,topic
	 */
	private static Map<String, String> msgIdTopic=new ConcurrentHashMap<>(100000);
	
	/**
	 * 存放事务不能正常推送回去的消息ID
	 * topic,msgids
	 */
	private static Map<String,Map<String,MsgInfo>> tansMsgIds=new ConcurrentHashMap<>(100);
	
	private static AtomicLong msgSize=new AtomicLong(0);
	public static AtomicLong tansSize=new AtomicLong(0);
	
	private static final Object value=new Object();

	private static ReentrantLock lock=new ReentrantLock();
	/**
	 * @return
	 */
//	public static Map<String, ChannelContext> getMsgIdChannels() {
//		return msgIdChannels;
//	}
	
	/**查询通过
	 * @param id
	 * @return
	 */
//	public static ChannelContext getMsgIdChannels(String id) {
//		return msgIdChannels.get(id);
//	}

	/**添加队列相连的通道
	 * @param topic
	 * @param channel
	 */
//	public static void addMsgIdChannels(OneMessage msg, ChannelContext channel) {
//		if(cc==null)cc=channel;
//		MsgTanscationInfo.msgIdChannels.put(msg.get_id(), cc);
//		addMsgIdTopic(msg.get_id(), msg.getTopic());
//		msgSize.addAndGet(1);;
//	}
	
	/**删除
	 * @param id
	 */
//	public static void removeMsgIdChannels(String id) {
//		removeWarnningMsgIds(id);
////		msgIdChannels.remove(id);
////		msgIdTopic.remove(id);
//		msgSize.decrementAndGet();
//	}

//	public static void removeMsgIdChannels(List<String> msgIds) {
//		for(String id:msgIds) {
//			removeMsgIdChannels( id);
//		}
//	}
	/**通过消息ID查询对应的Topic
	 * @param id
	 * @return
	 */
	public static String getMsgIdTopic(String id) {
		return msgIdTopic.get(id);
	}
//
	/**等待消费成功
	 * @param id
	 * @param topic
	 */
	public static void addMsgIdTopic(String id, String topic) {
		msgIdTopic.put(id, topic);
		msgSize.addAndGet(1);
	}

	/**
	 * @param topic
	 * @param length 获取消息长度
	 * @return
	 */
	public static synchronized Set<MsgInfo> getTansMsgIds(String topic,Integer length) {
		HashSet<MsgInfo> temp=new HashSet<>(100);
		if(length==null) {
			length=1;
		}
		int i=0;
		if(tansMsgIds.get(topic)!=null) {
			Set<MsgInfo> ids=new HashSet<>();
					ids.addAll(tansMsgIds.get(topic).values());
			if(ids.size()<=length) {
				return ids;
			}else {
				HashSet<MsgInfo> set=new HashSet<>();
				set.addAll(ids);
				@SuppressWarnings("unchecked")
				HashSet<MsgInfo> cloneSet=(HashSet<MsgInfo>) set.clone();
				for(MsgInfo id:cloneSet) {
					if(i<length) {
						temp.add(id);
						i++;
					}else {
						break;
					}
				}
//				cloneSet.clear();
			}
		}
		return temp;
	}

	public static void removeTansMsgIds(Set<MsgInfo> msgs) {
		for(MsgInfo msg:msgs) {
			Map<String,MsgInfo> temp=tansMsgIds.get(msg.getTopic());
			if(temp!=null)temp.remove(msg.getId());
			msgIdTopic.remove(msg.getId());
			msgSize.decrementAndGet();
//			tansSize.decrementAndGet();
			tansSize.addAndGet(1);
		}
	}
	
	/**
	 * @param warnningMsgIds
	 */
	public static void addTansMsgIds(MsgInfo msg) {
		Map<String,MsgInfo> temp=tansMsgIds.get(msg.getTopic());
		if(temp==null) {
			temp=new ConcurrentHashMap<>(100000);
		}
//		lock.lock();
//		try {
//			
//		} finally {
//			lock.unlock();
//		}
		temp.put(msg.getId(),msg);
		tansMsgIds.put(msg.getTopic(),temp);
		msgIdTopic.put(msg.getId(),msg.getTopic());
	}

	/**
	 * @param msgs
	 */
	public static void addTansMsgIds(Set<MsgInfo> msgs) {
//		System.out.println("addTansMsgIds = "+msgs.size());
//		int i=1;
		for(MsgInfo msg:msgs) {
			addTansMsgIds(msg);
//			System.out.println("=="+msg.getId());
		}
//		System.out.println(tansSize.get()+" ==addTansMsgIds == "+getTansMsgSize("topic-1"));
//		if(tansSize.get()>getTansMsgSize("topic-1")) {
//			try {
//				Thread.sleep(100);
//				System.out.println(tansSize.get()+" ==addTansMsgIds 22== "+getTansMsgSize("topic-1"));
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
	}
	public static long getTansMsgSize(String topic) {
//		String topic=getMsgIdTopic(id);
//		System.out.println(tansSize.get());
		if(tansMsgIds.get(topic)==null)return 0L;
		return tansMsgIds.get(topic).size();
	}
	public static long getMsgSize() {
		if(msgSize.get()<0)msgSize.set(0);
		return msgSize.get();
	}

	public static Map<String, Map<String,MsgInfo>> getTansMsgIds() {
		return tansMsgIds;
	}
}
