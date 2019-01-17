/**
 * 
 */
package org.one.broker.message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.tio.core.ChannelContext;

/**队列对应消费客户端信息
 * @author yangkunguo
 *
 */
public class QueneInfo {

	/**
	 * topic,channel
	 */
	private static Map<String, Set<ChannelContext>> topicChannels=new ConcurrentHashMap<>(16);
	

	/**
	 * @return
	 */
	public static Map<String, Set<ChannelContext>> getTopicChannels() {
		return topicChannels;
	}
	/**
	 * @param topic
	 * @return
	 */
	public static Set<ChannelContext> getTopicChannels(String topic) {
		return topicChannels.get(topic)==null?new HashSet<>():topicChannels.get(topic);
	}

	/**添加队列相连的通道
	 * @param topic
	 * @param channel
	 */
	public static synchronized void addTopicChannels(String topic, ChannelContext channel) {
		Set<ChannelContext> chas=topicChannels.get(topic);
		if(chas==null) {
			chas=new HashSet<>();
		}else {
			List<ChannelContext> dels=new ArrayList<>();
			for(ChannelContext cc:chas) {
				if(cc==null || cc.isClosed || cc.getClientNode().getIp().equals("$UNKNOWN")) {
					dels.add(cc);
				}
			}
			for(ChannelContext cc:dels) {
				chas.remove(cc);
			}
		}
		if(channel==null || channel.isClosed || channel.getClientNode().getIp().equals("$UNKNOWN")) {
			return;
		}
		chas.add(channel);
		QueneInfo.topicChannels.put(topic, chas);
	}
}
