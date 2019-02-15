/**
 * 
 */
package org.one.remote.producer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.one.remote.cmd.OneMessage;
import org.tio.core.ChannelContext;

/**消息发送失败缓存
 * @author yangkunguo
 *
 */
public class SendCache {

	private static Map<String, OneMessage> rertyMsgs=new ConcurrentHashMap<>(100000);

	private static Map<String, OneMessage> sendMsgs=new ConcurrentHashMap<>(100000);
	
	private static AtomicLong size=new AtomicLong(0);
	
	public static Map<String, OneMessage> getRertyMsgs() {
		return rertyMsgs;
	}

	/**消息发送记录
	 * @param id
	 * @param msg
	 */
	public static void addRertyMsgs(String id, OneMessage msg) {
		rertyMsgs.put(id, msg);
		size.addAndGet(1);
	}
	/**发送成功后删除
	 * @param id
	 */
	public static void removeRertyMsgs(String id) {
		rertyMsgs.remove(id);
		size.decrementAndGet();
	}

	/**
	 * @return
	 */
	public static Long getRertyMsgsSize() {
		return size.get();
	}

	public static Map<String, OneMessage> getSendMsgs() {
		return sendMsgs;
	}

	public static void addSendMsgs(OneMessage sendMsg) {
		SendCache.sendMsgs.put(sendMsg.get_id(), sendMsg);
	}
}
