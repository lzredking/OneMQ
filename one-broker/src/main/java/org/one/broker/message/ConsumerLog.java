package org.one.broker.message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.one.remote.cmd.OneMessage;

/**先记录缓存，后面存盘
 * @author yangkunguo
 *
 */
public class ConsumerLog {

	//Client标识，topic,_id
	private static Map<String, Map<String,OneMessage>> logs=new ConcurrentHashMap<>(100);

	/**查看消息是否已经消费
	 * @param client
	 * @param tipic
	 * @param id
	 * @return
	 */
	public static OneMessage getLogs(String client, String tipic, String id) {
		Map<String,OneMessage> msgs=logs.get(client);
		if(msgs!=null) {
			return msgs.get(tipic+"-"+id);
		}
		return null;
	}

	/**消费记录
	 * @param client
	 * @param tipic
	 * @param id
	 */
	public static void addLogs(String client, OneMessage msg) {
		Map<String,OneMessage> msgs=logs.get(client);
		if(msgs==null) {
			msgs=new HashMap<>(100);
		}
		msgs.put(msg.getTopic()+"-"+msg.get_id(), msg);
		ConsumerLog.logs.put(client, msgs);
	}
	
	public static void addLogs(String client, List<OneMessage> msgs) {
		for(OneMessage msg:msgs)
			addLogs(client,msg);
	}
	/**删除记录
	 * @param client
	 * @param tipic
	 * @param id
	 */
	public static void delLogs(String client, String tipic, String id) {
		Map<String,OneMessage> msgs=logs.get(client);
		if(msgs==null) {
			msgs=new HashMap<>(100);
		}
		msgs.remove(tipic+"-"+id);
//		ConsumerLog.logs.put(client, msgs);
	}
	
}
