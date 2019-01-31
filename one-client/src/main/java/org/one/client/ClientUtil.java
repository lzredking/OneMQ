/**
 * 
 */
package org.one.client;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.one.client.producer.Producer;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.enums.RequestType;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class ClientUtil {


	/**定时任务查询消息
	 * @param channelContext
	 * @param msg
	 * @throws Exception
	 */
	public static boolean send(ChannelContext channelContext,OneConsumer msg){
		Command packet = new Command();
		packet.setReqType(RequestType.CONSUMMER);
		String json=Json.toJson(msg);
		try {
			packet.setBody(json.getBytes(Command.CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return Tio.send(channelContext, packet);
	}
	
	/**消息消费结果返回
	 * @param channelContext
	 * @param ask
	 * @param msg
	 */
	public static boolean askOK(ChannelContext channelContext,boolean ask,List<OneConsumer> msgs)  {
//		System.out.println(channelContext.getServerNode()+"---"+channelContext.isClosed);
		Command packet = new Command();
		packet.setReqType(RequestType.ASKOK);
		if(!ask) {
			packet.setReqType(RequestType.ASKERR);
		}
//		for(OneMessage msg:msgs) {
//			msg.setBody(null);
//		}
		String json=Json.toJson(msgs);
		try {
			packet.setBody(json.getBytes(Command.CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return Tio.send(channelContext, packet);
	}

	/**发送空包消息
	 * @param channelContext
	 * @param msg
	 * @return
	 */
	public static boolean sendNullMessage(ChannelContext channelContext, OneMessage msg) {
		Command packet = new Command();
		packet.setReqType(RequestType.MESSAGE);
		msg.setBody(null);
		String json=Json.toJson(msg);
		try {
			packet.setBody(json.getBytes(Command.CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return Tio.send(channelContext, packet);
		
	}
	
	/**通知Broker,有堆积事务ID可以返回
	 * @param channelContext
	 * @param msg
	 * @return
	 */
	public static boolean sendTansctionMessage(ChannelContext channelContext, OneMessage msg) {
//		System.out.println(msg);
		Command packet = new Command();
		packet.setReqType(RequestType.PRODUCER);
		msg.setBody(null);
		msg.set_id(null);
		String json=Json.toJson(msg);
		try {
			packet.setBody(json.getBytes(Command.CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return Tio.send(channelContext, packet);
		
	}
	
	public static void reqTanscationMsg(ChannelContext channelContext,Producer producer) {
		if(producer.getMsgTanscationListener()!=null) {
			OneMessage msg=new OneMessage(producer.getTopic(), null);
			Map<String, List<ChannelContext>> tanscatChans=ClientInfo.getTanscationChannels();
			for(List<ChannelContext> channelContexts :tanscatChans.values()) {
				for(ChannelContext channelContext2:channelContexts) {
					ClientUtil.sendTansctionMessage(channelContext2, msg);
	//				if(channelContext.getServerNode().equals(channelContext2.getServerNode())) {
	////					System.out.println("--------------------------------------------------------------"+channelContext2);
	//				}
				}
			}	
		}
	}
}
