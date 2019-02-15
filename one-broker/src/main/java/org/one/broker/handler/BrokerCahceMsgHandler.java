/**
 * 
 */
package org.one.broker.handler;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.one.broker.BrokerStart;
import org.one.broker.message.CacheMsg;
import org.one.broker.message.MsgTanscationInfo;
import org.one.broker.message.QueneInfo;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.tio.client.ClientChannelContext;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class BrokerCahceMsgHandler implements BrokerHandler{

	
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        if (body != null) {
    		String json = new String(body, Command.CHARSET);
    		saveMsg(channelContext,json);
        }
	}
	
	private synchronized void saveMsg(ChannelContext channelContext,String json) throws UnsupportedEncodingException, InterruptedException {
		OneBroker broker=BrokerStart.getBroker();
		OneMessage msg=Json.toBean(json, OneMessage.class);
		broker.setQueueName(msg.getTopic());
		//消息缓存
		if(msg.get_id()==null) {
			System.out.println(msg);
			return;
		}
		CacheMsg.addCacheTopicMsg(msg);
//		brokerStore.putMessage(msg);
		
		//检查是否注册过
//		System.out.println(CacheMsg.getCacheTopic(msg.getTopic()));
		if(!CacheMsg.getCacheTopic(msg.getTopic())
				|| CacheMsg.getSize(msg)%100==0) {
			CacheMsg.setCacheTopic(msg.getTopic());
			//注册队列信息
			Command register = new Command();
			register.setReqType(RequestType.BROKER);
			register.setBody(Json.toJson(broker).getBytes(Command.CHARSET));
			for(ClientChannelContext channel: BrokerStart.getRegistChannels()) {
				Tio.send(channel, register);
			}
		}
		System.out.println("消息数量："+CacheMsg.getSize(msg)+"...没返回事务数:"+MsgTanscationInfo.getMsgSize());
		//确认收到消息
		if(msg.getConfirm()==1) {
			confirmMsg(channelContext,msg.get_id());
		}
		//确认消费消息
		if(msg.getTranscation()==1) {
			MsgTanscationInfo.addMsgIdTopic(msg.get_id(), msg.getTopic());
		}
		//如果有堆积的异常消息，在此消化
//		TanscationMessage.confirmTanscation(channelContext, msg.getTopic());
		
		send2Client(msg);
	}
	
	/**收到消息后马来上推送空包
	 * @param msg
	 */
	void send2Client(OneMessage msg) {
//		if(isPush.get()) {
//			return;
//		}
		Command resppacket = new Command();
    	resppacket.setReqType(RequestType.COMSUMER_MSG);//消费消息
    	Set<ChannelContext> channelContexts=QueneInfo.getTopicChannels(msg.getTopic());
    	List<ChannelContext> list=new ArrayList<>();
    	
    	if(channelContexts.size()>0)
    		list.addAll(channelContexts);
    	
    	ChannelContext channel=null;
    	if(list.size()==1) {
    		channel=list.get(0);
    	}else if(list.size()>1){
    		Random ra =new Random();
    		channel=list.get(ra.nextInt(list.size()-1));
    	}
//    	try {
//			resppacket.setBody(Json.toJson(msg).getBytes(Command.CHARSET));
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		}
    	Tio.send(channel, resppacket);
	}
	
	/**
	 * @param channelContext
	 * @param id
	 * @throws UnsupportedEncodingException
	 */
	private void confirmMsg(ChannelContext channelContext,String id) throws UnsupportedEncodingException {
		Command resppacket = new Command();
		resppacket.setReqType(RequestType.MSG_CONFIRM);
		resppacket.setBody(id.getBytes(Command.CHARSET));
		Tio.send(channelContext, resppacket);
	}
	
}
