/**
 * 
 */
package org.one.broker.handler;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.one.broker.BrokerServerHandler;
import org.one.broker.message.ConsumerLog;
import org.one.broker.message.MsgTanscationInfo;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

import com.alibaba.fastjson.JSONArray;

/**
 * @author yangkunguo
 *
 */
public class ConfirMsgHandler implements BrokerHandler{

	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        if(body!=null) {
        	String json = new String(body, Command.CHARSET);
        	List<OneConsumer> msgs=JSONArray.parseArray(json, OneConsumer.class);
        	//通知消息发送者，已经消费成功
        	String client=channelContext.getClientNode().toString();
        	Set<MsgInfo> ids=new LinkedHashSet<>(msgs.size());
        	for(OneConsumer msg :msgs) {
        		ConsumerLog.delLogs(client, msg.getTopic(), msg.get_id());
        		ids.add(new MsgInfo(msg.get_id(), msg.getTopic()));
//        		tansNum.addAndGet(1);
        	}
//		System.out.println("收到消息确认=="+ids.size());
        	
        	send2Producer(ids);
//		msg.set_id(null);
        	OneConsumer OneConsumer=msgs.get(0);
        	//发送下一条数据
        	Command cmd = new Command();
        	cmd.setBody(Json.toJson(OneConsumer).getBytes(Command.CHARSET));
        	BrokerServerHandler.handlers.get(RequestType.CONSUMMER).handler(cmd, channelContext);
//        	send2Comsumer(channelContext,OneConsumer);
        }
		
	}

	/**消费确认
	 * @param msgid
	 * @throws UnsupportedEncodingException
	 */
	void send2Producer(Set<MsgInfo> msgs) throws UnsupportedEncodingException {
		MsgTanscationInfo.addTansMsgIds(msgs);
//		ChannelContext chan=MsgTanscationInfo.getMsgIdChannels(ids.get(0));
//		for(String id:ids)
//			new TanscationMessage().tanscationMsg(chan , id);
	}
}
