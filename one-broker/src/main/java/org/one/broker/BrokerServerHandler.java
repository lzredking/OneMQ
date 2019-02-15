/**
 * 
 */
package org.one.broker;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.one.broker.handler.BrokerCahceMsgHandler;
import org.one.broker.handler.BrokerHandler;
import org.one.broker.handler.ComsumerMsgHandler;
import org.one.broker.handler.ConfirMsgHandler;
import org.one.broker.message.CacheMsg;
import org.one.broker.message.ConsumerLog;
import org.one.broker.message.MsgTanscationInfo;
import org.one.broker.message.tanscation.TanscationMessage;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.one.remote.server.RemotingServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.date.DateUtils;
import org.tio.utils.json.Json;

import com.alibaba.fastjson.JSONArray;
import com.one.store.BrokerStore;

/**只做两件事，接受消息，返回消息
 * @author yangkunguo
 *
 */
public class BrokerServerHandler extends RemotingServerHandler{


	private static Logger log=LoggerFactory.getLogger(BrokerServerHandler.class);
	
	private AtomicBoolean isPush=new AtomicBoolean(false);
	
//	public static AtomicLong tansNum=new AtomicLong(0);
	private AtomicLong msgcount=new AtomicLong(0);
	
	private BrokerStore brokerStore;
	
	public static Map<Byte, BrokerHandler> handlers=new HashMap<>();
	
	{

		handlers.put(RequestType.MESSAGE, new BrokerCahceMsgHandler());
		handlers.put(RequestType.CONSUMMER, new ComsumerMsgHandler());
		handlers.put(RequestType.ASKOK, new ConfirMsgHandler());
//		handlers.put(RequestType.PRODUCER, new BrokerCahceMsgHandler());
	}
	
	public BrokerServerHandler(BrokerStore brokerStore) {
		super();
		this.brokerStore = brokerStore;
		
		
	}
	/* (non-Javadoc)
	 * @see org.one.remote.server.RemotingServerHandler#handler(org.tio.core.intf.Packet, org.tio.core.ChannelContext)
	 */
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        byte reqtype = command.getReqType();
        try {
        	if (body != null) {
        		String str = new String(body, Command.CHARSET);
//        		System.out.println(reqtype+"--收到消息：" + str);
        		//请求消息消费成功数据
        		if(reqtype == RequestType.PRODUCER) {
        			OneMessage msg=Json.toBean(str, OneMessage.class);
//        			Map<String, Map<String, OneMessage>> map=CacheMsg.getCacheTopicMsg();
//        			Map<String, Map<String,MsgInfo>> mapc=MsgTanscationInfo.getTansMsgIds();
//        			System.out.println("收到消息事务请求--"+MsgTanscationInfo.getTansMsgSize(msg.getTopic())
//        			+"-没返回事务数："+MsgTanscationInfo.getMsgSize());
        			System.out.println(msgcount.get()+" <==当前消息总数量："+CacheMsg.getSize(msg)+"--已经返回消息数: "+MsgTanscationInfo.tansSize.get()+" 没返回事务数:"+MsgTanscationInfo.getMsgSize());
        			if(CacheMsg.getSize(msg)==0) {
//        				System.out.println(mapc);
//        				MsgTanscationInfo.getMsgIdTopic(id)
//        				System.out.println(MsgTanscationInfo.getTansMsgSize(msg.getTopic()));
//        				System.out.println(MsgTanscationInfo.getTansMsgIds(msg.getTopic(), 10));
        			}
        			//如果有堆积的异常事务消息，在此消化
        			new TanscationMessage().confirmTanscation(channelContext, msg.getTopic());
        		}
        		//接收生产者消息
        		else if(reqtype == RequestType.MESSAGE) {
//        			if("ok".equals(str))return;
//        			System.out.println(DateUtils.formatDateTime(new Date())+"=="+str);
        			handlers.get(reqtype).handler(packet, channelContext);//(channelContext, str);
        			msgcount.addAndGet(1);
        		}
        		//发送消息给消费者
        		else if(reqtype == RequestType.CONSUMMER) {
//        			System.out.println("收到消息消费请求   "+DateUtils.formatDateTime(new Date()));
        			handlers.get(reqtype).handler(packet, channelContext);
        		}
        		//消费成功
        		else if(reqtype == RequestType.ASKOK) {
        			handlers.get(reqtype).handler(packet, channelContext);
//        			tansNum.addAndGet(1);
        		}
        		//消费失败
        		else if(reqtype == RequestType.ASKERR) {
        			log.info("收到消息失败");
        			List<OneConsumer> msgs=JSONArray.parseArray(str, OneConsumer.class);
        			String client=channelContext.getClientNode().toString();
        			
        			//失败消息返回消息池
        			for(OneConsumer msg :msgs) {
        				
        				ConsumerLog.delLogs(client, msg.getTopic(), msg.get_id());
//        				OneMessage omsg=CacheMsg.addCacheMsgNumber(msg.getTopic(), msg.get_id());
        				OneMessage omsg=ConsumerLog.getLogs(client, msg.getTopic(), msg.get_id());
        				if(omsg==null) {
        					System.out.println(msg);
        				}
        				CacheMsg.addCacheTopicMsg(omsg);
        				//通知事务失败
        			}
        			
//        			OneConsumer OneConsumer=msgs.get(0);
//        			send2Comsumer(channelContext,OneConsumer);
        		}else {
        			
        			Command resppacket = new Command();
        			resppacket.setReqType(RequestType.MESSAGE);
//        			resppacket.setBody(("收到了你的消息，你的消息是:" + str).getBytes(Command.CHARSET));
        			Tio.send(channelContext, resppacket);
        		}
        		//返回消息
        	}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	
	
	
	
	
	
}
