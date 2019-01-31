/**
 * 
 */
package org.one.broker;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.one.broker.message.CacheMsg;
import org.one.broker.message.ConsumerLog;
import org.one.broker.message.MsgTanscationInfo;
import org.one.broker.message.QueneInfo;
import org.one.broker.message.tanscation.TanscationMessage;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.one.remote.server.RemotingServerHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tio.client.ClientChannelContext;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.date.DateUtils;
import org.tio.utils.json.Json;

import com.alibaba.fastjson.JSONArray;
import com.one.store.BrokerStore;
import com.one.store.file.MappedFile;

/**只做两件事，接受消息，返回消息
 * @author yangkunguo
 *
 */
public class BrokerServerHandler extends RemotingServerHandler{


	private static Logger log=LoggerFactory.getLogger(BrokerServerHandler.class);
	
	private AtomicBoolean isPush=new AtomicBoolean(false);
	
	private AtomicLong tansNum=new AtomicLong(0);
	private AtomicLong msgcount=new AtomicLong(0);
	
	private BrokerStore brokerStore;
	
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
        		//
        		if(reqtype == RequestType.PRODUCER) {
        			OneMessage msg=Json.toBean(str, OneMessage.class);
//        			Map<String, Map<String, OneMessage>> map=CacheMsg.getCacheTopicMsg();
//        			Map<String, Map<String,MsgInfo>> mapc=MsgTanscationInfo.getTansMsgIds();
//        			System.out.println("收到消息事务请求--"+MsgTanscationInfo.getTansMsgSize(msg.getTopic())
//        			+"-没返回事务数："+MsgTanscationInfo.getMsgSize());
        			System.out.println(msgcount.get()+" <==当前消息总数量："+CacheMsg.getSize()+"--已经返回消息数: "+tansNum.get()+" 没返回事务数:"+MsgTanscationInfo.getMsgSize());
        			if(CacheMsg.getSize()==0) {
//        				System.out.println(mapc);
//        				MsgTanscationInfo.getMsgIdTopic(id)
//        				System.out.println(MsgTanscationInfo.getTansMsgSize(msg.getTopic()));
//        				System.out.println(MsgTanscationInfo.getTansMsgIds(msg.getTopic(), 10));
        			}
        			//如果有堆积的异常事务消息，在此消化
        			TanscationMessage.confirmTanscation(channelContext, msg.getTopic());
        		}
        		//接收生产者消息
        		else if(reqtype == RequestType.MESSAGE) {
//        			if("ok".equals(str))return;
//        			System.out.println(DateUtils.formatDateTime(new Date())+"=="+str);
        			saveMsg(channelContext, str);
        			msgcount.addAndGet(1);
        		}
        		//发送消息给消费者
        		else if(reqtype == RequestType.CONSUMMER) {
//        			System.out.println("收到消息消费请求   "+DateUtils.formatDateTime(new Date()));
        			OneConsumer cons=Json.toBean(str, OneConsumer.class);
        			QueneInfo.addTopicChannels(cons.getTopic(), channelContext);
        			//标记已经消费记录
        			send2Comsumer(channelContext,cons);
        		}
        		//消费成功
        		else if(reqtype == RequestType.ASKOK) {
        			List<OneConsumer> msgs=JSONArray.parseArray(str, OneConsumer.class);
        			//通知消息发送者，已经消费成功
        			String client=channelContext.getClientNode().toString();
        			Set<MsgInfo> ids=new LinkedHashSet<>(msgs.size());
        			for(OneConsumer msg :msgs) {
        				ConsumerLog.delLogs(client, msg.getTopic(), msg.get_id());
        				ids.add(new MsgInfo(msg.get_id(), msg.getTopic()));
        				tansNum.addAndGet(1);;
        			}
//        			System.out.println("收到消息确认=="+ids.size());
        			
        			send2Producer(ids);
//        			msg.set_id(null);
        			OneConsumer OneConsumer=msgs.get(0);
        			//发送下一条数据
        			send2Comsumer(channelContext,OneConsumer);
        		}
        		//消费失败
        		else if(reqtype == RequestType.ASKERR) {
        			log.info("收到消息失败");
        			List<OneConsumer> msgs=JSONArray.parseArray(str, OneConsumer.class);
        			String client=channelContext.getClientNode().toString();
        			//失败消息返回消息池
        			for(OneConsumer msg :msgs) {
        				
        				ConsumerLog.delLogs(client, msg.getTopic(), msg.get_id());
        				OneMessage omsg=CacheMsg.addCacheMsgNumber(msg.getTopic(), msg.get_id());
        				if(omsg==null) {
        					System.out.println(msg);
        				}
        				CacheMsg.addCacheTopicMsg(omsg);
        				//通知事务失败
        			}
        			
        			OneConsumer OneConsumer=msgs.get(0);
        			send2Comsumer(channelContext,OneConsumer);
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
	
	/**使用已经连接的通道，返回数据
	 * @param channelContext
	 * @param cons
	 * @throws UnsupportedEncodingException
	 */
	private synchronized void send2Comsumer(ChannelContext channelContext,OneConsumer cons) throws UnsupportedEncodingException {
		String consumer=channelContext.getClientNode().toString();
		if(channelContext.getClientNode().getIp().equals("$UNKNOWN")) {
			return;
		}
		isPush.set(true);
//		System.out.println("消息余数："+CacheMsg.getSize());
    	List<OneMessage> omsg=new ArrayList<>(100);
    	//指定消息ID
    	if(cons.getTopic()!=null && cons.get_id()!=null) {
    		omsg.add(CacheMsg.getCacheMsg(cons.getTopic(), cons.get_id()));
    	}else {
    		//一条
    		if(cons.getReadSize()==null) {
    			cons.setReadSize(1);
    		}//else 
    		{//多条
    			List<OneMessage> msgs=CacheMsg.getCacheTopicMsg(cons.getTopic());
    			if(!msgs.isEmpty()) {
    				synchronized (msgs) {
    					int size=0;
    					for(OneMessage msg:msgs) {
    						
    						if(size==cons.getReadSize())break;
    						
//    						msg=this.brokerStore.readMessage(cons.getTopic(), msg.get_id());
//    						System.out.println(msgs.size()+" = "+msg);
    						if(msg!=null) {
    							omsg.add(msg);
    							ConsumerLog.addLogs(consumer, msg);
    							CacheMsg.removeCacheMsg(msg);
    							size++;
    						}
    					}
					}
//    				if(msgs.size()<=cons.getSize()) {
//    					omsg.addAll(msgs);
//    				}else {
//    				}
    				
//    				for(int i=0;i<cons.getSize();i++) {
//    					OneMessage msg=msgs.get(i);
//    					omsg.add(msg);
//    					ConsumerLog.addLogs(consumer, msg.getTopic(), msg.get_id());
//    				}
    			}else{
//    				System.out.println("消息余数："+CacheMsg.getSize());
    			}
    		}
    	}
    	
    	
//    	log.info("收到消息请求，返回消息："+CacheMsg.getSize()+"--"+cons.getTopic()+"--"+Json.toJson(omsg));
    	Command resppacket = new Command();
    	resppacket.setReqType(RequestType.COMSUMER_MSG);//消费消息
    	if(!omsg.isEmpty()) {
    		boolean isBlank=true;
    		for(OneMessage msg:omsg) {
    			if(msg!=null)
    				isBlank=false;
    		}
    		if(!isBlank) {
    			String json=Json.toJson(omsg);
    			System.out.println(CacheMsg.getSize());
    			resppacket.setBody(json.getBytes(Command.CHARSET));
    		}
    	}
    	boolean ok=Tio.send(channelContext, resppacket);
//    	System.out.println(ok);
    	isPush.set(false);
	}
	
	private synchronized void saveMsg(ChannelContext channelContext,String json) throws UnsupportedEncodingException {
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
		System.out.println(CacheMsg.getCacheTopic(msg.getTopic()));
		if(!CacheMsg.getCacheTopic(msg.getTopic())
				|| CacheMsg.getSize()%100==0) {
			CacheMsg.setCacheTopic(msg.getTopic());
			//注册队列信息
			Command register = new Command();
			register.setReqType(RequestType.BROKER);
			register.setBody(Json.toJson(broker).getBytes(Command.CHARSET));
			for(ClientChannelContext channel: BrokerStart.getRegistChannels()) {
				Tio.send(channel, register);
			}
		}
		System.out.println("消息数量："+CacheMsg.getSize()+"...没返回事务数:"+MsgTanscationInfo.getMsgSize());
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
		if(isPush.get()) {
			return;
		}
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
