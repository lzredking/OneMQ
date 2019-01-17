/**
 * 
 */
package org.one.client;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;

import org.one.client.consumer.Consumer;
import org.one.client.producer.Producer;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;

/**检查Broker连接状态
 * @author yangkunguo
 *
 */
public class CheckBroker {

//	private Byte reqType;
	Object obj=null;
	public CheckBroker(Object obj) {
		this.obj=obj;
	}
	/**启动检查Broker连接状态
	 * @param type
	 */
	public void start(Byte type) {
//		reqType=type;
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				System.out.println(".....");
				while(true) {
					try {
						Thread.sleep(1000*10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					Map<String, ChannelContext> chans=ClientInfo.getBrokerChannelsMap();
					Map<String, ChannelContext> tanscatChans=ClientInfo.getTanscationChannels();
					Collection<OneBroker> brokers=ClientInfo.getBrokers().values();
					System.out.println("check borker...size..."+brokers.size());
					
					
					long time=System.currentTimeMillis();
					for(OneBroker broker:brokers) {
						ChannelContext channel=chans.get(broker.getBrokerName());
						ChannelContext channel2=tanscatChans.get(broker.getBrokerName());
						System.out.println(channel+"...check borker channel isClosed..."+channel.isClosed);
						if(chans.get(broker.getBrokerName()).isClosed) {
							System.out.println("尝试重连下线Broker...");
							if(obj instanceof Consumer) {
								ClientUtil.send(channel, new OneConsumer(((Consumer) obj).getTopic()));
							}else if(obj instanceof Producer) {
								ClientUtil.sendNullMessage(channel, new OneMessage(((Producer) obj).getTopic(),null));
								//Tascation
								Producer producer=(Producer) obj;
								if(producer.getMsgTanscationListener()!=null) {
									OneMessage msg=new OneMessage(producer.getTopic(), null);
									ClientUtil.sendTansctionMessage(channel2, msg);
								}
							}
							
						}
						//
						else{
							
							if(channel==null || channel.isClosed) {
								ClientInfo.removeBroker(broker);
								continue;
							}
							if(obj instanceof Consumer) {
//								ClientUtil.send(channel, new OneConsumer(((Consumer) obj).getTopic()));
							}
							else if(obj instanceof Producer) {
								Producer producer=(Producer) obj;
								ClientUtil.sendTansctionMessage(channel, new OneMessage(producer.getTopic(),null));
								producer.handlerFailedMsg(channel);
								//Tascation
								if(producer.getMsgTanscationListener()!=null) {
									OneMessage msg=new OneMessage(producer.getTopic(), null);
									ClientUtil.sendTansctionMessage(channel2, msg);
								}
							}
//							Command command = new Command();
////							if(reqType == RequestType.PRODUCER) {
////								command.setReqType(RequestType.MESSAGE);
////							}else {
////								command.setReqType(RequestType.CONSUMMER);
////							}
//							command.setReqType(RequestType.HEART_BEAT);
//							try {
//								command.setBody("ok".getBytes(Command.CHARSET));
//							} catch (UnsupportedEncodingException e) {
//								e.printStackTrace();
//							}
//							Tio.send(chans.get(broker.getBrokerName()), command);
						}
						
						if(time>(broker.getLastTime()+1000*60)) {
							//删除下线Broker
							if(chans.get(broker.getBrokerName()).isClosed) {
								System.out.println("删除下线Broker...");
								ClientInfo.removeBroker(broker);
								if(obj instanceof Consumer) {
									Consumer consumer=(Consumer) obj;
									try {
										consumer.registerConsumer();
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							}
							//转移Broker里的消息
						}
					}
				}
				
				
			}
		}).start();
	}

}
