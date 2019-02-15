/**
 * 
 */
package test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.one.client.producer.MsgConfirmListener;
import org.one.client.producer.MsgTanscationListener;
import org.one.client.producer.Producer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.producer.SendCache;

/**
 * @author yangkunguo
 *
 */
public class ProducerDemo {

	private static AtomicLong sendNum= new AtomicLong(0);
	private static AtomicLong sendOK= new AtomicLong(0);
	private static AtomicLong resNum= new AtomicLong(0);
	/**测试
	 * @param s
	 */
	public static void main(String []s) {
		Producer producer=new Producer("producer-1");
		producer.setTopic("topic-1");
		producer.setRegisterServer("127.0.0.1:6666");
//		producer.setTanscation(true);
		producer.setMsgConfirmListener(new MsgConfirmListener() {
			
			@Override
			public void confirm(String id) {
				// 发送成功的消息
				sendOK.incrementAndGet();
//				System.out.println(id);
				System.out.println("确认发送成功："+sendOK.get());
			}
		});
		
		producer.setMsgTanscationListener(new MsgTanscationListener() {
			
			@Override
			public void tanscation(List<MsgInfo> ids) {
				// 消费成功
				resNum.addAndGet(ids.size());
//				System.out.println(id);
				for(MsgInfo msg:ids)
					SendCache.getSendMsgs().remove(msg.getId());
				System.out.println(resNum.get()+" 确认消费成功："+ids.size());
				System.out.println("发送成功数："+sendOK.get()+"=="+sendNum.get()+" 没收到事务消息数："+SendCache.getSendMsgs().size());
			}
		});
		producer.start();
		
		Thread th=new Thread(new Runnable() {
			
			@Override
			public void run() {
				int c=0;
				while(true) {
					long s=System.currentTimeMillis();
					try {
						for(int i=0;i<100;i++) {
							
							long num=sendNum.addAndGet(1);
							
							OneMessage msg=producer.sendMessage("test");
							
							if(msg!=null) {
								SendCache.addSendMsgs(msg);
							}
						}
						Thread.sleep(10);
						System.out.println(sendNum.get()+"--消息发送完毕：");
						System.out.println("没收到事务消息数=="+SendCache.getSendMsgs().size());
						long e=System.currentTimeMillis();
						System.out.println("时间："+(e-s));
						if((sendNum.get())>=1000000) {
							break;
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					c++;
				}
				
			}
		});
		th.start();
	}
}
