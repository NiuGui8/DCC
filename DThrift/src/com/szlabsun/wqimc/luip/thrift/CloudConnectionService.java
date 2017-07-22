package com.szlabsun.wqimc.luip.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erchashu.thrift.DuplexClientService;
import com.erchashu.thrift.CallFramedTransport;
import com.szlabsun.wqimc.api.agent.InstrumentAgent;
import com.szlabsun.wqimc.api.manager.InstrumentManager;

/**
 * 云端连接服务。
 */
public class CloudConnectionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudConnectionService.class.getName());
    private static CloudConnectionService instance = new CloudConnectionService();

    // 服务响应对象链
    private InstrumentAgentHandler handler;
    private TProcessor processor;
    
    // 客户端调用对象链
    private CallFramedTransport transport;
    private TProtocol protocol;
    private InstrumentManager.Client client;
    private DuplexClientService bidirectionalService;
    
    private String serverHost = "localhost";
    private int serverPort = 9090;
    
    private CloudConnectionService() {
    }

    public static CloudConnectionService getInstance() {
        return instance;
    }
    
    InstrumentManager.Client getClient() {
    	return client;
    }
    
    /**
     * 设置服务器连接参数.
     *
     * @param host 服务器主机名/IP地址。
     * @param port 服务端口号。
     */
    public void setServer(String host, int port) {
        serverHost = host;
        serverPort = port;
    }
    
    public boolean start() {
    	boolean ret = false;
    	
        // 客户端调用链
        transport = new CallFramedTransport(5000);
        protocol = new  TCompactProtocol(transport);
        client = new InstrumentManager.Client (protocol);
        bidirectionalService = new DuplexClientService(transport);
        bidirectionalService.setServer(serverHost, serverPort);

        // 客户端提供的反向 RPC 服务处理链
        handler = new InstrumentAgentHandler();
        processor = new InstrumentAgent.Processor<InstrumentAgentHandler>(handler);
        bidirectionalService.setServiceArgs(new DuplexClientService.Args(processor)
                .protocolFactory(new TCompactProtocol.Factory())
                .transportFactory(new TFramedTransport.Factory())
                );

        try {
			bidirectionalService.connect();
	        ret = true;
		} catch (TTransportException e) {
			LOGGER.error("Connect to server fault!", e);
		}
        
        if (ret) {
	        bidirectionalService.start();
        }
        
        return ret;
    }

    public void stop() {
    	bidirectionalService.disconnect();
    }
}
