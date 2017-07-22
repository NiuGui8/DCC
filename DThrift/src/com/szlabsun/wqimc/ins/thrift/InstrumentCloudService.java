package com.szlabsun.wqimc.ins.thrift;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import com.erchashu.thrift.DuplexCallServer;
import com.szlabsun.wqimc.api.manager.InstrumentManager;

/**
 * 仪器云服务。
 */
public class InstrumentCloudService {
    private static InstrumentCloudService instance = new InstrumentCloudService();

    private InstrumentManagerHandler handler;
    private TProcessor processor;
    private DuplexCallServer server;
    private int listenPort;
    
    private InstrumentCloudService() {
        listenPort = 9090;
    }

    public static InstrumentCloudService getInstance() {
        return instance;
    }
    
    public void setListenPort(int port) {
        listenPort = port;
    }
    
    public int getListenPort() {
        return listenPort;
    }
    
    public void start() {
        try {
            handler = new InstrumentManagerHandler();
            processor = new InstrumentManager.Processor<InstrumentManagerHandler>(handler);
            
            TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(listenPort);
            server = new DuplexCallServer(
                    new DuplexCallServer.Args(serverTransport)
                    .processor(processor)
                    .protocolFactory(new TCompactProtocol.Factory())
                    .transportFactory(new TFramedTransport.Factory())
                    .selectorThreads(4)
                    .workerThreads(50)
                    );
            server.setServerEventHandler(InstrumentEventHandler.getInstance());
            server.setClientFactory(new InstrumentAgentClientFactory());
            server.setClientProtocolFactory(new TCompactProtocol.Factory());
            server.setClientCallTimeout(5000);
        } catch (TTransportException e) {
            e.printStackTrace();
        }

        Runnable simple = new Runnable() {
            public void run() {
                System.out.printf("Starting service on [%d] ...\n", listenPort);
                server.serve();
            }
        };

        new Thread(simple).start();
    }

    public void stop() {
        server.stop();
    }
}
