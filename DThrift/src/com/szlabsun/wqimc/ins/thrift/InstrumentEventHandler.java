package com.szlabsun.wqimc.ins.thrift;

import java.net.Socket;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erchashu.thrift.ThreadingClientManager;

public class InstrumentEventHandler implements TServerEventHandler {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
    private static InstrumentEventHandler instance = new InstrumentEventHandler();
    
    private InstrumentEventHandler() {
    }

    public static InstrumentEventHandler getInstance() {
        return instance;
    }

    @Override
    public void preServe() {
        
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
        Socket socket = ThreadingClientManager.getInstance().getSocket();
        if (socket != null) {
            LOGGER.info("Client {} connected.", socket.getRemoteSocketAddress().toString());
        } else {
            LOGGER.warn("Client connected.");
        }
        return new InstrumentContext();
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
        // 客户端连接断开，删除该客户端上下文
        InstrumentContext context = (InstrumentContext)serverContext;
        InstrumentContextManager.getInstance().removeContext(context.getId());
        LOGGER.info("Client {} disconnected, remove it.", context.getId().toString());
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
    }

}
