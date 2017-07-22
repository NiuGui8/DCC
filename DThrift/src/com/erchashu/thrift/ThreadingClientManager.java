package com.erchashu.thrift;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.server.ServerContext;

import com.erchashu.thrift.DuplexCallServer.DuplexFrameBuffer;

/**
 * 线程动态关联的客户端管理器.
 * 
 * 保存线程相关对象，以便于在RPC服务接口实现代码中方便取得关联的客户端对象。
 */
public class ThreadingClientManager {
    private static ThreadingClientManager instance = new ThreadingClientManager();
    protected HashMap<Long, Object> invokeContexts;
    
    private ThreadingClientManager() {
        invokeContexts = new HashMap<Long, Object>();
    }

    public static ThreadingClientManager getInstance() {
        return instance;
    }

    /**
     * 关联的客户端 FrameBuffer 到当前线程. 在服务器每次处理 RPC 请求前，
     * 都将调用调用本函数与当前线程关联，以便于在 RPC 实现函数中，
     * 取得客户端关联的上下文。
     * @param fb 要附加的客户端 FrameBuffer。
     */
    public void attachFrameBuffer(DuplexFrameBuffer fb) {
        invokeContexts.put(Thread.currentThread().getId(), fb);
    }

    /**
     * 关联的客户端 Socket 到当前线程. 在服务器每次处理客户端连接前，
     * 都将调用调用本函数与当前线程关联，以便于用户在 {@link TServerEventHandler.createContext()} 
     * 的实现函数中，取得线程关联的客户端 Socket。
     * @param fb 要附加的客户端 FrameBuffer。
     */
    public void attachSocket(Socket socket) {
        invokeContexts.put(Thread.currentThread().getId(), socket);
    }

    /**
     * 取消当前线程的关联对象. 
     */
    public void detach() {
        invokeContexts.remove(Thread.currentThread().getId());
    }

    /**
     * 获取线程关联的客户端 Socket. 
     * 仅在调用  {@link attachSocket()} 后，在 {@link TServerEventHandler.createContext()}
     * 的实现函数中调用才有效。
     * @return 线程关联的客户端 FrameBuffer。
     */
    public Socket getSocket() {
        return (Socket)invokeContexts.get(Thread.currentThread().getId());
    }

    /**
     * 获取线程关联的客户端 FrameBuffer. 
     * 仅在调用  {@link attachFrameBuffer()} 后，在 RPC 实现函数中调用才有效。
     * @return 线程关联的客户端 FrameBuffer。
     */
    public DuplexFrameBuffer getFrameBuffer() {
        return (DuplexFrameBuffer)invokeContexts.get(Thread.currentThread().getId());
    }


    /**
     * 获取线程关联的客户端上下文. 
     * 仅在调用  {@link attachFrameBuffer()} 后，在 RPC 实现函数中调用才有效。
     * @return 客户端上下文。
     */
    public ServerContext getContext() {
        DuplexFrameBuffer fb = (DuplexFrameBuffer)invokeContexts.get(Thread.currentThread().getId());
        ServerContext context = null;
        if (fb != null) {
            context = fb.getContext();
        }
        return context;
    }

    /**
     * 获取线程关联的客户端的反向调用 Stub 对象. 
     * 仅在调用  {@link attachFrameBuffer()} 后，在 RPC 实现函数中调用才有效。
     * @return 客户端反向调用 Stub。
     */
    public TServiceClient getClient() {
        DuplexFrameBuffer fb = (DuplexFrameBuffer)invokeContexts.get(Thread.currentThread().getId());
        TServiceClient client = null;
        if (fb != null) {
            client = fb.getClient();
        }
        return client;
    }

    /**
     * 获取线程关联的客户端 Socket 地址. 
     * 仅在调用  {@link attachFrameBuffer()} 后，在 RPC 实现函数中调用才有效。
     * @return 客户端反向调用 Stub。
     */
    public InetSocketAddress getClientAddress() {
        DuplexFrameBuffer fb = (DuplexFrameBuffer)invokeContexts.get(Thread.currentThread().getId());
        InetSocketAddress address = null;
        if (fb != null) {
            Socket socket = fb.getSocket();
            if (socket != null) {
                address = (InetSocketAddress)socket.getRemoteSocketAddress();
            }
        }
        return address;
    }

}