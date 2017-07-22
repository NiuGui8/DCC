package com.szlabsun.wqimc.ins.thrift;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import com.szlabsun.wqimc.api.agent.InstrumentAgent;

/**
 * 仪器上下文管理器.
 * 统一管理所有的仪器上下文。
 */
public class InstrumentContextManager {
    public HashMap<UUID, InstrumentContext> contexts;
    private static InstrumentContextManager instance = new InstrumentContextManager();

    private InstrumentContextManager() {
        contexts = new HashMap<UUID, InstrumentContext>();
    }


    public static InstrumentContextManager getInstance() {
        return instance;
    }

    /**
     * 增加一个仪器上下文.
     * @param id 仪器ID。
     * @param context 仪器上下文对象。
     */
    public void addContext(UUID id, InstrumentContext context) {
        contexts.put(id,  context);
    }

    /**
     * 删除指定仪器的上下文.
     */
    public void removeContext(UUID id) {
        contexts.remove(id);
    }

    /**
     * 清空所有仪器的上下文.
     */
    public void clear() {
        contexts.clear();
    }

    /**
     * 获取指定仪器的上下文对象.
     * @param id 仪器ID。
     * @return 仪器上下文对象。
     */
    public InstrumentContext getContext(UUID id) {
        return contexts.get(id);
    }

    /**
     * 获取指定仪器的反向调用 Stub.
     * @param id 仪器ID。
     * @return 反向调用 Stub对象。
     */
    public InstrumentAgent.Client getClient(UUID id) {
        InstrumentAgent.Client client = null;
        InstrumentContext context = contexts.get(id);
        if (context != null) {
            client = context.client;
        }
        return client;
    }

    /**
     * 获取仪器上下文映射表的迭代器，以便遍历所有的仪器上下文.
     */
    public Iterator<Map.Entry<UUID, InstrumentContext>> getContextIterator() {
        return contexts.entrySet().iterator();
    }
    
    public int getContextNum() {
        return contexts.size();
    }
}