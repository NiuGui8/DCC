package com.szlabsun.wqimc.ins.thrift;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.szlabsun.wqimc.api.agent.InstrumentAgent;

public class SimServer {
    private final static Logger LOGGER = LoggerFactory.getLogger("SimServer");
    
    public static void main(String[] args) {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");

        // 命令行参数配置
        Options options = new Options();
        options.addOption("p", "port", true, "Listen port [9090]");
        options.addOption("i", "interval", true, "Call interval time(ms) [1000]");
        options.addOption("c", "count", true, "Test count [INF]");
        options.addOption("v", "version", false, "Show version information");
        options.addOption("h", "help", false, "Show usage");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SimServer", options );
            return;
        }
        // 命令行参数解析
        String value = null;
        int port = 9090;
        int callInterval = 1000;
        long testCount = Long.MAX_VALUE;
        if (cmd.hasOption("v")) {
            System.out.println("SimServer V1.0.0");
            return;
        }
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SimServer", options );
            return;
        }
        if (cmd.hasOption("p")) {
            value = cmd.getOptionValue("p");
            port = Integer.parseInt(value);
        }
        if (cmd.hasOption("i")) {
            value = cmd.getOptionValue("i");
            callInterval = Integer.parseInt(value);
        }
        if (cmd.hasOption("c")) {
            value = cmd.getOptionValue("c");
            testCount = Long.parseLong(value);
        }
        
        InstrumentCloudService service = InstrumentCloudService.getInstance();
        service.setListenPort(port);
        service.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        InstrumentContextManager manager = InstrumentContextManager.getInstance();
        long num = 0;
        long errorCount = 0;
        String reply;
        String echoData;
        int reportCount = 5 * (int)(1000.0 / callInterval); // 5 秒报告一次
        reportCount = (reportCount <= 0) ? 1 : reportCount;
        while (num <= testCount) {
            num++;
            if (manager.getContextNum() > 0) {
                Iterator<Map.Entry<UUID, InstrumentContext>> iterator = manager.getContextIterator();
                while (iterator.hasNext()) {
                    Map.Entry<UUID, InstrumentContext> entry = iterator.next();
                    UUID uuid = entry.getKey();
                    InstrumentAgent.Client client = entry.getValue().getClient();
                    
                    try {
                        echoData = "S[" + uuid.toString() + "]-" + num;
                        reply = client.echo(echoData);
                        LOGGER.debug("RCALL: echo({}),  RET: {}", echoData, reply);
                    } catch (Exception e) {
                        LOGGER.error("RET: Exception", e);
                        LOGGER.error("Call ERROR! count:{}", ++errorCount);
                    }
                }
                
                try {
                    Thread.sleep(callInterval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
                if (num % reportCount == 0) {
                    LOGGER.info("Testing...  TestNum={}, ErrorCount={}", num, errorCount);
                }
                
            } else {
                // 没有在线的客户端
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        LOGGER.info("Test finish! TestNum={}, ErrorCount={}", num, errorCount);
    }
}
