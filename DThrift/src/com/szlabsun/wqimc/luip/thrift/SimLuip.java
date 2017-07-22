package com.szlabsun.wqimc.luip.thrift;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.szlabsun.wqimc.api.manager.Instrument;
import com.szlabsun.wqimc.api.manager.InstrumentManager;

public class SimLuip {
    private final static Logger LOGGER = LoggerFactory.getLogger("SimLuip");
    
    public static void main(String[] args) {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");

        // 命令行参数配置
        Options options = new Options();
        options.addOption("s", "server", true, "Server host/ip [localhost]");
        options.addOption("p", "port", true, "Server port [9090]");
        options.addOption("i", "interval", true, "Call interval time(ms) [1000]");
        options.addOption("d", "id", true, "Instrument ID [1]");
        options.addOption("n", "name", true, "Instrument Name [COD]");
        options.addOption("t", "type", true, "Instrument Type [PT62-COD]");
        options.addOption("c", "count", true, "Test count [INF]");
        options.addOption("v", "version", false, "Show version information");
        options.addOption("h", "help", false, "Show usage");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SimLuip", options );
            return;
        }
        // 命令行参数解析
        String value = null;
        String server = "localhost";
        int port = 9090;
        int callInterval = 1000;
        long instrumentID = 1;
        String instrumentName = "COD";
        String instrumentType = "PT62-COD";
        long testCount = Long.MAX_VALUE;
        if (cmd.hasOption("v")) {
            System.out.println("SimLuip V1.0.0");
            return;
        }
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "SimLuip", options );
            return;
        }
        if (cmd.hasOption("s")) {
            server = cmd.getOptionValue("s");
        }
        if (cmd.hasOption("p")) {
            value = cmd.getOptionValue("p");
            port = Integer.parseInt(value);
        }
        if (cmd.hasOption("i")) {
            value = cmd.getOptionValue("i");
            callInterval = Integer.parseInt(value);
        }
        if (cmd.hasOption("d")) {
            value = cmd.getOptionValue("d");
            instrumentID = Integer.parseInt(value);
        }
        if (cmd.hasOption("n")) {
            instrumentName = cmd.getOptionValue("n");
        }
        if (cmd.hasOption("t")) {
            instrumentType = cmd.getOptionValue("t");
        }
        if (cmd.hasOption("c")) {
            value = cmd.getOptionValue("c");
            testCount = Long.parseLong(value);
        }
        LOGGER.info("Instrument[{},{},{}]", instrumentType, instrumentName, instrumentID);
        
    	CloudConnectionService service = CloudConnectionService.getInstance();
    	service.setServer(server, port);

    	if (service.start()) {
        	InstrumentManager.Client client = service.getClient();
        	UUID id = new UUID(0x2222333377778888L, instrumentID);
        	ByteBuffer buf = ByteBuffer.allocate(16);
        	buf.putLong(id.getMostSignificantBits());
            buf.putLong(id.getLeastSignificantBits());
            buf.flip();
            
            // 上传，注册
            Instrument instrument = new Instrument();
            instrument.uuid = buf;
            instrument.name = instrumentName;
            instrument.type = instrumentType;
            boolean ret;
            try {
                ret = client.upload(instrument);
                LOGGER.info("upload({}) RET: {}", id.toString(),
                        ret);
            } catch (TException e1) {
                LOGGER.info("upload({}) TException:{}", id.toString(),
                        e1.toString());
            }

            long num = 0;
            long errorCount = 0;
            String reply;
            String echoData;
            int reportCount = 5 * (int)(1000.0 / callInterval); // 5 秒报告一次
            reportCount = (reportCount <= 0) ? 1 : reportCount;
            while (num <= testCount) {
                num++;
                try {
                    echoData = "L[" + id.toString() + "]-" + num;
                    reply = client.echo(echoData);
                    LOGGER.debug("CALL: echo({}),  RET: {}", echoData, reply);
                    Thread.sleep(callInterval);
                } catch (TTransportException e) {
                    LOGGER.error("RET: TException", e);
                    LOGGER.error("Call ERROR! count:{}", ++errorCount);
                    if (e.getType() == TTransportException.NOT_OPEN) {
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.error("RET: Exception, Exit!", e);
                    LOGGER.error("Call ERROR! count:{}", ++errorCount);
                    break;
                }
                
                if (num % reportCount == 0) {
                    LOGGER.info("Testing...  TestNum={}, ErrorCount={}", num, errorCount);
                }
            }

            LOGGER.info("Test finish! TestNum={}, ErrorCount={}", num, errorCount);
        	service.stop();
    	}
    	
    }

}
