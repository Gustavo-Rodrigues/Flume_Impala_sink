package com.semantix;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSink.class);
    private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc4.Driver";
    private static String connectionUrl = "jdbc:impala://localhost:21050";
    public Connection con = null;
    private String ip;
    private String port;

    public void configure(Context context) {
        String ip = context.getString("ip","localhost");
        String port = context.getString("port","21050");
        LOG.info("IP: " + ip);
        LOG.info("PORT: " + port);
        this.ip = ip;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            // Set JDBC Impala Driver
            Class.forName(JDBC_DRIVER_NAME);
            LOG.info("STARTING CONNECTION WITH IMPALA");
            con = DriverManager.getConnection(connectionUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void stop () {
        try {
            LOG.info("CLOSING CONNECTION");
            con.close();
        } catch (SQLException sql){
            sql.printStackTrace();
        }

    }

    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        try {
            txn.begin();
            Event event = ch.take();
            //event.toString();
            byte[] eventContent = event.getBody();
            LOG.info("EVENT CONTENT: " + new String(eventContent));
            String[] fields = new String(eventContent).split(",");
            Statement stmt = con.createStatement();
            String alarme = "\"" +fields[0] + "\"";
            String objeto = "\"" +fields[1] + "\"";
            //int
            String status_content = fields[2];
            //timestamp
            String startts = fields[3];
            //timestamp
            String endts = fields[4];
            String urgency = "\"" +fields[5] + "\"";

            LOG.info("STATEMENT: " +"INSERT INTO claro.slaview values ("+ alarme + "," + objeto + "," +
                    status_content + "," + startts + "," + endts + "," + urgency + ")");
            stmt.execute("INSERT INTO claro.slaview values ("+ alarme + "," + objeto + "," + status_content
                    + "," + startts + "," + endts + "," + urgency + ");");
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            if(txn != null){
                txn.rollback();
            }
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            if(txn != null){
                txn.close();
            }
        }
        return status;
    }
}
