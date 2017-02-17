package com.semantix;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    TODO
    - Maybe will be necessary to create a class for kerberos and follow the
    hdfs sink
 */

public class CustomSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(CustomSink.class);
    private String JDBC_DRIVER_NAME;
    public Connection con = null;
    private String ip;
    private String port;
    private String kerberosKeytab;
    private String kerberosPrincipal;
    private String database;
    private String table;
    private String kerberosFqdn;
    private String kerberosLogLevel;
    private String kerberosLogPath;
    private String kerberosRealm;
    private String jdbcDriver;

    public void configure(Context context) {
        String ip = context.getString("ip","localhost");
        String port = context.getString("port","21050");
        String kerberosKeytab = context.getString("kerberosKeytab");
        String kerberosPrincipal = context.getString("kerberosPrincipal");
        String JDBC_DRIVER_NAME = context.getString("jdbcDriverName","com.cloudera.impala.jdbc4.Driver");
        String database = context.getString("database");
        String table = context.getString("table");
        String kerberosFqdn = context.getString("kerberosFqdn");
        String kerberosLogLevel = context.getString("kerberosLogLevel");
        String kerberosLogPath = context.getString("kerberosLogPath");
        String kerberosRealm = context.getString("kerberosRealm");

        LOG.info("IP: " + ip);
        LOG.info("PORT: " + port);
        LOG.info("kerberosPrincipal: " + kerberosPrincipal);
        LOG.info("kerberosKeytab: " + kerberosKeytab);
        LOG.info("JDBC driver name: " + JDBC_DRIVER_NAME);
        LOG.info("Database: " + database);
        LOG.info("Table: "+ table);
        LOG.info("Kerberos FQDN (Fully qualified domain name): " + kerberosFqdn);
        LOG.info("kerberos log level: " + kerberosLogLevel);
        LOG.info("Kerberos log path: "+ kerberosLogPath);
        LOG.info("Kerberos realm: " + kerberosRealm);

        this.ip = ip;
        this.port = port;
        this.kerberosPrincipal = kerberosPrincipal;
        this.kerberosKeytab = kerberosKeytab;
        this.JDBC_DRIVER_NAME = JDBC_DRIVER_NAME;
        this.database = database;
        this.table = table;
        this.kerberosFqdn = kerberosFqdn;
        this.kerberosLogLevel = kerberosLogLevel;
        this.kerberosLogPath = kerberosLogPath;
        this.kerberosRealm = kerberosRealm;
    }

    @Override
    public void start() {
        try {
            // Authenticating Kerberos principal
            if(kerberosPrincipal != null){
                LOG.info("Principal Authentication: ");
//                final String user = "cloudera@CLOUDERA.COM";
//                final String keyPath = "cloudera.keytab";
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
                Class.forName(JDBC_DRIVER_NAME);
                String krbRealm = kerberosPrincipal.split("@")[1];
                LOG.info("krb Realm"+ krbRealm);
                LOG.info("STARTING CONNECTION WITH IMPALA");

                con = DriverManager.getConnection("jdbc:impala://"+ip+":"+port+";AuthMech="+ 1+";KrbRealm= "+
                        krbRealm +";HostFQDN="+kerberosFqdn+";KrbServiceName=impala; LogLevel="+ kerberosLogLevel+";LogPath="+kerberosLogPath);
            }
            else {
                // Set JDBC Impala Driver
                LOG.info("STARTING CONNECTION WITH IMPALA");
                con = DriverManager.getConnection("jdbc:impala://" + ip + ":" + port);
            }
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

        Status status =  Status.BACKOFF;
        Transaction txn = null;

        try {
            txn = getChannel().getTransaction();
            txn.begin();
            Event event = getChannel().take();
            //event.toString();
            if(event != null) {
                byte[] eventContent = event.getBody();
                LOG.info("EVENT CONTENT: " + new String(eventContent));
                String[] fields = new String(eventContent).split(",");
                LOG.info("BEFORE CREATION OF STATEMENT");
                Statement stmt = con.createStatement();
                LOG.info("AFTER CREATION OF STATEMENT");
                String alarme = "\"" + fields[0] + "\"";
                String objeto = "\"" + fields[1] + "\"";
                //int
                String status_content = fields[2];
                //timestamp
                String startts = "\"" + fields[3] + "\"";
                //timestamp
                String endts = "\"" + fields[4] + "\"";
                String urgency = "\"" + fields[5] + "\"";
                //            String yearMonth = startts.split("-")[0].substring(1) + startts.split("-")[1];
                //            LOG.info("YEARMONTH" + yearMonth);

                LOG.info("STATEMENT: " + "INSERT INTO" + database +"." + table +"  values (" + alarme + "," + objeto + "," +
                        status_content + "," + startts + "," + endts + "," + urgency + ")");
                String statement = "INSERT INTO" + database +"." + table +"  values (" + alarme + "," + objeto + "," + status_content
                        + "," + startts + "," + endts + "," + urgency + ");";

                String fixed = statement.replace("\n","");
                LOG.info("FIXED: "+ fixed);
                stmt.execute(fixed);
            }
            txn.commit();
            status = Status.READY;
        }
        catch (Exception e) {
            e.printStackTrace();
            if(txn != null){
                txn.rollback();
            }
            status = Status.BACKOFF;
//            throw new EventDeliveryException("Error while processing " + "data", e);
        }
        finally {
            if(txn != null){
                txn.close();
            }
        }
        return status;
    }
}
