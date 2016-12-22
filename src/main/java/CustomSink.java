import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class CustomSink extends AbstractSink implements Configurable {
    private static final Logger logger = Logger.getLogger("CustomSink");
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static String connectionUrl;
    public Connection con = null;

    private String myProp;

    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");

        // Process the myProp value (e.g. validation)

        // Store myProp for later retrieval by process() method
        this.myProp = myProp;
    }

    @Override
    public void start() {
        // Initialize the connection to the external repository (e.g. HDFS) that
        // this Sink will forward Events to ..

        try {
            // Set JDBC Impala Driver
            Class.forName(JDBC_DRIVER_NAME);
            // Connect to Impala
            con = DriverManager.getConnection(connectionUrl);
            // Init Statement
//            Statement stmt = con.createStatement();
            // Invalidate metadata to update changes
//            stmt.execute(sqlStatementInvalidate);
//            logger.info("Select from Impala table with security : OK");

        } catch (Exception e) {
            logger.severe(e.getMessage());
        }

    }

    @Override
    public void stop () {
        // Disconnect from the external respository and do any
        // additional cleanup (e.g. releasing resources or nulling-out
        // field values) ..
        try {
            con.close();
        } catch (SQLException sql){
            logger.severe(sql.getMessage());
        }

    }

    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();

            // Send the Event to the external repository.
            // storeSomeData(e);
            Statement stmt = con.createStatement();
            // Invalidate metadata to update changes
//            stmt.execute(sqlStatementInvalidate);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }
}
