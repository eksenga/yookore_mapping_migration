package com.yookos.data.migration;

import com.yookos.data.migration.Entity.Ticket;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * Creator      :   emile
 * Date         :   15/10/06
 * Description  :
 */
public class Processor implements Runnable {
    private  static final String YOOKOS = "jdbc:postgresql://192.168.120.75:5432/yookos";
    private  static final String UAA = "jdbc:postgresql://10.10.10.227:5432/uaa";

    // check blocking queue, get ticket, get offset, query db for next batch
    private long offset;
    private long batchSize;
    private final BlockingQueue queue;
    private final int id;

    Connection yookos_conn = getConnection(YOOKOS, "postgres", "");;
    Connection uaa_conn = getConnection(UAA, "postgres", "postgres");
    Statement stmt;
    Statement yookos_stmt;

    public Processor(BlockingQueue<Ticket> queue, int id, long batchSize) {
        this.queue = queue;
        this.id = id;
        this.batchSize = batchSize;
        try {
            this.offset = ((Ticket)queue.take()).offset;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Initialized Processor " + id + " offset " + offset + " (from item " + offset + " to item " + (offset + batchSize) + ")");
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();

        try {
            stmt = uaa_conn.createStatement();
            yookos_stmt = yookos_conn.createStatement();

            HashMap<String, String> map = this.extract();
            this.transform(map);

            //close ish
            yookos_conn.close();
            uaa_conn.close();

            stmt.close();
            yookos_stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Running Time " + id + " : " + ((System.currentTimeMillis() - startTime) / 1000));
    }

    private static boolean isPostgresDriverLoaded() {

        try {
            //System.out.println("-------- PostgreSQL JDBC Connection Testing ------------");
            Class.forName("org.postgresql.Driver");
            //System.out.println("PostgreSQL JDBC Driver Registered!");
            //System.out.println();
            return true;
        }
        catch (ClassNotFoundException e) {
            System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
            e.printStackTrace();
            return false;
        }
    }

    private static Connection getConnection(String url, String username, String password) {

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);
            return connection;
        }
        catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return null;
        }
    }

    private void transform(HashMap<String, String> map) {

        if (isPostgresDriverLoaded()) {
            try {
                Iterator<String> iter = map.keySet().iterator();
                ResultSet rs = null;
                String uuid = null;
                String username = null;
                long jiveuserid;
                int count = 0;

                while (iter.hasNext()) {
                    username = iter.next();
                    uuid = map.get(username);
                    rs = yookos_stmt.executeQuery(String.format("SELECT * FROM jiveuser WHERE username = '%s' LIMIT 10;", username));

                    while (rs.next()) {
                        count++;
                        jiveuserid = rs.getLong("userid");
                        System.out.println(count + ". " + username + " " + jiveuserid + " " + uuid);
                        load(jiveuserid, uuid, username);
                    }
                }

                rs.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void load(long jiveuserid, String uuid, String username) {

        if (isPostgresDriverLoaded()) {
            try {
                stmt.executeUpdate(String.format("INSERT INTO user_mappings (jiveuserid, uuid, username) VALUES (%s, '%s', '%s');", jiveuserid, uuid, username));
            } catch (SQLException e) {
                System.out.println();
                System.out.println();
                System.out.println("======================================================================");
                System.out.println();
                System.out.println(String.format("::: ERROR : %s, '%s', '%s'",jiveuserid, uuid, username));
                System.out.println();
                System.out.println("======================================================================");
                System.out.println();
                System.out.println();


                //e.printStackTrace();
            }
        }
    }

    private HashMap<String, String> extract() {

        HashMap<String, String> map = new HashMap<String, String>();

        if (isPostgresDriverLoaded()) {
            try {
                Connection connection = getConnection(UAA, "postgres", "postgres");
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM users OFFSET %s LIMIT %s;", offset, batchSize)); // add offset as well
                int count = 0;
                while (rs.next()) {
                    //extract
                    ++count;
                    String userId = rs.getString("id");
                    String username = rs.getString("username");
                    map.put(username, userId);
                }
                rs.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return map;
    }
}
