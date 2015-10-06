package com.yookos.data.migration;

import com.yookos.data.migration.Entity.Ticket;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;

public class Main {
	Mapping mapping;

	public static void main(String[] args) {
        // variables
        final int batch = 5000;
        final int max = 100000;
        final int threads = 20;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        int count = 1;

        // create queue
        BlockingQueue<Ticket> queue = new LinkedBlockingDeque<Ticket>();

        //populate queue
        for(int i = 1; i < max; i+=batch) {
            queue.add(
                    new Ticket(i)
            );
        }

        while(!queue.isEmpty()){
            es.submit(new Processor(queue, count, batch));
            count++;
        }
	}
}
