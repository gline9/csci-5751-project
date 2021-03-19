package com.gline9.csci.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class Main
{
    public static void main(String[] args) throws IOException
    {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration))
        {
            printAverages(connection);
        }
    }

    public static void printAverages(Connection connection) {
        System.out.println("Test");
    }

}
