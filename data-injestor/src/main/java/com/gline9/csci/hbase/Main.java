package com.gline9.csci.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Main
{
    public static void main(String[] args) throws IOException
    {

        Configuration configuration = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(configuration))
        {
            createTables(connection);
            putMetadata(connection, args[0]);
        }
    }

    private static void putMetadata(Connection connection, String metadataFileName) throws IOException
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File file = new File(metadataFileName);
        BufferedReader reader = Files.newBufferedReader(file.toPath());

        Table metadataTable = connection.getTable(TableName.valueOf("metadata"));
        Table productLinksTable = connection.getTable(TableName.valueOf("productLinks"));

        int lines = 0;
        List<Put> metadataPuts = new ArrayList<>();
        List<Put> productLinkPuts = new ArrayList<>();
        while (reader.ready())
        {
            lines++;
            String line = reader.readLine();
            MetadataEntry metadataEntry = objectMapper.readValue(line, MetadataEntry.class);
            metadataPuts.add(metadataEntry.toMetadataPut());
            productLinkPuts.add(metadataEntry.toProductLinkPut());

            if (lines % 1000 == 0)
            {
                printMetadataProgress(lines);
            }

            if (metadataPuts.size() >= 10000)
            {
                metadataTable.put(metadataPuts);
                metadataPuts.clear();
                System.out.println("Processed metadata batch: Line " + lines);
            }

            if (productLinkPuts.size() >= 10000)
            {
                productLinksTable.put(productLinkPuts);
                productLinkPuts.clear();
                System.out.println("Processed product links batch: Line " + lines);
            }
        }

        metadataTable.put(metadataPuts);
        metadataPuts.clear();
        System.out.println("Processed metadata batch: Line " + lines);

        productLinksTable.put(productLinkPuts);
        productLinkPuts.clear();
        System.out.println("Processed product links batch: Line " + lines);

        System.out.println("Done!");
    }

    private static void printMetadataProgress(int iterations)
    {
        final int totalIterations = 15023059;
        double percentDone = iterations / (double)totalIterations;

        int barLength = 40;
        int barProgress = (int)(percentDone * barLength);
        int barRemaining = barLength - barProgress;
        String barCharacters = new String(new char[barProgress]).replace("\0", "#");
        String barFillCharacters = new String(new char[barRemaining]).replace("\0", ".");

        System.out.printf("[%s%s] %.2f%% (%d/%d)%n", barCharacters, barFillCharacters, percentDone * 100, iterations, totalIterations);
    }

    private static void createTables(Connection connection) throws IOException
    {
        Admin admin = connection.getAdmin();

        TableDescriptor reviewsTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("reviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("r")).build();

        admin.createTable(reviewsTable);

        TableDescriptor metadataTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("metadata"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("m"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("c"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(metadataTable);

        TableDescriptor productLinksTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("productLinks"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("m"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("v"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("b")).build();

        admin.createTable(productLinksTable);

        TableDescriptor overallReviewsTable = TableDescriptorBuilder.newBuilder(TableName.valueOf("overallReviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(overallReviewsTable);

        TableDescriptor brandReviews = TableDescriptorBuilder.newBuilder(TableName.valueOf("brandReviews"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("o")).build();

        admin.createTable(brandReviews);
    }
}
