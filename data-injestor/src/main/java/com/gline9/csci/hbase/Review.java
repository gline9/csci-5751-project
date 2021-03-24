package com.gline9.csci.hbase;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Review
{
    private String asin;
    private Double overall;
    private String reviewText;
    private String summary;
    private String reviewerID;

    public Put toReviewPut()
    {
        Put put = new Put(Bytes.toBytes(asin + "-" + reviewerID));
        byte[] reviewFamily = Bytes.toBytes("r");
        if (null != overall)
        {
            put.addColumn(reviewFamily, Bytes.toBytes("rating"), Bytes.toBytes(overall.byteValue()));
        }
        if (null != reviewText)
        {
            put.addColumn(reviewFamily, Bytes.toBytes("review"), Bytes.toBytes(reviewText));
        }
        if (null != summary)
        {
            put.addColumn(reviewFamily, Bytes.toBytes("summary"), Bytes.toBytes(summary));
        }

        return put;
    }

    public Put toMetadataPut()
    {
        Put put = new Put(Bytes.toBytes(asin));
        put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(reviewerID), Bytes.toBytes(overall.byteValue()));
        return put;
    }

    public Put toOverallReviewPut()
    {
        Put put = new Put(Bytes.toBytes(reviewerID));
        put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(asin), Bytes.toBytes(overall.byteValue()));
        return put;
    }

    public Get toProductGet()
    {
        Get get = new Get(Bytes.toBytes(asin));
        get.addFamily(Bytes.toBytes("c"));
        get.addColumn(Bytes.toBytes("m"), Bytes.toBytes("brand"));

        return get;
    }

    public Optional<List<Put>> toBrandReviewPuts(Map<String, Optional<BrandResult>> resultMap)
    {
        if (!resultMap.containsKey(asin))
        {
            return Optional.empty();
        }

        BrandResult result = resultMap.get(asin).get();

        if (null == result.getBrandName() || "".equals(result.getBrandName().trim()) || result.getCategories().isEmpty())
        {
            return Optional.empty();
        }

        String brand = result.getBrandName();

        List<Put> puts = new ArrayList<>();

        for (String category : result.getCategories())
        {
            Put put = new Put(Bytes.toBytes(category + "-" + brand));
            put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(asin + "-" + reviewerID), Bytes.toBytes(overall.byteValue()));
            puts.add(put);
        }

        return Optional.of(puts);
    }

    public String toString()
    {
        return String.format("ReviewerID: '%s', ASIN: '%s', Rating: '%.1f', Summary: '%s', ReviewText: '%s'", reviewerID, asin, overall, summary, reviewText);
    }
}
