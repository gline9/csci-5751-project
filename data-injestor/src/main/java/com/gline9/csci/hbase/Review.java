package com.gline9.csci.hbase;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

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

    public String toString()
    {
        return String.format("ReviewerID: '%s', ASIN: '%s', Rating: '%.1f', Summary: '%s', ReviewText: '%s'", reviewerID, asin, overall, summary, reviewText);
    }
}
