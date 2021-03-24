package com.gline9.csci.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Optional;

public class PageRankRecord
{
    private final String asin;
    private final Long rank;

    public PageRankRecord(Result result)
    {
        asin = Bytes.toString(result.getRow());
        rank = Optional.ofNullable(result.getValue(Bytes.toBytes("r"), Bytes.toBytes("rank"))).map(Bytes::toLong).orElse(0L);
    }

    public PageRankRecord(String asin, Long rank)
    {
        this.asin = asin;
        this.rank = rank;
    }

    public PageRankRecord merge(PageRankRecord o)
    {
        return new PageRankRecord(asin, rank == null || rank == 0 ? o.rank : rank);
    }

    public String getAsin()
    {
        return asin;
    }

    public Long getRank()
    {
        return rank;
    }

}
