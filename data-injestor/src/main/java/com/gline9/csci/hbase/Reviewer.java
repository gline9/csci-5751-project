package com.gline9.csci.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Reviewer
{
    private final String reviewerID;
    private final Map<String, Integer> reviews = new HashMap<>();
    private final double averageReview;

    public Reviewer(Result result)
    {
        reviewerID = Bytes.toString(result.getRow());
        for (Map.Entry<byte[], byte[]> review : result.getFamilyMap(Bytes.toBytes("o")).entrySet())
        {
            reviews.put(Bytes.toString(review.getKey()), (int)Bytes.toShort(review.getValue()));
        }
        averageReview = getAverageReview();
    }

    public static Comparator<Reviewer> getMaxingComparator()
    {
        return Comparator.comparingDouble((Reviewer a) -> a.averageReview)
                .thenComparingInt(a -> a.reviews.size())
                .thenComparing(a -> a.reviewerID);
    }

    public static Comparator<Reviewer> getMiningComparator()
    {
        return Comparator.comparingDouble((Reviewer a) -> a.averageReview)
                .thenComparingInt(a -> -a.reviews.size())
                .thenComparing(a -> a.reviewerID);
    }

    private double getAverageReview()
    {
        return reviews.values().stream().mapToInt(Integer::valueOf).average().orElse(-1d);
    }

    public String toString()
    {
        return String.format("reviewerID: %s, Average Rating: %.2f, Number of reviews: %d", reviewerID, averageReview, reviews.size());
    }
}
