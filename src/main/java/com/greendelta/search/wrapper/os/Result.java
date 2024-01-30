package com.greendelta.search.wrapper.os;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.nested.ParsedNested;
import org.opensearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

import com.greendelta.search.wrapper.SearchFilterType;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;
import com.greendelta.search.wrapper.aggregations.results.AggregationResult;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultBuilder;
import com.greendelta.search.wrapper.aggregations.results.AggregationResultEntry;
import com.greendelta.search.wrapper.os.Search.OsResponse;

class Result {

	private static final String RANGE_TYPE = "range";
	private static final String NESTED_TYPE = "nested";

	static List<AggregationResult> aggregations(OsResponse response) {
		return addAggregations(response, response.getAggregations());
	}

	static List<AggregationResult> addAggregations(OsResponse response, List<Aggregation> aggreagtions) {
		var results = new ArrayList<AggregationResult>();
		for (var aggregation : aggreagtions) {
			if (!aggregation.getType().equals(NESTED_TYPE)) {
				results.add(addAggregation(response, aggregation));
			} else {
				results.addAll(addAggregations(response, ((ParsedNested) aggregation).getAggregations().asList()));
			}
		}
		return results;
	}

	private static AggregationResult addAggregation(OsResponse response, Aggregation aggregation) {
		var builder = new AggregationResultBuilder();
		builder.name(aggregation.getName()).type(mapType(aggregation.getType()));
		putEntries(response, aggregation, builder);
		return builder.build();
	}

	private static SearchFilterType mapType(String type) {
		if (type == null)
			return null;
		return switch (type) {
			case StringTerms.NAME, LongTerms.NAME, DoubleTerms.NAME -> TermsAggregation.TYPE;
			case RANGE_TYPE -> RangeAggregation.TYPE;
			default -> SearchFilterType.UNKNOWN;
		};
	}

	private static void putEntries(OsResponse response, Aggregation aggregation, AggregationResultBuilder builder) {
		var totalCount = 0;
		switch (aggregation.getType()) {
			case StringTerms.NAME, LongTerms.NAME, DoubleTerms.NAME:
				for (var bucket : response.getTermBuckets(aggregation)) {
					var count = getCount(bucket.getDocCount(), bucket.getAggregations().asList());
					builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), count));
					totalCount += count;
				}
				builder.totalCount(totalCount);
				break;
			case RANGE_TYPE:
				for (var bucket : response.getRangeBuckets(aggregation)) {
					var data = new Object[] { bucket.getFrom(), bucket.getTo() };
					var count = getCount(bucket.getDocCount(), bucket.getAggregations().asList());
					builder.addEntry(new AggregationResultEntry(bucket.getKeyAsString(), count, data));
					totalCount += count;
				}
				builder.totalCount(totalCount);
				break;
		}
	}

	private static long getCount(long bucketCount, List<Aggregation> aggregations) {
		for (var aggregation : aggregations)
			if (aggregation instanceof ParsedReverseNested nested)
				return nested.getDocCount();
		return bucketCount;
	}

	static void extend(SearchResult<Map<String, Object>> result, long totalHits, SearchQuery searchQuery) {
		result.resultInfo.totalCount = totalHits;
		result.resultInfo.currentPage = searchQuery.getPage();
		result.resultInfo.pageSize = searchQuery.getPageSize();
		var totalCount = result.resultInfo.totalCount;
		if (searchQuery.getPageSize() != 0) {
			var pageCount = (int) totalCount / searchQuery.getPageSize();
			if ((totalCount % searchQuery.getPageSize()) != 0) {
				pageCount = 1 + pageCount;
			}
			result.resultInfo.pageCount = pageCount;
		}
	}

}
