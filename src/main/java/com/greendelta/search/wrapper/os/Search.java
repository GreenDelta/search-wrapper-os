package com.greendelta.search.wrapper.os;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;
import com.greendelta.search.wrapper.SearchSorting;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;

class Search {

	static SearchResult<Map<String, Object>> run(OsRequest request, SearchQuery searchQuery) {
		prepare(request, searchQuery);
		try {
			SearchResult<Map<String, Object>> result = new SearchResult<>();
			OsResponse response = null;
			boolean doContinue = true;
			long totalHits = 0;
			while (doContinue) {
				if (!searchQuery.isPaged()) {
					request.setFrom(result.data.size());
				}
				response = request.execute();
				SearchHit[] hits = response.getHits();
				for (SearchHit hit : hits) {
					if (searchQuery.getFullResult()) {
						result.data.add(hit.getSourceAsMap());
					} else {
						result.data.add(Collections.singletonMap("documentId", hit.getId()));
					}
				}
				totalHits = response.getTotalHits();
				doContinue = !searchQuery.isPaged() && result.data.size() != totalHits;
				result.aggregations.addAll(Result.aggregations(response));
			}
			result.resultInfo.count = result.data.size();
			Result.extend(result, totalHits, searchQuery);
			return result;
		} catch (Exception e) {
			// TODO handle exception
			SearchResult<Map<String, Object>> result = new SearchResult<>();
			Result.extend(result, 0, searchQuery);
			return result;
		}
	}

	static Set<String> ids(OsRequest request, SearchQuery searchQuery) {
		prepare(request, searchQuery);
		try {
			Set<String> ids = new HashSet<>();
			OsResponse response = null;
			boolean doContinue = true;
			long totalHits = 0;
			while (doContinue) {
				if (!searchQuery.isPaged()) {
					request.setFrom(ids.size());
				}
				response = request.execute();
				SearchHit[] hits = response.getHits();
				for (SearchHit hit : hits) {
					ids.add(hit.getId());
				}
				totalHits = response.getTotalHits();
				doContinue = !searchQuery.isPaged() && ids.size() != totalHits;
			}
			return ids;
		} catch (Exception e) {
			// TODO handle exception
			return new HashSet<>();
		}

	}

	private static OsRequest prepare(OsRequest request, SearchQuery searchQuery) {
		setupPaging(request, searchQuery);
		setupSorting(request, searchQuery);
		setupAggregations(request, searchQuery);
		request.setQuery(Query.create(searchQuery));
		return request;
	}

	private static void setupPaging(OsRequest request, SearchQuery searchQuery) {
		int start = (searchQuery.getPage() - 1) * searchQuery.getPageSize();
		if (start > 0) {
			request.setFrom(start);
		}
		if (!searchQuery.isPaged()) {
			request.setSize(10000);
		} else {
			if (searchQuery.getPageSize() > 0) {
				request.setSize(searchQuery.getPageSize());
			} else {
				request.setSize(SearchQuery.DEFAULT_PAGE_SIZE);
			}
		}
	}

	private static void setupSorting(OsRequest request, SearchQuery searchQuery) {
		for (Entry<String, SearchSorting> entry : searchQuery.getSortBy().entrySet()) {
			SortOrder value = entry.getValue() == SearchSorting.ASC ? SortOrder.ASC : SortOrder.DESC;
			request.addSort(entry.getKey(), value);
		}
	}

	private static void setupAggregations(OsRequest request, SearchQuery searchQuery) {
		for (SearchAggregation aggregation : searchQuery.getAggregations()) {
			request.addAggregation(com.greendelta.search.wrapper.os.Aggregation.builder(aggregation));
		}
	}

	interface OsRequest {

		void setFrom(int from);

		void setSize(int size);

		void addSort(String field, SortOrder order);

		void addAggregation(AggregationBuilder aggregation);

		void setQuery(QueryBuilder query);

		OsResponse execute() throws IOException;

	}

	interface OsResponse {

		SearchHit[] getHits();

		long getTotalHits();

		List<Aggregation> getAggregations();

		List<? extends Bucket> getTermBuckets(Aggregation aggregation);

		List<? extends org.opensearch.search.aggregations.bucket.range.Range.Bucket> getRangeBuckets(
				Aggregation aggregation);

	}

}
