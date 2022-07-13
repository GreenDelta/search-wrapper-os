package com.greendelta.search.wrapper.os;

import java.io.IOException;

import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.sort.SortOrder;

import com.greendelta.search.wrapper.os.Search.OsRequest;

class Request implements OsRequest {

	private final SearchRequestBuilder request;

	Request(Client client, String indexName) {
		request = client.prepareSearch(indexName);
	}

	@Override
	public void setFrom(int from) {
		request.setFrom(from);
	}

	@Override
	public void setSize(int size) {
		request.setSize(size);
	}

	@Override
	public void addSort(String field, SortOrder order) {
		request.addSort(field, order);
	}

	@Override
	public void addAggregation(AggregationBuilder aggregation) {
		request.addAggregation(aggregation);
	}

	@Override
	public void setQuery(QueryBuilder query) {
		request.setQuery(query);
	}
	
	@Override
	public void addField(String field) {
		request.addFetchField(field);
	}

	@Override
	public Response execute() throws IOException {
		return new Response(request.execute().actionGet());
	}

}
