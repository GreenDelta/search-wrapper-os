package com.greendelta.search.wrapper.os;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import com.greendelta.search.wrapper.SearchClient;
import com.greendelta.search.wrapper.SearchQuery;
import com.greendelta.search.wrapper.SearchResult;

public class OsClient implements SearchClient {

	private final Client client;
	private final String indexName;

	public OsClient(Client client, String indexName) {
		this.client = client;
		this.indexName = indexName;
	}

	@Override
	public SearchResult<Map<String, Object>> search(SearchQuery searchQuery) {
		try {
			var request = new Request(client, indexName);
			return Search.run(request, searchQuery);
		} catch (Exception e) {
			e.printStackTrace();
			return new SearchResult<>();
		}
	}

	@Override
	public Set<String> searchIds(SearchQuery searchQuery) {
		var request = new Request(client, indexName);
		return Search.ids(request, searchQuery);
	}

	@Override
	public void create(Map<String, String> settings) {
		var exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (exists)
			return;
		var indexSettings = settings.get("config");
		var request = new CreateIndexRequest(indexName);
		request.settings(Settings.builder()
				.loadFromSource(indexSettings, XContentType.JSON).put("number_of_shards", 1));
		client.admin().indices()
				.create(request).actionGet();
		var mapping = settings.get("mapping");
		var mappingRequest = Requests.putMappingRequest(indexName);
		mappingRequest.source(mapping, XContentType.JSON);
		client.admin().indices().putMapping(mappingRequest).actionGet();
	}

	@Override
	public void index(String id, Map<String, Object> content) {
		client.index(indexRequest(id, content, true)).actionGet();
	}

	@Override
	public void index(Map<String, Map<String, Object>> contentsById) {
		var builder = client.prepareBulk();
		for (var id : contentsById.keySet()) {
			var content = contentsById.get(id);
			builder.add(indexRequest(id, content, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private IndexRequest indexRequest(String id, Map<String, Object> content, boolean refresh) {
		var builder = client.prepareIndex(indexName).setId(id);
		builder.setOpType(OpType.INDEX).setSource(content);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	@Override
	public void update(String id, Map<String, Object> update) {
		client.update(updateRequest(id, update, true)).actionGet();
	}

	@Override
	public void update(String id, String script, Map<String, Object> parameters) {
		client.update(updateRequest(id, script, parameters, true)).actionGet();
	}

	@Override
	public void update(Set<String> ids, Map<String, Object> update) {
		var builder = client.prepareBulk();
		for (var id : ids) {
			builder.add(updateRequest(id, update, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	@Override
	public void update(Set<String> ids, String script, Map<String, Object> parameters) {
		var builder = client.prepareBulk();
		for (var id : ids) {
			builder.add(updateRequest(id, script, parameters, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	@Override
	public void update(Map<String, Map<String, Object>> updatesById) {
		BulkRequestBuilder builder = client.prepareBulk();
		for (var id : updatesById.keySet()) {
			var update = updatesById.get(id);
			builder.add(updateRequest(id, update, false));
		}
		client.bulk(builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private UpdateRequest updateRequest(String id, Map<String, Object> content, boolean refresh) {
		var builder = client.prepareUpdate(indexName, id);
		builder.setDoc(content);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	private UpdateRequest updateRequest(String id, String script, Map<String, Object> parameters, boolean refresh) {
		var builder = client.prepareUpdate(indexName, id);
		builder.setScript(new Script(ScriptType.INLINE, "painless", script, parameters));
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	@Override
	public void remove(String id) {
		client.delete(deleteRequest(id, true)).actionGet();
	}

	@Override
	public void remove(Set<String> ids) {
		var bulk = client.prepareBulk();
		for (var id : ids) {
			bulk.add(deleteRequest(id, false));
		}
		client.bulk(bulk.setRefreshPolicy(RefreshPolicy.IMMEDIATE).request()).actionGet();
	}

	private DeleteRequest deleteRequest(String id, boolean refresh) {
		var builder = client.prepareDelete(indexName, id);
		if (refresh) {
			builder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
		}
		return builder.request();
	}

	@Override
	public boolean has(String id) {
		var builder = client.prepareGet(indexName, id);
		var response = client.get(builder.request()).actionGet();
		if (response == null)
			return false;
		return response.isExists();
	}

	@Override
	public Map<String, Object> get(String id) {
		var builder = client.prepareGet(indexName, id);
		var response = client.get(builder.request()).actionGet();
		if (response == null)
			return null;
		var source = response.getSource();
		if (source == null || source.isEmpty())
			return null;
		return source;
	}

	@Override
	public List<Map<String, Object>> get(Set<String> ids) {
		var builder = client.prepareMultiGet();
		builder.add(indexName, ids);
		var response = client.multiGet(builder.request()).actionGet();
		if (response == null)
			return null;
		var results = new ArrayList<Map<String, Object>>();
		var it = response.iterator();
		while (it.hasNext()) {
			var resp = it.next().getResponse();
			if (resp == null)
				continue;
			var source = resp.getSource();
			if (source == null || source.isEmpty())
				continue;
			results.add(source);
		}
		return results;
	}

	@Override
	public void clear() {
		var exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists)
			return;
		var mapping = client.admin().indices()
				.prepareGetMappings(indexName).execute().actionGet()
				.getMappings().get(indexName).getSourceAsMap();
		client.admin().indices()
				.delete(new DeleteIndexRequest(indexName)).actionGet();
		var request = new CreateIndexRequest(indexName);
		request.settings(Settings.builder().put("max_result_window", 2147483647).put("number_of_shards", 1));
		client.admin().indices()
				.create(request).actionGet();
		var mappingRequest = Requests.putMappingRequest(indexName);
		mappingRequest.source(mapping);
		client.admin().indices()
				.putMapping(mappingRequest).actionGet();
	}

	@Override
	public void delete() {
		var exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists)
			return;
		client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
	}

}
