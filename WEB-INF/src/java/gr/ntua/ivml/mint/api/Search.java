package gr.ntua.ivml.mint.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.FacetParams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.concurrent.Solarizer;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.DataUpload;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.view.Import;

public class Search {

	public static Logger log = Logger.getLogger(Search.class);
	public static final int FIELD_LIMIT = 20;

	private Map<String, String[]> params;
	private SolrQuery solrQuery;
	private QueryResponse queryResponse;
	private ArrayNode errors;
	private boolean hasFacets;
	private boolean facetsOnly;
	private boolean datasetsOnly;
	private boolean itemsOnly;
	private JsonNodeFactory json = JsonNodeFactory.instance;
	private Map<String, Tag> tagmap = new HashMap<String, Tag>();
	// Requested number of Tags
	private int tags = -1;
	private boolean help;

	public Search(Map<String, String[]> params) {
		this.params = params;
		this.errors = json.arrayNode();
		this.solrQuery = new SolrQuery();
		this.itemsOnly = params.containsKey("item_id");
		this.datasetsOnly = isTrueParam("datasets_only");
		this.facetsOnly = isTrueParam("facets_only");
		this.hasFacets = isTrueParam("facets_only");
		this.help = false;
	}

	private boolean isTrueParam(String name) {
		return params != null && params.containsKey(name) && params.get(name)[0].equals("true");
	}

	private String getParam(String name) {
		return params.get(name)[0];
	}

	static class Tag implements Comparable<Tag> {
		String term;
		double score;

		public int compareTo(Tag b) {
			return term.compareTo(b.term);
		}

		public boolean equals(Tag b) {
			return term.equals(b.term);
		}
	}

	public static JsonNode search(User user, Map<String, String[]> params) {
		Search search = new Search(params);
		// request for item id's gives back the whole xml for those items
		if (params.containsKey("item_id"))
			search.itemsOnly = true;
		else
			search.querySolr(user);
		return search.getfinalResult();
	}

	private JsonNode getfinalResult() {
		ObjectNode result = json.objectNode();
		if (help)
			return getHelpPage();
		try {
			if (itemsOnly)
				return result.set("items", getWholeItems(params.get("item_id")));
			result.set("searchMeta", getSearchMeta());
			if (datasetsOnly)
				return result.set("datasets", getDatasets());
			if (hasFacets)
				result.set("facets", getFacets());
			if (!facetsOnly)
				result.set("items", getItems());
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			if (errors.size() != 0)
				result.set("errors", errors);
		}
		return result;
	}

	private void querySolr(User user) {
		if (params == null || params.isEmpty()) {
			help = true;
			return;
		}
		try {
			// Try to create a SolrQuery
			if (params.containsKey("query")) {
				StringBuffer query = new StringBuffer(getParam("query"));
				if (user == null || !user.hasRight(User.SUPER_USER)) {
					query.append(" AND ( published_b:true");
					if (user != null)
						user.getAccessibleOrganizations().stream()
								.forEach(o -> query.append(" OR organization_id:").append(o.getDbID()));
					query.append(" )");
				}
				if (params.containsKey("orgid"))
					query.append(" AND ( organization_id:" + params.get("orgid")[0]).append(" )");
				solrQuery.setQuery(query.toString());
			} else {
				help = true;
				return;
			}
			if (params.containsKey("filter_query"))
				solrQuery.setFilterQueries(params.get("filter_query"));
			addNumericParameter(params.get("start"), (query, start) -> {
				query.setStart(start);
			});
			addNumericParameter(params.get("max_rows"), (query, max) -> {
				query.setRows(max);
			});
			addNumericParameter(params.get("facet_offset"), (query, offset) -> {
				query.set(FacetParams.FACET_OFFSET, offset);
			});
			addNumericParameter(params.get("facet_limit"), (query, limit) -> {
				query.setFacetLimit(limit);
			});

			if ((!params.containsKey("max_rows") || solrQuery.getRows() > 0) && params.containsKey("tags")) {
				tags = Integer.parseInt(getParam("tags"));
				addNumericParameter(params.get("tags"), (query, tags) -> {
					query.setParam("tv", true);
					query.setParam("tv.fl", "tags");
					query.setParam("tv.tf_idf ", true);
					query.setParam("tv.all", true);
				});
			}
			if (params.containsKey("field"))
				solrQuery.setFields(params.get("field"));

			if (params.containsKey("field_set"))
				errors.add("Fieldset support is not there yet.");

			if (params.containsKey("sort")) {
				for (String sortfield : params.get("sort")) {
					if (sortfield.startsWith("-"))
						solrQuery.addSort(sortfield.substring(1), ORDER.desc);
					else
						solrQuery.addSort(sortfield, ORDER.asc);
				}
			}
			if (params.containsKey("facet_field")) {
				hasFacets = true;
				for (String facetField : params.get("facet_field"))
					solrQuery.addFacetField(facetField);
				if (facetsOnly)
					solrQuery.setRows(0);
				solrQuery.setFacetMinCount(1);
			}
			if (datasetsOnly) {
				solrQuery.addFacetField("dataset_id");
				solrQuery.setRows(0);
				solrQuery.setFacetMinCount(1);
			}
			log.info("Solr Query = \"" + solrQuery.getQuery() + "\"");
			queryResponse = Solarizer.getSolrClient().query(solrQuery);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			errors.add(e.getMessage());
		}
	}

	private void addNumericParameter(String[] parameter, BiConsumer<SolrQuery, Integer> handler) {
		if (parameter == null)
			return;
		try {
			int value = Integer.parseInt(parameter[0]);
			handler.accept(solrQuery, value);
		} catch (NumberFormatException e) {
			errors.add(parameter[0] + " is not a valid number.");
		}
	}

	/**
	 * JSON - ized help for server use
	 * 
	 * @return
	 * 
	 * @return
	 * 
	 */
	private ObjectNode getHelpPage() {
		ObjectNode help = json.objectNode();
		ObjectNode params = json.objectNode();
		ArrayNode fields = json.arrayNode();
		help.set("supported_parameters", params);
		help.set("fields", fields);
		params.put("query",
				"A lucene query in QueryParser format."
						+ "The all:(...) field queries are modified to hit a subset of fields with"
						+ " different weights to have relevance sorted results.");
		params.put("filter_query",
				"filter queries are \"anded\" to you query and improve the performance of caching. Multiple filter_query parameters possible.");
		params.put("max_rows", "How many fieldsets do you want to get back maximally?");
		params.put("start", "Where in the result list should the output start.");
		params.put("field", "You can specify which fields you want back. Multiple fields possible.");
		params.put("item_id", "When you have the item_ids you can specify to get the XML for those items back."
				+ "Add as many item_id=??? as you like.");
		params.put("sort",
				"Specify a field to sort by, preceed the name with '-' for descending sort. Multiple sorts possible.");
		params.put("facet_field", "Field name where facets should be reported. (value and hit count for field).");
		params.put("facet_offset", "Set an offset for facet fields to allow paging");
		params.put("facet_limit",
				"Set the maximum number of constraint counts that should be returned for the facet fields. A negative value means unlimited");
		params.put("facets_only", "Return only the facets for this query");
		params.put("datasets_only", "Return only the datasets for the items found");
		params.put("relatedItems", "Searches for related items based on given EUScreen id of item.");
		params.put("tags", "If and how many tags for a tag cloud should be generated.");
		params.put("sitemap", "With any value will return a list of production=yes video urls.");
		LukeRequest lr = new LukeRequest();
		try {
			LukeResponse lp = lr.process(Solarizer.getSolrClient());
			List<String> fieldNames = new ArrayList<String>();
			fieldNames.addAll(lp.getFieldInfo().keySet());
			Collections.sort(fieldNames);
			for (String name : fieldNames) {
				fields.add(name);
			}
		} catch (Exception e) {
			log.error("Request for field names failed!", e);
		}
		return help;
	}

	private JsonNode getSearchMeta() {
		ObjectNode searchMeta = json.objectNode();
		SolrDocumentList sdl = queryResponse.getResults();
		searchMeta.put("hits", sdl.getNumFound());
		searchMeta.put("offest", sdl.getStart());
		searchMeta.put("count", sdl.size());
		if (tags > 0)
			putTagsInSearchMeta(searchMeta);
		return searchMeta;
	}

	private JsonNode getItems() {
		SolrDocumentList sdl = queryResponse.getResults();
		ArrayNode items = json.arrayNode();
		for (SolrDocument sd : sdl) {
			ObjectNode fields = json.objectNode();
			for (String fieldName : sd.getFieldNames()) {
				int limit = FIELD_LIMIT;
				ArrayNode fieldValues = json.arrayNode();
				for (Object val : sd.getFieldValues(fieldName)) {
					if (limit == 0)
						break;
					else
						limit -= 1;
					fieldValues.add(val.toString());
				}
				fields.set(fieldName, fieldValues);
			}
			items.add(fields);
		}
		return items;
	}

	private JsonNode getDatasets() {
		ArrayNode datasets = json.arrayNode();
		if (queryResponse.getFacetFields() == null || queryResponse.getFacetField("dataset_id") == null)
			return datasets;
		FacetField ff = queryResponse.getFacetField("dataset_id");
		List<Long> datasetIds = ff.getValues().stream().map(c -> Long.parseLong(c.getName()))
				.collect(Collectors.toList());
		Map<String, Long> itemHitsMap = ff.getValues().stream()
				.collect(Collectors.toMap(Count::getName, Count::getCount));
		List<Dataset> dss = DB.getDatasetDAO().getByIds(datasetIds);
		// Set<Dataset> origins = new HashSet<Dataset>();
		// Set<Dataset> derived = new HashSet<Dataset>();
		// dss.stream().forEach(d -> (d instanceof DataUpload ? origins :
		// derived).add(d));
		// derived.stream().forEach(d -> origins.add(d.getOrigin()));
		MultiMap map = new MultiValueMap();
		for (Dataset d : dss) {
			if (d instanceof DataUpload)
				map.put(d, null);
			else
				map.put(d.getOrigin(), d);
		}
		for (Dataset ds : (Set<Dataset>) map.keySet()) {
			ObjectNode datasetInfo;
			try {
				datasetInfo = datasetToJson(ds, itemHitsMap);
				ArrayNode children = json.arrayNode();
				for (Dataset child : (List<Dataset>) map.get(ds)) {
					if (child == null)
						continue;
					children.add(datasetToJson(child, itemHitsMap));
				}
				if (children.size() != 0)
					datasetInfo.set("children", children);
			} catch (Exception e) {
				log.error("Adding info for dataset id " + ds.getDbID() + " failed!", e);
				continue;
			}
			datasets.add(datasetInfo);
		}
		return datasets;
	}

	private ObjectNode datasetToJson(Dataset dataset, Map<String, Long> itemHitsMap) {
		Long itemHits = itemHitsMap.get(String.valueOf(dataset.getDbID()));
		ObjectNode datasetInfo = json.objectNode();
		long datasetId = dataset.getDbID();
		datasetInfo.put("dataset_id", datasetId);
		datasetInfo.put("name", dataset.getName());
		datasetInfo.put("type", dataset.getClass().getSimpleName());
		datasetInfo.put("format", ((DataUpload) dataset.getOrigin()).getStructuralFormat());
		Import imp = new Import(dataset);
		imp.setStatus();
		datasetInfo.put("status", imp.getStatus());
		if (itemHits == null) {
			datasetInfo.put("item_hits", 0);
			return datasetInfo;
		}
		datasetInfo.put("created", dataset.getCreated().toString());
		datasetInfo.put("creator", dataset.getCreator().getName());
		datasetInfo.put("item_count", dataset.getItemCount());
		datasetInfo.put("is_published", dataset.isPublished());
		datasetInfo.put("item_count", dataset.getItemCount());
		datasetInfo.put("item_hits", itemHits);
		datasetInfo.put("label_number", dataset.getFolders().size());
		imp.setMessage();
		imp.setStatusIcon();
		datasetInfo.put("message", imp.getMessage());
		datasetInfo.put("status_icon", imp.getStatusIcon());
		if (dataset.getSchema() != null)
			datasetInfo.put("schema", dataset.getSchema().getName());
		return datasetInfo;
	}

	private JsonNode getFacets() {
		ObjectNode facets = json.objectNode();
		if (queryResponse.getFacetFields() != null) {
			for (FacetField ff : queryResponse.getFacetFields()) {
				ObjectNode facetValues = json.objectNode();
				for (Count c : ff.getValues()) {
					facetValues.put(c.getName(), c.getCount());
				}
				facets.set(ff.getName(), facetValues);
			}
		}
		return facets;
	}

	/**
	 * In case items are requested directly, this is what we give back.
	 * 
	 * @param itemIds
	 */
	private JsonNode getWholeItems(String[] itemIds) {
		ArrayNode items = json.arrayNode();
		for (String itemId : itemIds) {
			try {
				long dbId = Long.parseLong(itemId);
				Item item = DB.getItemDAO().getById(dbId, false);
				if (item == null)
					errors.add(itemId + " is not a valid item id.");
				else
					items.add(getItemJson(item));
			} catch (NumberFormatException nf) {
				errors.add("item_id " + itemId + " is not a valid number");
			}
		}
		return items;
	}

	private JsonNode getItemJson(Item item) {
		ObjectNode itemNode = json.objectNode();
		itemNode.put("item_id", item.getDbID());
		itemNode.put("item_xml", item.getXml());
		return itemNode;
	}

	private void putTagsInSearchMeta(ObjectNode searchMeta) {
		ArrayNode tagsNode = json.arrayNode();
		searchMeta.set("tags", tagsNode);
		double maxScore = 0.0;
		SortedSet<Tag> sortedList = new TreeSet<Tag>(new Comparator<Tag>() {
			public int compare(Tag a, Tag b) {
				if (a.score > b.score)
					return -1;
				if (a.score < b.score)
					return 1;
				if (a.term.length() > b.term.length())
					return -1;
				if (a.term.length() < b.term.length())
					return 1;
				return 0;
			}
		});
		sortedList.addAll(tagmap.values());
		if (sortedList.size() > 0) {
			maxScore = sortedList.first().score;
			for (Tag t : sortedList) {
				ObjectNode tag = json.objectNode();
				tag.put("term", t.term);
				tag.put("score", (int) (100 * (t.score / maxScore)));
				if (--tags <= 0)
					break;
			}
		}
	}
}
