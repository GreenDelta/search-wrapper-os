package com.greendelta.search.wrapper.os;

import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

import com.greendelta.search.wrapper.aggregations.RangeAggregation;
import com.greendelta.search.wrapper.aggregations.SearchAggregation;
import com.greendelta.search.wrapper.aggregations.TermsAggregation;

class Aggregation {

	static AggregationBuilder builder(SearchAggregation aggregation) {
		var builder = createBuilder(aggregation);
		if (isNested(aggregation.field)) {
			builder = nest(builder, aggregation);
		}
		return builder;
	}

	private static AggregationBuilder createBuilder(SearchAggregation aggregation) {
		return switch (aggregation.type) {
			case TERM -> termsBuilder((TermsAggregation) aggregation);
			case RANGE -> rangeBuilder((RangeAggregation) aggregation);
			default -> null;
		};
	}

	private static AggregationBuilder termsBuilder(TermsAggregation aggregation) {
		return AggregationBuilders.terms(aggregation.name).field(aggregation.field).size(Integer.MAX_VALUE);
	}

	private static AggregationBuilder rangeBuilder(RangeAggregation aggregation) {
		var builder = AggregationBuilders.range(aggregation.name).field(aggregation.field);
		for (var range : aggregation.ranges) {
			if (range[0] == null) {
				builder.addUnboundedTo(range[1]);
			} else if (range[1] == null) {
				builder.addUnboundedFrom(range[0]);
			} else {
				builder.addRange(range[0], range[1]);
			}
		}
		return builder;
	}

	private static AggregationBuilder nest(AggregationBuilder builder, SearchAggregation aggregation) {
		var path = aggregation.field;
		var name = aggregation.name;
		builder.subAggregation(AggregationBuilders.reverseNested(name + "-r"));
		while (path.contains(".")) {
			name += "-n";
			path = path.substring(0, path.lastIndexOf("."));
			builder = AggregationBuilders.nested(name, path).subAggregation(builder);
		}
		return builder;
	}

	private static boolean isNested(String field) {
		return field.contains(".");
	}

}
