package com.greendelta.search.wrapper.os;

import com.greendelta.search.wrapper.score.Case;
import com.greendelta.search.wrapper.score.Comparator;
import com.greendelta.search.wrapper.score.Score;

class Script {

	static String from(Score score) {
		var dWeight = score.getDefaultWeight();
		if (score.getCases().length == 0)
			return "return " + dWeight + ";";
		var s = getMethods(score);
		s += "def[] fieldValues = new def[" + score.fields.size() + "];";
		s += "def[] values = new def[" + score.fields.size() + "];";
		for (var i = 0; i < score.fields.size(); i++) {
			var field = score.fields.get(i);
			var escaper = field.value instanceof String ? "\"" : "";
			var numDef = field.value instanceof Long ? "L" : "";
			s += "fieldValues[" + i + "] = doc['" + field.name + "'].getValue();";
			if (field.lowerLimit != null) {
				s += "if (fieldValues[" + i + "] < " + field.lowerLimit + ") { return " + dWeight + "; }";
			}
			if (field.upperLimit != null) {
				s += "if (fieldValues[" + i + "] >  " + field.upperLimit + ") { return " + dWeight + "; }";
			}
			s += "values[" + i + "] = " + escaper + "" + field.value + "" + escaper + numDef + ";";
		}
		s += cases(score);
		return s;
	}

	private static String cases(Score score) {
		var s = "";
		var hadElse = false;
		for (var c : score.getCases()) {
			if (!c.conditions.isEmpty()) {
				s += conditions(c);
			} else {
				s += "return " + c.weight + ";";
				hadElse = true;
				break;
			}
		}
		if (!hadElse) {
			s += "return " + score.getDefaultWeight() + ";"; // default case
		}
		return s;
	}

	private static String conditions(Case scoreCase) {
		var s = "if (";
		var firstCondition = true;
		for (var con : scoreCase.conditions) {
			if (!firstCondition) {
				s += " && ";
			}
			if (con.comparator == Comparator.EQUALS) {
				s += "(" + con.value1 + ") != null && " + (con.value1) + ".equals(" + con.value2 + ")";
			} else {
				var numDef1 = con.value1 instanceof Long ? "L" : "";
				var numDef2 = con.value2 instanceof Long ? "L" : "";
				s += con.value1 + numDef1 + " " + toString(con.comparator) + " " + con.value2 + numDef2;
			}
			firstCondition = false;
		}
		s += ") { return " + scoreCase.weight + "; } ";
		return s;
	}

	private static String getDistanceMethod() {
		var s = "double toRad(double degree) { return degree * Math.PI / 180; }";
		s += "double getDistance(double lat1, double lon1, double lat2, double lon2) { ";
		s += "double earthRadius = 6371;";
		s += "double rdLat = toRad(lat2-lat1);";
		s += "double rdLon = toRad(lon2-lon1);";
		s += "double rLat1 = toRad(lat1);";
		s += "double rLat2 = toRad(lat2);";
		s += "double a = Math.sin(rdLat/2) * Math.sin(rdLat/2) + Math.sin(rdLon/2) * Math.sin(rdLon/2) * Math.cos(rLat1) * Math.cos(rLat2);";
		s += "double b = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));";
		s += "return earthRadius * b;";
		s += "}";
		return s;
	}

	private static String getMethods(Score score) {
		var s = getDistanceMethod();
		s += "String substring(String value, int from, int to) { if (value == null || from == -1 || to == -1) { return null; } return value.substring(from, to); }";
		s += "int indexOf(String value, String phrase) { if (value == null || phrase == null) { return -1; } return value.indexOf(phrase); }";
		s += "int lastIndexOf(String value, String phrase) { if (value == null || phrase == null) { return -1; } return value.lastIndexOf(phrase); }";
		s += "double abs(double value) { return Math.abs(value); }";
		s += "double min(double v1, double v2) { return Math.min(v1, v2); }";
		return s;
	}

	private static String toString(Comparator comparator) {
		return switch (comparator) {
			case IS -> "==";
			case IS_LESS_THAN -> "<";
			case IS_LESS_OR_EQUAL_THAN -> "<=";
			case IS_GREATER_THAN -> ">";
			case IS_GREATER_OR_EQUAL_THAN -> ">=";
			default -> "==";
		};
	}

}
