package gr.ntua.ivml.mint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import net.sf.saxon.functions.Put;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class OpDirectObject {

	class FieldGroup extends HashMap<String, Object> {

		@Override
		public Object put(String field, Object value) {
			if (value instanceof List) {
				Object previousValue =  get("field");
				if (!containsKey(field))
					super.put(field, new ArrayList<String>());
				((ArrayList<String>) get(field)).addAll((List) value);
				return previousValue;
			}
			return super.put(field, value);
		}

		public Object put(String field, Object value, boolean isArray) {
			if (isArray && (value instanceof String))
				// ArrayList<String> arrayValue = new ArrayList<String>();
				// arrayValue.add((String) value);
				return put(field, Arrays.asList(value));
			return put(field, value);
		}

	}

	List<FieldGroup> languageAwareFields = new ArrayList<FieldGroup>();
	FieldGroup languageNonAwareFields = new FieldGroup();

	// res.addLanguagedField( "title","en","This is the title"
	// and I want to serialize into Json with jackson easily

	Integer id;

	public void addField(String field, String lang, Object value, boolean isArray) {
		if (lang == null) {
			languageNonAwareFields.put(field, value, isArray);
			return;
		}
		for (FieldGroup lf : languageAwareFields) {
			if (lf.containsKey("language") && lf.get("language").equals(lang)) {
				lf.put(field, value, isArray);
				return;
			}
		}
		FieldGroup lf = new FieldGroup();
		lf.put("language", lang);
		lf.put(field, value, isArray);
		languageAwareFields.add(lf);
	}

	public void addCustomLanguagedField(String field, String lang, String key, String value) {
		for (FieldGroup lf : languageAwareFields) {

		}
	}

}
