# Feature guide for interconnecting the EuropeanaTranslate API with MINT


The instructions  below explain how to make use of the EuropeanaTranslate machine translation API to enrich your MINT collections. Note that the translation functionality is not yet available via the MINT UI, but is currently supported via a set of scripts that take care of invoking the machine translation API to retrieve automatic translations for a selected set of metadata fields of a certain MINT dataset; of forming the retrieved translations as an AnnotationSet object; and then applying those annotations as enrichments to a MINT dataset after the specification of certain filtering conditions.



Prerequisites:
 * The procedure makes use of the JQ JSON processing tool, so you should make sure that it is available.
 * The dataset that you want to translate is in EDM shape and downloaded from the MINT Download button.
 * Groovy is used to execute some scripts.
 * MINT needs to be added to the classpath, which contains helper classes and configurations for the Pangeanic API endpoint.

## Step by Step

* Download the Dataset to translate from the MINT UI
* Invoke the tgzToAnnotationSet groovy script. It creates a W3C compatible JSON-LD annotation object for this dataset that contains the automatic translations. The annotation object structure can be found here: https://docs.google.com/document/d/1Cq1Qqx0ji7Vw8iwLVis1CfpYKtv-72ojkcvjnQzrKjs/
* Use JQ to create a CSVfile with the translation you wish to apply. A basic invocation would be: 
```
jq -r <annotation.json '["scope", "id", "confidence", "english" ] ,
 (.["@graph"][] |
   [ .scope, .target.source, .confidence, .body.value] ) | 
@csv ' >dataset_1234_translations.csv
```
* To modify the process, change field names in tgzToAnnotationSet.groovy script, or add filters to jq for confidence, scope or whatever other condition seems appropriate for your dataset.
* upload the csv file to the correct organization into mint via the upload script:

`groovy uploadCsv.groovy dataset_1234_translations.csv 1022`

* Invoke the script for Enrichment from the MINT script console. This needs to be parameterized with the correct dataset ID, the enrichment ID of the uploaded CSV file, and which column in it contains which values. If there are record or value based filterings to apply, this script is a good place.


