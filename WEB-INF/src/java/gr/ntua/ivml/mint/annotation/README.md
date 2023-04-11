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

`groovy MintUploadEnrichment.groovy -m http://mint.server.url/mint/instance -u user:password -o 1022 dataset_1234_translations.csv`

* Invoke the script for Enrichment from the MINT script console. This needs to be parameterized with the correct dataset ID, the enrichment ID of the uploaded CSV file, and which column in it contains which values. If there are record or value based filterings to apply, this script is a good place.

```
import gr.ntua.ivml.mint.util.SetCounter;
import gr.ntua.ivml.mint.util.StringUtils;
import org.apache.log4j.Logger;
import java.io.*

@CompileStatic
def makeModifier() {
   // enter the id of the uploaded enrichment
   Enrichment.EnrichBuilder translateEnrich = DB.enrichmentDAO.getById( 9999l, false ).getEnrichBuilder( 1 ) 
  
   def mod = { Document doc -> 

        Nodes nodes = doc.query( "//*[local-name()='ProvidedCHO']/@*[local-name()='about']");
        if( nodes.size() != 1 ) return;

        // make sure there were no duplicates before
        String key = nodes.get(0).getValue();

        for( String[] row: translateEnrich.getWithKey( key )) {
            String english = row[3]
            Optional<String> confidence = Optional.ofNullable( row[2] ).filter( { String s ->  !StringUtils.empty( s )} as java.util.function.Predicate<String> )
            EdmEnrichBuilder.enrichLiteral( doc, row[0], "en", english, "SoftwareAgent",  confidence  ) 
        }


   } as ThrowingConsumer<Document>
   return mod
} 


def interceptor = Interceptor.modifyInterceptor( makeModifier() )

// works with EDM schemas that support wasGeneratedBy and confidence attributes
def schema = DB.xmlSchemaDAO.simpleGet( "name = 'EDM annotated'")

// Name this better if you have changed the scripts
new EdmEnrichBuilder()
  .setName( "Standard EDM translations to English" )
  .setTargetSchema( schema  )
  .setDocumentInterceptor( interceptor )
  // put the correct dataset id here ... the one you downloaded as tgz
  .submit( 1234l )

```
