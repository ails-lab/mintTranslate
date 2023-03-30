import gr.ntua.ivml.mint.persistent.*

// Print summary of all datasets in the organization

// use listOrgs() or listOrgs( "substring" ) to get org overview  
// use showOrgContent( 1000 ) to get contents

// listOrgs( "muse")
// showOrgContent( 1000 )

def listOrgs() {
    DB.organizationDAO.findAll().each{ println( "$it.dbID: $it.englishName")}    
}

def listOrgs( String substring ) {
    def pattern = ~( "(?i)" + substring )
    DB.organizationDAO.findAll().findAll{ it.englishName =~ pattern }.each{ println( "$it.dbID: $it.englishName")}
}

def showOrgContent( long orgId ) {
    def org = DB.organizationDAO.getById( orgId, false )
    def allDs = DB.datasetDAO.simpleList( "organization=$org.dbID")
    def uploads = allDs.findAll{ it.parentDataset ==null}
    def prs = DB.publicationRecordDAO.findAll()

    println( " ORG: $org.dbID - '$org.englishName'")
    println( " ENRICHMENTS \n" )
    def enrichments = DB.enrichmentDAO.simpleList( "organization=$orgId")
    enrichments.each{
        println( "$it.dbID '$it.name'")
    }
    println( " \n DATASETS \n")
    for( ds in uploads ) {
        showAll( ds, 0, prs, allDs )
    }

}

def published( ds, prs  ) {
    // return target 
    def pr = prs.find{ it.publishedDataset.dbID == ds.dbID }
    if( pr != null ) return "Published to $pr.target "
    else return ""
}

def showAll( ds, indent, prs, allDs ) {
    if( indent > 0 ) (1..indent).each{ print( " " )}
    println( show(ds, prs  ))
    allDs.findAll{ (it.parentDataset != null) && (it.parentDataset.dbID == ds.dbID) }.each{
        child ->
        showAll( child, indent+2, prs, allDs )
    }
}

def show( ds, prs ) {
    String res = ""
    String projectsLabels = ""
    if( ds instanceof Transformation ) {
        if( ds.mapping != null ) {
          res += "M$ds.dbID '$ds.mapping.name' "            
        } else if( ds.enrichment != null ) {
            res += "E$ds.dbID '$ds.enrichment.name' "
        } else if( ds.crosswalk != null ) {
            res += "W$ds.dbID Walked "
        } else {
            res += "C$ds.dbID '$ds.name' "
        }
    } else if ( ds instanceof AnnotatedDataset )
        res += "A$ds.dbID Annotation "
    else {
        res += "U$ds.dbID $ds.name "
        ds.getProjectNames().each{ projectsLabels += it + " " }
        ds.getFolders().each{ projectsLabels += it.split()[0] + " " }
    }
    if( ds.schema != null ) res += "Schema:$ds.schema.name: "
    if( projectsLabels.length() > 0 ) 
        res += "Tags:' " + projectsLabels + "' "
    res += published( ds,prs  )
    res += "Items:" + ds.itemCount
    return res
}