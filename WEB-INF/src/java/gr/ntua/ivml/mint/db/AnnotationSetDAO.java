package gr.ntua.ivml.mint.db;

import java.util.List;

import gr.ntua.ivml.mint.persistent.AnnotationSet;
import gr.ntua.ivml.mint.persistent.Organization;

public class AnnotationSetDAO extends DAO<AnnotationSet, Long> {

    public List<AnnotationSet> findByOrganization(Organization org ) {
            return getSession().createQuery("from AnnotationSet where organization=:org order by organization")
                    .setEntity("org", org)
                    .list();
    }

}
