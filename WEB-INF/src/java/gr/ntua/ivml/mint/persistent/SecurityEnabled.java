package gr.ntua.ivml.mint.persistent;

import java.util.Collections;
import java.util.List;

/**
 * Anything that has an Organization or Project ids can be checked for access rights
 * @author arne
 *
 */
public interface SecurityEnabled {
	public Organization getOrganization();
	public default List<Integer> getProjectIds() {
		return Collections.emptyList();
	};
}
