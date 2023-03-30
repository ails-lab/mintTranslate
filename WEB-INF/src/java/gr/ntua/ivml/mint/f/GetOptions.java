package gr.ntua.ivml.mint.f;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.User;

// More project, more dataset options
public interface GetOptions {
	public ArrayNode options( Dataset ds, User u );
}