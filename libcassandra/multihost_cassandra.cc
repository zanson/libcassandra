/*
 * LibCassandra
 * Copyright (C) 2011 Mateusz Korniak
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>


#include "libgenthrift/Cassandra.h"
#include "libcassandra/multihost_cassandra.h"



using namespace libcassandra;
using namespace std;


boost::shared_ptr<Cassandra> 
libcassandra::connect_cassandra_client(const std::string & host, int port, const std::string& keyspace, int socket_timeout)
{
	boost::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(host, port));
	socket->setConnTimeout(socket_timeout);
	socket->setRecvTimeout(socket_timeout);
	socket->setSendTimeout(socket_timeout);
	boost::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TFramedTransport(socket));
	boost::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
	transport->open(); // throws an exception 	
	org::apache::cassandra::CassandraClient * thrift_client = new org::apache::cassandra::CassandraClient(protocol);
	boost::shared_ptr<Cassandra> cassandra(new Cassandra(thrift_client, host, port, keyspace));
	return cassandra;
}


// ************************************************************************
// MultihostCassandra
// ************************************************************************


MultihostCassandra::MultihostCassandra(const std::string & n_keyspace, int n_socket_timeout):
	keyspace(n_keyspace),
	socket_timeout(n_socket_timeout)
{
    // TODO: Move to common_constructor() ?
    common_constructor();
}


void 
libcassandra::MultihostCassandra::common_constructor() {
}


int
libcassandra::MultihostCassandra::add_cluster_node(const std::string & host, int port) {
	cassandra_states.push_back(CassandraStateRow());
	CassandraStateRow & state_row = cassandra_states.back();
	try {
		boost::shared_ptr<Cassandra>  cassandra = connect_cassandra_client(host,port,keyspace,socket_timeout);
		state_row.cassandra = cassandra;
		// clog << "CLOG: Connected " <<  *cassandra << endl; TODO:
		clog << "CLOG: Connected to " << host << ":" << port << " (timeout: " << socket_timeout << " ms)." << endl;
		
	} catch ( apache::thrift::transport::TTransportException & tte) {
		// Host not available  state_row.cassandra stays NULL
		cerr << "CERROR: Creating cassandra instance connected to " << host << ":" << port << " (timeout: " << socket_timeout << " ms)." << endl;
	}
	return cassandra_states.size();
}




