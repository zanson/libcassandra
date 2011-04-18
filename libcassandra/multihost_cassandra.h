/*
 * LibCassandra
 * Copyright (C) 2011 Mateusz Korniak
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#ifndef __LIBCASSANDRA_MULTIHOST_CASSANDRA_H
#define __LIBCASSANDRA_MULTIHOST_CASSANDRA_H

#include "libcassandra/cassandra.h"

namespace libcassandra
{
	
/*
 TODO: Deal with exceptions:
 terminate called after throwing an instance of 'apache::thrift::transport::TTransportException'
 
 RULE: We use state of first Cassandra object as template for new ones.
 RULE: State (keyspace, timeouts, consistency levels etc) of all Cassandra objects is consistent 
 */

boost::shared_ptr<Cassandra> 
connect_cassandra_client(const std::string & host, int port, const std::string& keyspace, int socket_timeout = 10000);
  /**
   * Takes care of thrift clients, connects and returns Cassandra
   * @param[in] host
   * @param[in] port
   * @param[in] keyspace
   * @param[in] socket_timeout   Each socket operation timeouts in miliseconds.
   * @return a shared ptr which points to a Cassandra client
   */



class MultihostCassandra
{
public:
	MultihostCassandra(const std::string & n_keyspace, int n_socket_timeout = 10000);
	
	int add_cluster_node(const std::string & host, int port);
		/**
		* Adds node on which we should operate. All nodes must be from same cluster
		* @param[in] host
		* @param[in] port
		* @return  Final number of nodes in interface 
		*/
	
private:
	void common_constructor();
	class CassandraStateRow {
		public:
			CassandraStateRow() {
				// cassandra 
				state = init;
			}
			boost::shared_ptr<Cassandra> cassandra;
			 
			enum state_t {init, operational} state; 
	};
	const std::string keyspace;
	int socket_timeout;
	std::vector<CassandraStateRow> cassandra_states;
};

} // namespace libcassandra




#endif // __LIBCASSANDRA_MULTIHOST_CASSANDRA_H