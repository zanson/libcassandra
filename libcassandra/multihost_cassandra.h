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

#include <time.h>
#include <deque>

#include "libcassandra/cassandra.h"

namespace libcassandra
{
	
/*
 TODO: Deal with exceptions:
 terminate called after throwing an instance of 'apache::thrift::transport::TTransportException'
 
 RULE: We use state of first Cassandra object as template for new ones.
 RULE: State (keyspace, timeouts, consistency levels etc) of all Cassandra objects is consistent 
 */
/**
 * Takes care of thrift clients, connects and returns Cassandra
 * @param[in] host
 * @param[in] port
 * @param[in] keyspace
 * @param[in] socket_timeout   Each socket operation timeouts in miliseconds.
 * @return a shared ptr which points to a Cassandra client
 */
boost::shared_ptr<Cassandra> 
connect_cassandra_client(const std::string & host, int port, const std::string& keyspace, int socket_timeout = 10000);



class MultihostCassandra
{
public:
	MultihostCassandra(const std::string & n_keyspace, int n_socket_timeout = 10000, int n_connection_retry_interval=30);
private:
	void common_constructor();
public:
	/**
	* Adds node on which we should operate. All nodes must be from same cluster
	* @param[in] host
	* @param[in] port
	* @return  Final number of nodes in interface 
	*/
	int add_cluster_node(const std::string & host, int port);
		
	void debug_print_state(const std::string & state_name);
	
	/// Below methods following standard cassandra interface 
	
	/**
	* Retrieve multiple columns by column slice predicate
	*
	* @param[out] result_columns  the result
	* @param[in] key the column key
	* @param[in] column_family the column family
	* @param[in] column_slice_predicate the list of column slice predicate
	* @param[in] consistency_level Consistency level (optional)
	*/
	void getColumns(std::vector<org::apache::cassandra::Column> & result_columns,
			 const std::string & key,
			 const std::string & column_family,
			 const ColumnSlicePredicate & column_slice_predicate,
			 org::apache::cassandra::ConsistencyLevel::type consistency_level);
  
	void inline getColumns(std::vector<org::apache::cassandra::Column> & result_columns,
			  const std::string & key,
			  const std::string & column_family,
			  const ColumnSlicePredicate & column_slice_predicate) {
		getColumns(result_columns, key, column_family, column_slice_predicate, default_read_consistency_level);
	};
	
	/**
	* Retrieve a column value
	*
	* @param[in] key the column key
	* @param[in] column_family the column family
	* @param[in] column_name the column name (optional)
	* @return the value for the column that corresponds to the given parameters
	*/
	std::string getColumnValue(const std::string& key,
                             const std::string& column_family,
                             const std::string& column_name);
	
	
	
	class CassandraStateRow {
		public:
			CassandraStateRow(const std::string & a_host, int a_port) {
				// cassandra 
				host = a_host;
				port = a_port;
				state = init;
				// socket_error_clock = 0; // TODO: Make sure it's standard to be integer-like ?
			}
			boost::shared_ptr<Cassandra> cassandra;
			 
			std::string host;
			int port;
			enum state_t {init, operational} state; 
			// clock_t socket_error_clock; /// http://www.cplusplus.com/reference/clibrary/ctime/clock/
			struct timeval socket_error_timeval;
			
			/**
			* Switches to fully operational state, cassandra is ready to answer queries
			*/
			void switch_to_operational_state(boost::shared_ptr<Cassandra> a_cassandra) {
				cassandra = a_cassandra;
				state = operational;
			}
			
			/**
			* Switches to error state, time when error occured is logged, 
			*/
			void  switch_to_socket_error_state() {
				cassandra.reset();
				state = init;
				// socket_error_clock = clock(); /// TODO: Switch to http://stackoverflow.com/questions/588307/c-obtaining-milliseconds-time-on-linux-clock-doesnt-seem-to-work-properly
							      /// http://linux.die.net/man/2/gettimeofday
				gettimeofday(&socket_error_timeval,NULL);
			}
	};
private:
	const std::string keyspace;
	int socket_timeout; // milisecons
	int connection_retry_interval; // seconds
	org::apache::cassandra::ConsistencyLevel::type default_read_consistency_level; // TODO: Make accessors
	org::apache::cassandra::ConsistencyLevel::type default_write_consistency_level; // TODO: Make accessors
	
	// std::vector<CassandraStateRow> cassandra_states;
	/// http://www.sgi.com/tech/stl/Deque.html
	std::deque<CassandraStateRow> cassandra_states;
	boost::shared_ptr<Cassandra>  pick_cassandra();
	void mark_picked_cassandra_error(const Cassandra & error_cassandra);
	
	
public:
	friend std::ostream& operator<< (std::ostream& o, const CassandraStateRow & state_row);
	
};
	

std::ostream & operator<< (std::ostream & os, const MultihostCassandra::CassandraStateRow & state_row);


} // namespace libcassandra
#endif // __LIBCASSANDRA_MULTIHOST_CASSANDRA_H