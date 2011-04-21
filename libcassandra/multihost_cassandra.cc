/*
 * LibCassandra
 * Copyright (C) 2011 Mateusz Korniak
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <exception>
#include <stdexcept> 


#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>


#include "libgenthrift/Cassandra.h"
#include "libcassandra/multihost_cassandra.h"
#include "libcassandra/cassandra_factory.h"

using namespace libcassandra;
using namespace std;


static void
debug_check_get_columns(Cassandra & cassandra, const std::string & info_txt) {
	/*
	std::vector<org::apache::cassandra::Column> result_columns;
	ColumnSlicePredicate pred ("first","third");
	clog << "CDEBUG: Quering using " << pred << " ( " << info_txt << " )." << endl;
        cassandra.get_columns(result_columns, "sarah","Data",pred);
        clog << "CDEBUG: Got " << result_columns.size() << " columns." << endl;
	*/
        string res= cassandra.getColumnValue("sarah", "Data", "first");
        clog << "CDEBUG: debug_check_get_columns(): " << info_txt << " - Value in column retrieved as 1st is: " << res << endl;
}


/*


CassandraClient *CassandraFactory::createThriftClient(const string &in_host,
                                                      int in_port,
                                                      int in_conn_timeout,
                                                      int in_recv_timeout,
                                                      int in_send_timeout)
{
  boost::shared_ptr<TSocket> socket(new TSocket(in_host, in_port));

  if (in_conn_timeout >= 0) {
    socket->setConnTimeout(in_conn_timeout);
  }
  if (in_recv_timeout >= 0) {
    socket->setRecvTimeout(in_recv_timeout);
  }
  if (in_send_timeout >= 0) {
    socket->setSendTimeout(in_send_timeout);
  }

  boost::shared_ptr<TTransport> transport = boost::shared_ptr<TTransport>(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  transport->open(); // throws an exception 

  CassandraClient *client= new(std::nothrow) CassandraClient(protocol);

  return client;
}
tr1::shared_ptr<Cassandra> CassandraFactory::create(const string& keyspace)
{
  CassandraClient *thrift_client= createThriftClient(host, port, conn_timeout, recv_timeout, send_timeout);
  tr1::shared_ptr<Cassandra> ret(new Cassandra(thrift_client, host, port, keyspace));
  return ret;
}


 */

boost::shared_ptr<Cassandra> 
libcassandra::connect_cassandra_client(const std::string & host, int port, const std::string& keyspace, int socket_timeout)
{
	clog << "CCALLED: connect_cassandra_client(host='" << host << "', port=" << port << " keyspace='" << keyspace << "' socket_timeout=" << socket_timeout << " )" << endl;
	/*
	boost::shared_ptr<apache::thrift::transport::TSocket> socket(new apache::thrift::transport::TSocket(host, port));
	socket->setConnTimeout(socket_timeout);
	socket->setRecvTimeout(socket_timeout);
	socket->setSendTimeout(socket_timeout);
	boost::shared_ptr<apache::thrift::transport::TTransport> transport(new apache::thrift::transport::TFramedTransport(socket));
	boost::shared_ptr<apache::thrift::protocol::TProtocol> protocol(new apache::thrift::protocol::TBinaryProtocol(transport));
	transport->open(); // throws an exception 	
	org::apache::cassandra::CassandraClient * thrift_client = new(std::nothrow) org::apache::cassandra::CassandraClient(protocol);	
	boost::shared_ptr<Cassandra> cassandra(new Cassandra(thrift_client, host, port, keyspace));	
	*/
	
	CassandraFactory factory(host, port);
	boost::shared_ptr<Cassandra> cassandra(factory.create(keyspace));
	
	string clus_name= cassandra->getClusterName();
	clog << "CDEBUG: connect_cassandra_client(): cluster name: " << clus_name << endl;
	
	// debug_check_get_columns(*cassandra,"connect_cassandra_client(): Just after create.");
	return cassandra;
}


// ************************************************************************
// MultihostCassandra
// ************************************************************************


MultihostCassandra::MultihostCassandra(const std::string & n_keyspace, int n_socket_timeout, int n_connection_retry_interval):
	keyspace(n_keyspace),
	socket_timeout(n_socket_timeout),
	connection_retry_interval(n_connection_retry_interval)
{
	common_constructor();
	clog << "CDEBUG: socket_timeout: " << socket_timeout << endl;
}


void 
libcassandra::MultihostCassandra::common_constructor() {
	default_read_consistency_level =  org::apache::cassandra::ConsistencyLevel::QUORUM;
	default_write_consistency_level =  org::apache::cassandra::ConsistencyLevel::QUORUM;
}


int
libcassandra::MultihostCassandra::add_cluster_node(const std::string & host, int port) {
	/// TODO: Add checking if connected to same cluster ? cluster name ? token ring same ?
	///       What if cluster is changing it's toplogy ? User knows better ;) ?
	cassandra_states.push_back(CassandraStateRow(host,port));
	CassandraStateRow & state_row = cassandra_states.back();
	try {
		boost::shared_ptr<Cassandra>  cassandra = connect_cassandra_client(host,port,keyspace,socket_timeout);
		// clog << "CLOG: Connected " <<  *cassandra << endl; TODO:
		clog << "CLOG: Connected to " << host << ":" << port << " KS: " << keyspace << " (timeout: " << socket_timeout << " ms)." << endl;
		debug_check_get_columns(*cassandra,"After first connect"); 
		state_row.switch_to_operational_state(cassandra);
		debug_check_get_columns(*cassandra,"After switch_to_operational"); 
		
	} catch ( exception & e) { // TODO: Switch to any exception
			// For sure apache::thrift::transport::TTransportException & tte
		// Host not available  state_row.cassandra stays NULL
		state_row.switch_to_socket_error_state();
		cerr << "CERROR: Creating cassandra instance connected to " << host << ":" << port << " (timeout: " << socket_timeout << " ms).";
		cerr << " error: " << e.what();
		cerr << endl;
	}
	debug_print_state("add_cluster_node() finished");
	return cassandra_states.size();
}


void 
libcassandra::MultihostCassandra::debug_print_state(const std::string & state_name) {
	clog << "CDEBUG: States (" << state_name << " clock: " << clock() << ") num:" << cassandra_states.size() << endl;
	
	for (std::deque<CassandraStateRow>::iterator state_it = cassandra_states.begin(); state_it != cassandra_states.end(); ++state_it)
	{
		clog << "CDEBUG: - " << *state_it << endl;
	}
}



void 
libcassandra::MultihostCassandra::getColumns(std::vector<org::apache::cassandra::Column> & result_columns,
					      const std::string & key,
					      const std::string & column_family,
					      const ColumnSlicePredicate & column_slice_predicate,
					      org::apache::cassandra::ConsistencyLevel::type consistency_level) 
{
	if (cassandra_states.empty() ) {
		throw logic_error("No cassandra nodes defined");
	}
	while (1) {
		boost::shared_ptr<Cassandra> picked_cassandra(pick_cassandra()); /// That may throw errors in case of serious failure
		try {
			clog << "CDEBUG: Calling getColumns(key=" << key << ",column_family=" << column_family << " column_slice_predicate=" << column_slice_predicate << " on " << picked_cassandra->getNode() ;
			picked_cassandra->getColumns(result_columns,key,column_family,column_slice_predicate,consistency_level);
			return;
		} catch (org::apache::cassandra::NotFoundException & nfe) { // Exceptions which we propagate
			clog << "CDEBUG: getColumns(key=" << key << ",column_family=" << column_family << ") failed NotFoundException: " << nfe.what() << " - propagating." << endl;
			throw;
		} catch (org::apache::cassandra::InvalidRequestException & ire) { // Exceptions which we propagate
			clog << "CDEBUG: getColumns(key=" << key << ",column_family=" << column_family << ") failed InvalidRequestException: " << ire.what() << " - propagating." << endl;
			throw;
		} catch (exception & e) {
			cerr << "CERROR: getColumns(key=" << key << ",column_family=" << column_family << ") failed: " << e.what() << endl;
			mark_picked_cassandra_error(*picked_cassandra);
		}
	}
}

std::string 
libcassandra::MultihostCassandra::getColumnValue(const std::string& key,
                             const std::string& column_family,
                             const std::string& column_name)
{
	if (cassandra_states.empty() ) {
		throw logic_error("No cassandra nodes defined");
	}
	while (1) {
		boost::shared_ptr<Cassandra> picked_cassandra(pick_cassandra()); /// That may throw errors in case of serious failure
		try {
			return picked_cassandra->getColumnValue(key,column_family,column_name);
		} catch (org::apache::cassandra::NotFoundException & nfe) { // Exceptions which we propagate
			throw;
		} catch (org::apache::cassandra::InvalidRequestException & ire) { // Exceptions which we propagate
			throw;
		} catch (exception & e) {
			cerr << "CERROR: getColumnValue(key='" << key << "', column_family='" << column_family << "', column_name='" << column_name << "') failed: " << e.what() << endl;
			mark_picked_cassandra_error(*picked_cassandra);
		}
	}
}


boost::shared_ptr<Cassandra> 
libcassandra::MultihostCassandra::pick_cassandra()
{
	/// Simplest version
	/// Failed connection goes at end of queue
	/// Same cassandra (first from  sequence ) is used all the time if connection is OK
	/// Connection is initilised when needed
	/// Most of tasks here could have been done in background thread (assuming proper locking of cassandra_states)
	
	
	boost::shared_ptr<Cassandra> picked_cassandra;
	///  for(auto it = seq.begin()   - type deduction  needes  -std=c++0x
	for (std::deque<CassandraStateRow>::iterator state_it = cassandra_states.begin(); state_it != cassandra_states.end(); ++state_it) {
		if (state_it->state == CassandraStateRow::operational) {
			picked_cassandra = state_it->cassandra;
			return picked_cassandra;
		} else if ( state_it->state == CassandraStateRow::init) {
			/// Checking interval since last socket error
			clock_t current_clock = clock();
			if ( current_clock-state_it->socket_error_clock >= CLOCKS_PER_SEC * connection_retry_interval ) {
				clog << "CDEBUG: Retrying to reconnect with: " << *state_it << endl;
				try {
					// boost::shared_ptr<Cassandra>  cassandra 
					picked_cassandra = connect_cassandra_client(state_it->host, state_it->port, keyspace, socket_timeout);
					state_it->switch_to_operational_state(picked_cassandra);
					return picked_cassandra;
					
				} catch (exception & e) {
					clog << "CDEBUG: Reconnect failed: " << e.what() << endl;
					state_it->switch_to_socket_error_state(); // Marking to reset last error time
				}
			}
			
		}
		
	}
	// TODO: What now ? No connection operational all connections recently in error
	//       Try every connection one by one until some timeout reached ?
	throw runtime_error("No connection operational all connections recently in error");
	// return picked_cassandra
}

void 
libcassandra::MultihostCassandra::mark_picked_cassandra_error(const Cassandra & error_cassandra)
{
	debug_print_state("mark_picked_cassandra_error() start.");
	for (std::deque<CassandraStateRow>::iterator state_it = cassandra_states.begin(); state_it != cassandra_states.end(); ++state_it) {
		if (state_it->cassandra.get() == &error_cassandra) {
			state_it->switch_to_socket_error_state();
			debug_print_state("mark_picked_cassandra_error() done.");
			return;
		}
	}
	debug_print_state("Unable to find error_cassandra");
	throw logic_error("Unable to find error_cassandra");
}


std::ostream & libcassandra::operator<< (std::ostream & os, const MultihostCassandra::CassandraStateRow & state_row)
{
	os << " state: " << state_row.state ; 
	os << " socket_error_clock: " << state_row.socket_error_clock;
	os << " (" << state_row.cassandra.get() << "/" << state_row.cassandra.use_count() << ") ";
	os << state_row.host << ":" << state_row.port;
	return os;
}
