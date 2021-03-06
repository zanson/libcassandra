/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <time.h>
#include <netinet/in.h>

#include <string>
#include <set>
#include <sstream>
#include <iostream>

#include "libgenthrift/Cassandra.h"
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>

#include "libcassandra/cassandra.h"
#include "libcassandra/exception.h"
#include "libcassandra/indexed_slices_query.h"
#include "libcassandra/keyspace.h"
#include "libcassandra/keyspace_definition.h"
#include "libcassandra/util_functions.h"

using namespace std;
using namespace org::apache::cassandra;
using namespace libcassandra;


// ************************************************************************
// ColumnSlicePredicate
// ************************************************************************

std::ostream& libcassandra::operator<< (std::ostream& os, const ColumnSlicePredicate & col_slice_predicate) {
  if (col_slice_predicate.__isset.slice_range) {
    const SliceRange & slice_range = col_slice_predicate.slice_range;
    os << "Columns slice: '"<< slice_range.start << "'..'" << slice_range.finish << "' count: " << slice_range.count;
    if ( slice_range.reversed) {
      os << " reversed";
    }
  } else if (col_slice_predicate.__isset.column_names) {
    const std::vector<std::string>  & column_names = col_slice_predicate.column_names;
    if (column_names.size() > 0 ) {
      os << "Column names (" << column_names.size() << ") '" << column_names.front() << "', ... '" << column_names.back() <<"'";
    } else {
      os << "Zero columns ColumnSlicePredicate";
    }
  } else {
    os << "Undefined state ColumnSlicePredicate";
  }
  return os;
}


// ************************************************************************
// Cassandra
// ************************************************************************
 
Cassandra::Cassandra()
  :
    thrift_client(NULL),
    host(),
    port(0),
    cluster_name(),
    server_version(),
    current_keyspace(),
    key_spaces(),
    token_map()
{
    // TODO: Move to common_constructor() ?
    default_read_consistency_level  = ConsistencyLevel::QUORUM;
    default_write_consistency_level = ConsistencyLevel::QUORUM;
}


Cassandra::Cassandra(CassandraClient *in_thrift_client,
                     const string &in_host,
                     int in_port)
  :
    thrift_client(in_thrift_client),
    host(in_host),
    port(in_port),
    cluster_name(),
    server_version(),
    current_keyspace(),
    key_spaces(),
    token_map()
{
    // TODO: Move to common_constructor() ?
    default_read_consistency_level  = ConsistencyLevel::QUORUM;
    default_write_consistency_level = ConsistencyLevel::QUORUM;
}


Cassandra::Cassandra(CassandraClient *in_thrift_client,
                     const string &in_host,
                     int in_port,
                     const string& keyspace)
  :
    thrift_client(in_thrift_client),
    host(in_host),
    port(in_port),
    cluster_name(),
    server_version(),
    current_keyspace(keyspace),
    key_spaces(),
    token_map()
{
    // TODO: Move to common_constructor() ?
    default_read_consistency_level  = ConsistencyLevel::QUORUM;
    default_write_consistency_level = ConsistencyLevel::QUORUM;
}


Cassandra::~Cassandra()
{
  delete thrift_client;
}

void Cassandra::setRecvTimeout(int recv_timeout) {

  if (recv_timeout > 0) {
    boost::shared_ptr<apache::thrift::transport::TTransport>            t1 = thrift_client->getInputProtocol()->getTransport();
    boost::shared_ptr<apache::thrift::transport::TFramedTransport>      t2 = boost::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport>(t1);
    boost::shared_ptr<apache::thrift::transport::TTransport>            t3 = t2->getUnderlyingTransport();
    boost::shared_ptr<apache::thrift::transport::TSocket>                s = boost::dynamic_pointer_cast<apache::thrift::transport::TSocket>(t3);
    s->setRecvTimeout(recv_timeout);
  }

}

void Cassandra::setSendTimeout(int send_timeout) {

  if (send_timeout > 0) {
    boost::shared_ptr<apache::thrift::transport::TTransport>            t1 = thrift_client->getOutputProtocol()->getTransport();
    boost::shared_ptr<apache::thrift::transport::TFramedTransport>      t2 = boost::dynamic_pointer_cast<apache::thrift::transport::TFramedTransport>(t1);
    boost::shared_ptr<apache::thrift::transport::TTransport>            t3 = t2->getUnderlyingTransport();
    boost::shared_ptr<apache::thrift::transport::TSocket>                s = boost::dynamic_pointer_cast<apache::thrift::transport::TSocket>(t3);
    s->setSendTimeout(send_timeout);
  }

}

CassandraClient *Cassandra::getCassandra()
{
  return thrift_client;
}


void Cassandra::login(const string& user, const string& password)
{
  AuthenticationRequest req;
  req.credentials["username"]= user;
  req.credentials["password"]= password;
  thrift_client->login(req);
}


void Cassandra::setKeyspace(const string& ks_name)
{
  current_keyspace.assign(ks_name);
  thrift_client->set_keyspace(ks_name);
}


string Cassandra::getCurrentKeyspace() const
{
  return current_keyspace;
}


void Cassandra::insertColumn(const int64_t timestamp,
                             const string& key,
                             const string& column_family,
                             const string& super_column_name,
                             const string& column_name,
                             const string& value,
                             const ConsistencyLevel::type level,
                             const int32_t ttl)
{
  ColumnParent col_parent;
  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }
  Column col;
  col.name.assign(column_name);
  col.value.assign(value);
  col.timestamp= timestamp;
  if (ttl) 
  {
    col.ttl=ttl;
    col.__isset.ttl=true;
  }
  /* 
   * actually perform the insert 
   * TODO - validate the ColumnParent before the insert
   */
  thrift_client->insert(key, col_parent, col, level);
}


void Cassandra::remove(const int64_t timestamp,
                       const string &key,
                       const ColumnPath &col_path,
                       const ConsistencyLevel::type level)
{
  thrift_client->remove(key, col_path, timestamp, level);
}

void Cassandra::remove(const string& key,
                       const string& column_family,
                       const string& super_column_name,
                       const string& column_name)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_path.super_column.assign(super_column_name);
    col_path.__isset.super_column= true;
  }
  if (! column_name.empty()) 
  {
    col_path.column.assign(column_name);
    col_path.__isset.column= true;
  }
  remove(key, col_path);
}


Column Cassandra::getColumn(const string& key,
                            const string& column_family,
                            const string& super_column_name,
                            const string& column_name,
                            const ConsistencyLevel::type level)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_path.super_column.assign(super_column_name);
    col_path.__isset.super_column= true;
  }
  col_path.column.assign(column_name);
  col_path.__isset.column= true;
  ColumnOrSuperColumn cosc;
  /* TODO - validate column path */
  thrift_client->get(cosc, key, col_path, level);
  if (cosc.column.name.empty())
  {
    /* throw an exception */
    throw(InvalidRequestException());
  }
  return cosc.column;
}


SuperColumn Cassandra::getSuperColumn(const string& key,
                                      const string& column_family,
                                      const string& super_column_name,
                                      const ConsistencyLevel::type level)
{
  ColumnPath col_path;
  col_path.column_family.assign(column_family);
  col_path.super_column.assign(super_column_name);
  /* this is ugly but thanks to thrift is needed */
  col_path.__isset.super_column= true;
  ColumnOrSuperColumn cosc;
  /* TODO - validate super column path */
  thrift_client->get(cosc, key, col_path, level);
  if (cosc.super_column.name.empty())
  {
    /* throw an exception */
    throw(InvalidRequestException());
  }
  return cosc.super_column;
}


vector<Column> Cassandra::getColumns(
    const string &key,
    const string &column_family,
    const string &super_column_name,
    const vector<string>& column_names,
    const ConsistencyLevel::type level
    )
{

  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<Column> result;

  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }

  pred.column_names = column_names;
  pred.__isset.column_names= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
      it != ret_cosc.end();
      ++it)
  {
    if (! (*it).column.name.empty())
    {
      result.push_back((*it).column);
    }
  }
  return result;
}


vector<Column> Cassandra::getColumns(const string &key,
                                     const std::string &column_family,
                                     const std::string &super_column_name,
                                     const SliceRange &range,
                                     const ConsistencyLevel::type level)
{

  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<Column> result;

  col_parent.column_family.assign(column_family);
  if (! super_column_name.empty()) 
  {
    col_parent.super_column.assign(super_column_name);
    col_parent.__isset.super_column= true;
  }

  pred.slice_range = range;
  pred.__isset.slice_range= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
      it != ret_cosc.end();
      ++it)
  {
    if (! (*it).column.name.empty())
    {
      result.push_back((*it).column);
    }
  }
  return result;
}


vector<SuperColumn> Cassandra::getSuperColumns(const string& key,
                                               const string& column_family,
                                               const vector<string>& super_column_names,
                                               const ConsistencyLevel::type level)
{
  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<SuperColumn> result;

  col_parent.column_family.assign(column_family);

  pred.column_names = super_column_names;
  pred.__isset.column_names= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
       it != ret_cosc.end();
       ++it)
  {
    if (! (*it).super_column.name.empty())
    {
      result.push_back((*it).super_column);
    }
  }
  return result;
}

void Cassandra::get_columns(std::vector<Column> &result_columns,
                            const std::string &key,
                            const std::string &column_family,
                            const ColumnSlicePredicate &column_slice_predicate,
                            const ConsistencyLevel::type consistency_level)
{

    ColumnParent col_parent;
    vector<ColumnOrSuperColumn> ret_cosc;  // TODO: consider faster version returning vector<ColumnOrSuperColumn> without any copying
    col_parent.column_family.assign(column_family);
    thrift_client->get_slice(ret_cosc, key, col_parent, column_slice_predicate, consistency_level);
    result_columns.clear();

    for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
            it != ret_cosc.end();
            ++it)
    {
        if (! it->column.name.empty())
        {
            result_columns.push_back(it->column);
        }
    }
    return;
}


vector<SuperColumn> Cassandra::getSuperColumns(
    const string& key,
    const string& column_family,
    const SliceRange &range,
    const ConsistencyLevel::type level)
{
  ColumnParent col_parent;
  SlicePredicate pred;
  vector<ColumnOrSuperColumn> ret_cosc;
  vector<SuperColumn> result;

  col_parent.column_family.assign(column_family);

  pred.slice_range = range;
  pred.__isset.slice_range= true;

  thrift_client->get_slice(ret_cosc, key, col_parent, pred, level);
  for (vector<ColumnOrSuperColumn>::iterator it= ret_cosc.begin();
       it != ret_cosc.end();
       ++it)
  {
    if (! (*it).super_column.name.empty())
    {
      result.push_back((*it).super_column);
    }
  }
  return result;
}


vector<SuperColumn> Cassandra::getSuperColumns(
    const string& key,
    const string& column_family,
    const SliceRange &range)
{
  return getSuperColumns(key, column_family, range, ConsistencyLevel::QUORUM);
}


map<string, vector<Column> > Cassandra::getRangeSlice(const ColumnParent& col_parent,
                                                      const SlicePredicate& pred,
                                                      const string& start,
                                                      const string& finish,
                                                      const int32_t row_count,
                                                      const ConsistencyLevel::type level)
{
  map<string, vector<Column> > ret;
  vector<KeySlice> key_slices;
  KeyRange key_range;
  key_range.start_key.assign(start);
  key_range.end_key.assign(finish);
  key_range.count= row_count;
  key_range.__isset.start_key= true;
  key_range.__isset.end_key= true;
  thrift_client->get_range_slices(key_slices,
                                  col_parent,
                                  pred,
                                  key_range,
                                  level);
  if (! key_slices.empty())
  {
    for (vector<KeySlice>::iterator it= key_slices.begin();
         it != key_slices.end();
         ++it)
    {
      ret.insert(make_pair((*it).key, getColumnList((*it).columns)));
    }
  }
  return ret;
}


map<string, vector<Column> > Cassandra::getRangeSlice(const ColumnParent& col_parent,
                                                      const SlicePredicate& pred,
                                                      const string& start,
                                                      const string& finish,
                                                      const int32_t row_count)
{
  return getRangeSlice(col_parent, pred, start, finish, row_count, ConsistencyLevel::QUORUM);
}


map<string, vector<SuperColumn> > Cassandra::getSuperRangeSlice(const ColumnParent& col_parent,
                                                                const SlicePredicate& pred,
                                                                const string& start,
                                                                const string& finish,
                                                                const int32_t row_count,
                                                                const ConsistencyLevel::type level)
{
  map<string, vector<SuperColumn> > ret;
  vector<KeySlice> key_slices;
  KeyRange key_range;
  key_range.start_key.assign(start);
  key_range.end_key.assign(finish);
  key_range.count= row_count;
  key_range.__isset.start_key= true;
  key_range.__isset.end_key= true;
  thrift_client->get_range_slices(key_slices,
                                  col_parent,
                                  pred,
                                  key_range,
                                  level);
  if (! key_slices.empty())
  {
    for (vector<KeySlice>::iterator it= key_slices.begin();
         it != key_slices.end();
         ++it)
    {
      ret.insert(make_pair((*it).key, getSuperColumnList((*it).columns)));
    }
  }
  return ret;
}



map<string, vector<SuperColumn> > Cassandra::getSuperRangeSlice(const ColumnParent& col_parent,
                                                                const SlicePredicate& pred,
                                                                const string& start,
                                                                const string& finish,
                                                                const int32_t row_count)
{
  return getSuperRangeSlice(col_parent, pred, start, finish, row_count, ConsistencyLevel::QUORUM);
}


map<string, map<string, string> >
Cassandra::getIndexedSlices(const IndexedSlicesQuery& query)
{
  map<string, map<string, string> > ret_map;
  vector<KeySlice> ret;
  SlicePredicate thrift_slice_pred= createSlicePredicateObject(query);
  ColumnParent thrift_col_parent;
  thrift_col_parent.column_family.assign(query.getColumnFamily());
  thrift_client->get_indexed_slices(ret, 
                                    thrift_col_parent, 
                                    query.getIndexClause(),
                                    thrift_slice_pred,
                                    query.getConsistencyLevel());
  
  for(vector<KeySlice>::iterator it= ret.begin();
      it != ret.end();
      ++it)
  {
    vector<Column> thrift_cols= getColumnList((*it).columns);
    map<string, string> rows;
    for (vector<Column>::iterator inner_it= thrift_cols.begin();
         inner_it != thrift_cols.end();
         ++inner_it)
    {
      rows.insert(make_pair((*inner_it).name, (*inner_it).value));
    }
    ret_map.insert(make_pair((*it).key, rows));
  }

  return ret_map;
}


int32_t Cassandra::getCount(const string& key, 
                            const ColumnParent& col_parent,
                            const SlicePredicate& pred,
                            const ConsistencyLevel::type level)
{
  return thrift_client->get_count(key, col_parent, pred, level);
}


vector<KeyspaceDefinition> Cassandra::getKeyspaces()
{
  vector<KsDef> thrift_ks_defs;
  thrift_client->describe_keyspaces(thrift_ks_defs);
  key_spaces.clear();
  for (vector<KsDef>::iterator it= thrift_ks_defs.begin();
         it != thrift_ks_defs.end();
         ++it)
    {
      KsDef thrift_entry= *it;
      KeyspaceDefinition entry(thrift_entry.name,
                               thrift_entry.strategy_class,
                               thrift_entry.strategy_options,
                               thrift_entry.replication_factor,
                               thrift_entry.cf_defs);
      key_spaces.push_back(entry);
    }

  return key_spaces;
}


string Cassandra::createColumnFamily(const ColumnFamilyDefinition& cf_def)
{
  string schema_id;
  CfDef thrift_cf_def= createCfDefObject(cf_def);
  thrift_client->system_add_column_family(schema_id, thrift_cf_def);
  return schema_id;
}

string Cassandra::updateColumnFamily(const ColumnFamilyDefinition& cf_def)
{
  string schema_id;
  CfDef thrift_cf_def= createCfDefObject(cf_def);
  thrift_client->system_update_column_family(schema_id, thrift_cf_def);
  return schema_id;
}

string Cassandra::dropColumnFamily(const string& cf_name)
{
  string schema_id;
  thrift_client->system_drop_column_family(schema_id, cf_name);
  return schema_id;
}


string Cassandra::createKeyspace(const KeyspaceDefinition& ks_def)
{
  string ret;
  KsDef thrift_ks_def= createKsDefObject(ks_def);
  thrift_client->system_add_keyspace(ret, thrift_ks_def);
  return ret;
}

string Cassandra::updateKeyspace(const KeyspaceDefinition& ks_def)
{
  string ret;
  KsDef thrift_ks_def= createKsDefObject(ks_def);
  thrift_client->system_update_keyspace(ret, thrift_ks_def);
  return ret;
}

string Cassandra::dropKeyspace(const string& ks_name)
{
  string ret;
  thrift_client->system_drop_keyspace(ret, ks_name);
  return ret;
}


string Cassandra::getClusterName()
{
  if (cluster_name.empty())
  {
    thrift_client->describe_cluster_name(cluster_name);
  }
  return cluster_name;
}


string Cassandra::getServerVersion()
{
  if (server_version.empty())
  {
    thrift_client->describe_version(server_version);
  }
  return server_version;
}


string Cassandra::getHost()
{
  return host;
}


int Cassandra::getPort() const
{
  return port;
}


std::vector<TokenRange> Cassandra::describeRing(const std::string &keyspace) {

  vector<TokenRange> ret;
  thrift_client->describe_ring(ret, keyspace);
  return ret;
   
}


void Cassandra::batchInsert(const std::vector<ColumnInsertTuple> &columns,
          const std::vector<SuperColumnInsertTuple> &super_columns,
          const ConsistencyLevel::type level)
{
  MutationsMap mutations;
  
  for (std::vector<ColumnInsertTuple>::const_iterator column = columns.begin();
       column != columns.end(); column++) {
    addToMap(*column, mutations);
}
  
  for (std::vector<SuperColumnInsertTuple>::const_iterator super_column = super_columns.begin();
       super_column != super_columns.end(); super_column++) {
    addToMap(*super_column, mutations);
  }
  
  thrift_client->batch_mutate(mutations, level);
}


void Cassandra::batchInsert(const std::vector<TimestampedColumnInsertTuple> &columns,
                            const ConsistencyLevel::type level)
{
  MutationsMap mutations;

  for (std::vector<TimestampedColumnInsertTuple>::const_iterator column = columns.begin();
       column != columns.end(); column++) {
    addToMap(*column, mutations);
  }

  thrift_client->batch_mutate(mutations, level);
}


void Cassandra::addToMap(const ColumnInsertTuple &tuple, MutationsMap &mutations)
{
  std::string column_family = std::tr1::get<0>(tuple);
  std::string key           = std::tr1::get<1>(tuple);
  std::string name          = std::tr1::get<2>(tuple);
  std::string value         = std::tr1::get<3>(tuple);

  Mutation mutation;

  mutation.column_or_supercolumn.column.name      = name; 
  mutation.column_or_supercolumn.column.value     = value; 
  mutation.column_or_supercolumn.column.timestamp = createTimestamp(); 
  mutation.column_or_supercolumn.__isset.column   = true;
  mutation.__isset.column_or_supercolumn          = true;

  if (mutations.find(key) == mutations.end()) {
    mutations[key] = std::map<std::string, 
                     std::vector<Mutation> >();
  } 

  std::map<std::string, 
             std::vector<Mutation> 
          > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  std::vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);

}


void Cassandra::addToMap(const TimestampedColumnInsertTuple &tuple, MutationsMap &mutations) {
  int64_t     timestamp     = std::tr1::get<0>(tuple);
  std::string column_family = std::tr1::get<1>(tuple);
  std::string key           = std::tr1::get<2>(tuple);
  std::string name          = std::tr1::get<3>(tuple);
  std::string value         = std::tr1::get<4>(tuple);

  Mutation mutation;

  mutation.column_or_supercolumn.column.name      = name; 
  mutation.column_or_supercolumn.column.value     = value; 
  mutation.column_or_supercolumn.column.timestamp = timestamp; 
  mutation.column_or_supercolumn.__isset.column   = true;
  mutation.__isset.column_or_supercolumn          = true;

  if (mutations.find(key) == mutations.end()) {
    mutations[key] = std::map<std::string, 
                     std::vector<Mutation> >();
  } 

  std::map<std::string, 
             std::vector<Mutation> 
          > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  std::vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  mutation_list.push_back(mutation);

}


void Cassandra::addToMap(const SuperColumnInsertTuple &tuple, MutationsMap &mutations) {

  std::string column_family = std::tr1::get<0>(tuple);
  std::string key           = std::tr1::get<1>(tuple);
  std::string super_column  = std::tr1::get<2>(tuple);
  std::string name          = std::tr1::get<3>(tuple);
  std::string value         = std::tr1::get<4>(tuple);

  if (mutations.find(key) == mutations.end()) {
    mutations[key] = std::map<std::string, std::vector<Mutation> >();
  } 

  std::map<std::string, 
             std::vector<Mutation> 
          > &mutations_per_cf = mutations[key];

  if (mutations_per_cf.find(column_family) == mutations_per_cf.end()) {
    mutations_per_cf[column_family] = std::vector<Mutation>();
  }

  std::vector<Mutation> &mutation_list = mutations_per_cf[column_family];

  Mutation mutation;

  //this is awful, is there a better way?

  for (std::vector<Mutation>::iterator it = mutation_list.begin();
       it != mutation_list.end(); it++) {

    if (it->column_or_supercolumn.__isset.super_column &&  
        it->column_or_supercolumn.super_column.name == super_column) {
      mutation = *it;      
      mutation_list.erase(it);
      break;
    }
  }

  mutation.column_or_supercolumn.super_column.name = super_column;
  mutation.column_or_supercolumn.__isset.super_column = true;
  mutation.__isset.column_or_supercolumn = true;

  Column column;

  column.name = name;
  column.value = value;
  column.timestamp = createTimestamp();

  mutation.column_or_supercolumn.super_column.columns.push_back(column);

  mutation_list.push_back(mutation);
}


bool Cassandra::findKeyspace(const string& name)
{
  for (vector<KeyspaceDefinition>::iterator it= key_spaces.begin();
       it != key_spaces.end();
       ++it)
  {
    if (name == it->getName())
    {
      return true;
    }
  }
  return false;
}


