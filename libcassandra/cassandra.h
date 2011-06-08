/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#ifndef __LIBCASSANDRA_CASSANDRA_H
#define __LIBCASSANDRA_CASSANDRA_H

#include <string>
#include <vector>
#include <set>
#include <map>
#include <iostream>
// TODO use boost::shared_ptr instead as used in Thrift generated code?:
#include <tr1/memory>   
#include <tr1/tuple>

#include "libgenthrift/cassandra_types.h"

#include "libcassandra/indexed_slices_query.h"
#include "libcassandra/keyspace_definition.h"

namespace org
{
namespace apache
{
namespace cassandra
{
class CassandraClient;
}
}
}

namespace libcassandra
{

class Keyspace;

class ColumnSlicePredicate : public org::apache::cassandra::SlicePredicate
/*
 * Represents column slice predicate
 * Extends org::apache::cassandra::SlicePredicate with few useful constructors
 */
{
public:
  static const int32_t default_count = 100;
  inline ColumnSlicePredicate(const std::string & start, const std::string & finish, int32_t count = default_count, bool reversed = false)
  {
    slice_range.start = start;
    slice_range.finish = finish;
    slice_range.count = count;
    slice_range.reversed = reversed;
    __isset.slice_range= true;
  };
  
  inline ColumnSlicePredicate(const std::string & start, const std::string & finish, bool reversed) 
  {
    slice_range.start = start;
    slice_range.finish = finish;
    slice_range.count = default_count;
    slice_range.reversed = reversed;
    __isset.slice_range= true;
  };
  inline ColumnSlicePredicate(const std::vector<std::string>  & n_column_names) 
  {
    column_names = n_column_names;
    __isset.column_names= true;
  }
  // inline void foo() const {};
  friend std::ostream& operator<< (std::ostream& o, const ColumnSlicePredicate & col_slice_predicate);
};

std::ostream& operator<< (std::ostream& os, const ColumnSlicePredicate & col_slice_predicate);

/**
 * enclosures the insertion of one column
 */
typedef std::tr1::tuple<std::string,  //column family
  std::string,  //key
  std::string,  //name
  std::string   //value
  > ColumnInsertTuple;
 
/**
 * enclosures the insertion of one super column
 */
typedef std::tr1::tuple<std::string,  //column family
  std::string,  //key
  std::string,  //supercolumn
  std::string,  //name
  std::string   //value
  > SuperColumnInsertTuple;
 
typedef std::tr1::tuple<int64_t,      //timestamp
  std::string,  //column family
  std::string,  //key
  std::string,  //name
  std::string   //value
  > TimestampedColumnInsertTuple;
 

class Cassandra
{
public:

public:

  Cassandra();
  Cassandra(org::apache::cassandra::CassandraClient *in_thrift_client,
            const std::string &in_host,
            int in_port);
  Cassandra(org::apache::cassandra::CassandraClient *in_thrift_client,
            const std::string &in_host,
            int in_port,
            const std::string& keyspace);
  ~Cassandra();

  enum FailoverPolicy
  {
    FAIL_FAST= 0, /* return error as is to user */
    ON_FAIL_TRY_ONE_NEXT_AVAILABLE, /* try 1 random server before returning to user */
    ON_FAIL_TRY_ALL_AVAILABLE /* try all available servers in cluster before return to user */
  };

  void setRecvTimeout(int recv_timeout);

  void setSendTimeout(int send_timeout);

  /**
   * @return the underlying cassandra thrift client.
   */
  org::apache::cassandra::CassandraClient *getCassandra();

  /**
   * Log for the current session
   * @param[in] user to use for authentication
   * @param[in] password to use for authentication
   */
  void login(const std::string& user, const std::string& password);

  /**
   * @return the keyspace associated with this session
   */
  std::string getCurrentKeyspace() const;

  /**
   * set the keyspace for the current connection
   * @param[in] ks_name name of the keyspace to specify for current session 
   */
  void setKeyspace(const std::string& ks_name);

  /**
   * @return all the keyspace definitions.
   */
  std::vector<KeyspaceDefinition> getKeyspaces();

  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   * @param[in] level consistency level
   * @param[in] ttl time to live
   */
  void insertColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const std::string& value,
                    org::apache::cassandra::ConsistencyLevel::type level,
                    const int32_t ttl=0);

  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const std::string& value,
                    const int32_t ttl=0);

  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const int64_t value,
                    const int32_t ttl=0);


  /**
   * Insert a column, directly in a columnfamily
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& column_name,
                    const std::string& value,
                    const int32_t ttl=0);

  /**
   * Insert a column, directly in a columnfamily
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& column_name,
                    const int64_t value,
                    const int32_t ttl=0);

  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] timestamp the timestamp to insert with
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   * @param[in] level consistency level
   * @param[in] ttl time to live
   */
  void insertColumn(const int64_t timestamp,
                    const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const std::string& value,
                    org::apache::cassandra::ConsistencyLevel::type level,
                    const int32_t ttl=0);

  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] timestamp the timestamp to insert with
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const int64_t timestamp,
                    const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const std::string& value,
                    const int32_t ttl=0);


  /**
   * Insert a column, possibly inside a supercolumn
   *
   * @param[in] timestamp the timestamp to insert with
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const int64_t timestamp,
                    const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name,
                    const int64_t value,
                    const int32_t ttl=0);


  /**
   * Insert a column, directly in a columnfamily
   *
   * @param[in] timestamp the timestamp to insert with
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const int64_t timestamp,
                    const std::string& key,
                    const std::string& column_family,
                    const std::string& column_name,
                    const std::string& value,
                    const int32_t ttl=0);

  /**
   * Insert a column, directly in a columnfamily
   *
   * @param[in] timestamp the timestamp to insert with
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name
   * @param[in] value the column value
   */
  void insertColumn(const int64_t timestamp,
                    const std::string& key,
                    const std::string& column_family,
                    const std::string& column_name,
                    const int64_t value,
                    const int32_t ttl=0);

  /**
   * Removes all the columns that match the given column path
   *
   * @param[in] key the column or super column key
   * @param[in] col_path the path to the column or super column
   * @param[in] level consistency level
   */
  void remove(const std::string& key,
              const org::apache::cassandra::ColumnPath& col_path,
              org::apache::cassandra::ConsistencyLevel::type level);

  /**
   * Removes all the columns that match the given column path
   *
   * @param[in] key the column or super column key
   * @param[in] col_path the path to the column or super column
   */
  void remove(const std::string& key,
              const org::apache::cassandra::ColumnPath& col_path);

  /**
   * Removes all the columns that match the given arguments
   * Can remove all under a column family, an individual column or supercolumn under a column family, or an individual column under a supercolumn
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name (optional)
   */
  void remove(const std::string& key,
              const std::string& column_family,
              const std::string& super_column_name,
              const std::string& column_name);

  /**
   * Remove a column, possibly inside a supercolumn
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name (optional)
   */
  void removeColumn(const std::string& key,
                    const std::string& column_family,
                    const std::string& super_column_name,
                    const std::string& column_name);


  /**
   * Remove a super column and all columns under it
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name
   */
  void removeSuperColumn(const std::string& key,
                         const std::string& column_family,
                         const std::string& super_column_name);

  /**
   * Rertieve a column.
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name (optional)
   * @param[in] level consistency level
   * @return a column
   */
  org::apache::cassandra::Column getColumn(const std::string& key,
                                           const std::string& column_family,
                                           const std::string& super_column_name,
                                           const std::string& column_name,
                                           org::apache::cassandra::ConsistencyLevel::type level);

  /**
   * Rertieve a column.
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name (optional)
   * @return a column
   */
  org::apache::cassandra::Column getColumn(const std::string& key,
                                           const std::string& column_family,
                                           const std::string& super_column_name,
                                           const std::string& column_name);

  /**
   * Retrieve a column
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name (optional)
   * @return a column
   */
  org::apache::cassandra::Column getColumn(const std::string& key,
                                           const std::string& column_family,
                                           const std::string& column_name);

  /**
   * Retrieve a column value
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_name the column name (optional)
   * @return the value for the column that corresponds to the given parameters
   */
  std::string getColumnValue(const std::string& key,
                             const std::string& column_family,
                             const std::string& super_column_name,
                             const std::string& column_name);

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

  /**
   * Retrieve a column value
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_name the column name (optional)
   * @return the value for the column that corresponds to the given parameters
   *         but as an integer
   */
  int64_t getIntegerColumnValue(const std::string& key,
                                const std::string& column_family,
                                const std::string& column_name);

  org::apache::cassandra::SuperColumn getSuperColumn(const std::string& key,
                                                     const std::string& column_family,
                                                     const std::string& super_column_name,
                                                     org::apache::cassandra::ConsistencyLevel::type level);

  org::apache::cassandra::SuperColumn getSuperColumn(const std::string& key,
                                                     const std::string& column_family,
                                                     const std::string& super_column_name);

  /*
   * Retrieve multiple columns by list of names
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] column_names the list of column names
   * @param[in] level Consistency level (optional)
   * @return A list of found columns
   */
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::string &super_column_name,
                                                         const std::vector<std::string> column_names,
                                                         org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::string &super_column_name,
                                                         const std::vector<std::string> column_names);

  /*
   * Retrieve multiple columns by list of names
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_names the list of column names
   * @param[in] level Consistency level (optional)
   * @return A list of found columns
   */
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::vector<std::string> column_names,
                                                         org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::vector<std::string> column_names);
  
  
  /*
   * Retrieve multiple columns by column slice predicate
   *
   * @param[out] result_columns  the result
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] column_slice_predicate the list of column slice predicate
   * @param[in] consistency_level Consistency level (optional)
   */
  void get_columns(std::vector<org::apache::cassandra::Column> & result_columns,
                   const std::string & key,
                   const std::string & column_family,
                   const ColumnSlicePredicate & column_slice_predicate,
                   org::apache::cassandra::ConsistencyLevel::type consistency_level);
  
  void inline get_columns(std::vector<org::apache::cassandra::Column> & result_columns,
                          const std::string & key,
                          const std::string & column_family,
                          const ColumnSlicePredicate & column_slice_predicate)
  {
    get_columns(result_columns, key, column_family, column_slice_predicate, default_read_consistency_level);
  }

  /**
   * Retrieve multiple columns by range
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_name the super column name (optional)
   * @param[in] range the range for the query
   * @param[in] level Consistency level (optional)
   * @return A list of found columns
   */
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::string &super_column_name,
                                                         const org::apache::cassandra::SliceRange &range,
                                                         org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const std::string &super_column_name,
                                                         const org::apache::cassandra::SliceRange &range);

  /**
   * Retrieve multiple columns by range
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] range the range for the query
   * @param[in] level Consistency level (optional)
   * @return A list of found columns
   */
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const org::apache::cassandra::SliceRange &range,
                                                         org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                         const std::string &column_family,
                                                         const org::apache::cassandra::SliceRange &range);


  /**
   * Retrieve multiple super columns by names
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] super_column_names the list of super column names
   * @param[in] level Consistency level (optional)
   * @return A list of found super columns
   */
  std::vector<org::apache::cassandra::SuperColumn> getSuperColumns(
                                                                   const std::string &key,
                                                                   const std::string &column_family,
                                                                   const std::vector<std::string> super_column_names,
                                                                   org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::SuperColumn> getSuperColumns(
                                                                   const std::string &key,
                                                                   const std::string &column_family,
                                                                   const std::vector<std::string> super_column_names);
  /**
   * Retrieve multiple super columns by range
   *
   * @param[in] key the column key
   * @param[in] column_family the column family
   * @param[in] range the range for the query
   * @param[in] level Consistency level (optional)
   * @return A list of found super columns
   */
  std::vector<org::apache::cassandra::SuperColumn> getSuperColumns(
                                                                   const std::string &key,
                                                                   const std::string &column_family,
                                                                   const org::apache::cassandra::SliceRange &range,
                                                                   org::apache::cassandra::ConsistencyLevel::type level);
  std::vector<org::apache::cassandra::SuperColumn> getSuperColumns(
                                                                   const std::string &key,
                                                                   const std::string &column_family,
                                                                   const org::apache::cassandra::SliceRange &range);


  std::map<std::string, std::vector<org::apache::cassandra::Column> >
  getRangeSlice(const org::apache::cassandra::ColumnParent& col_parent,
                const org::apache::cassandra::SlicePredicate& pred,
                const std::string& start,
                const std::string& finish,
                const int32_t row_count,
                org::apache::cassandra::ConsistencyLevel::type level);

  std::map<std::string, std::vector<org::apache::cassandra::Column> >
  getRangeSlice(const org::apache::cassandra::ColumnParent& col_parent,
                const org::apache::cassandra::SlicePredicate& pred,
                const std::string& start,
                const std::string& finish,
                const int32_t row_count);

  std::map<std::string, std::vector<org::apache::cassandra::SuperColumn> >
  getSuperRangeSlice(const org::apache::cassandra::ColumnParent& col_parent,
                     const org::apache::cassandra::SlicePredicate& pred,
                     const std::string& start,
                     const std::string& finish,
                     const int32_t count,
                     org::apache::cassandra::ConsistencyLevel::type level);

  std::map<std::string, std::vector<org::apache::cassandra::SuperColumn> >
  getSuperRangeSlice(const org::apache::cassandra::ColumnParent& col_parent,
                     const org::apache::cassandra::SlicePredicate& pred,
                     const std::string& start,
                     const std::string& finish,
                     const int32_t count);

  /**
   * Return a list of slices using the given query object
   * @param[in] query object that encapuslates everything needed
   *                  for a query using secondary indexes
   * @return a map of row keys to column names and values
   */
  std::map<std::string, std::map<std::string, std::string> >
  getIndexedSlices(const IndexedSlicesQuery& query);

  /**
   * @return number of columns in a row or super column
   */
  int32_t getCount(const std::string& key,
                   const org::apache::cassandra::ColumnParent& col_parent,
                   const org::apache::cassandra::SlicePredicate& pred,
                   org::apache::cassandra::ConsistencyLevel::type level);

  /**
   * @return number of columns in a row or super column
   */
  int32_t getCount(const std::string& key,
                   const org::apache::cassandra::ColumnParent& col_parent,
                   const org::apache::cassandra::SlicePredicate& pred);

  /**
   * Create a keyspace
   * @param[in] ks_def object representing defintion for keyspace to create
   * @return the schema ID for the keyspace created
   */
  std::string createKeyspace(const KeyspaceDefinition& ks_def);

  /**
   * Update a keyspace
   * @param[in] ks_def object representing defintion for keyspace to update
   * @return the schema ID for the keyspace created
   */
  std::string updateKeyspace(const KeyspaceDefinition& ks_def);

  /**
   * drop a keyspace
   * @param[in] ks_name the name of the keyspace to drop
   * @return the schema ID for the keyspace dropped
   */
  std::string dropKeyspace(const std::string& ks_name);

  /**
   * Create a column family
   * @param[in] cf_def object representing defintion for column family to create
   * @return the schema ID for the column family created
   */
  std::string createColumnFamily(const ColumnFamilyDefinition& cf_def);

  /**
   * Update a column family
   * @param[in] cf_def object representing defintion for column family to update
   * @return the schema ID for the column family created
   */
  std::string updateColumnFamily(const ColumnFamilyDefinition& cf_def);

  /**
   * drop a column family
   * @param[in] cf_name the name of the column family to drop
   * @return the schema ID for the column family dropped
   */
  std::string dropColumnFamily(const std::string& cf_name);

  /**
   * @return the target server cluster name.
   */
  std::string getClusterName();

  /**
   * @return the server version.
   */
  std::string getServerVersion();

  /**
   * @return a string property from the server
   */
  void getStringProperty(std::string &return_val, const std::string &property);

  /**
   * @return hostname
   */
  std::string getHost();

  /**
   * @return port number
   */
  int getPort() const;

  /**
   * Gets the token ring; a map of ranges to host addresses. Represented as a set of TokenRange
   * @param[in] keyspace the name of the keyspace
   * @return token ring map
   */
  std::vector<org::apache::cassandra::TokenRange> describeRing(const std::string &keyspace);

  /**
   * Inserts in the same call to cassandra a set of columns and supercolumns
   * @param[in] columns to insert
   * @param[in] super columns to insert
   */
  void batchInsert(const std::vector<ColumnInsertTuple> &columns,
                   const std::vector<SuperColumnInsertTuple> &super_columns, 
                   org::apache::cassandra::ConsistencyLevel::type level);


  void batchInsert(const std::vector<TimestampedColumnInsertTuple> &columns,
                   org::apache::cassandra::ConsistencyLevel::type level);


  void batchInsert(const std::vector<ColumnInsertTuple> &columns,
                   const std::vector<SuperColumnInsertTuple> &super_columns); 
 
private:

  /**
   * Finds the given keyspace in the list of keyspace definitions
   * @return true if found; false otherwise
   */
  bool findKeyspace(const std::string& name);

  org::apache::cassandra::CassandraClient *thrift_client;
  std::string host;
  int port;
  std::string cluster_name;
  std::string server_version;
  std::string current_keyspace;
  std::vector<KeyspaceDefinition> key_spaces;
  std::map<std::string, std::string> token_map;
  org::apache::cassandra::ConsistencyLevel::type default_read_consistency_level; // TODO: Make accessors
  org::apache::cassandra::ConsistencyLevel::type default_write_consistency_level; // TODO: Make accessors

  Cassandra(const Cassandra&);
  Cassandra &operator=(const Cassandra&);

  typedef std::map<std::string, 
                   std::map<std::string, 
                            std::vector<org::apache::cassandra::Mutation> 
                           > 
                  > MutationsMap;

  static void addToMap(const ColumnInsertTuple &tuple, MutationsMap &mutations); 
  static void addToMap(const TimestampedColumnInsertTuple &tuple, MutationsMap &mutations); 
  static void addToMap(const SuperColumnInsertTuple &tuple, MutationsMap &mutations); 

};

} /* end namespace libcassandra */

#endif /* __LIBCASSANDRA_CASSANDRA_H */
