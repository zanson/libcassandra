#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/column_family_definition.h>
#include <libcassandra/keyspace.h>
#include <libcassandra/keyspace_definition.h>

using namespace std;
using namespace libcassandra;

static string host("appserver3.biuro.ant.vpn");
static int port= 9160;
static int timeout= 5000;

int main()
{

    CassandraFactory factory(host, port);
    tr1::shared_ptr<Cassandra> client(factory.create());

    // Not really needed since the factory timeout sets all 3 by default:
    client->setRecvTimeout(timeout);
    client->setSendTimeout(timeout);

    string clus_name= client->getClusterName();
    cout << "cluster name: " << clus_name << endl;

    vector<KeyspaceDefinition> key_out= client->getKeyspaces();
    for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it)
    {
        cout << "keyspace: " << (*it).getName() << endl;
    }

    try
    {
        /* create keyspace */
        /*
        KeyspaceDefinition ks_def;
        ks_def.setName("drizzle");
        client->createKeyspace(ks_def);
        */
        client->setKeyspace("drizzle");

        /* create standard column family */
        /*
        ColumnFamilyDefinition cf_def;
        cf_def.setName("Data");
        cf_def.setKeyspaceName(ks_def.getName());
        client->createColumnFamily(cf_def);
        */

        /* insert data */
        client->insertColumn("sarah", "Data", "first", "this is data being inserted 1st!");
        client->insertColumn("sarah", "Data", "second", "this is data being inserted 2nd!");
        client->insertColumn("sarah", "Data", "third", "this is data being inserted 3rd!");
        /* retrieve that data */
        string res= client->getColumnValue("sarah", "Data", "first");
        cout << "Value in column retrieved as 1st is: " << res << endl;
        cout << "Value in column retrieved as 2nd is: " << client->getColumnValue("sarah", "Data", "second") << endl;
        cout << "Value in column retrieved as 3rd is: " << client->getColumnValue("sarah", "Data", "third") << endl;
        /*
          std::vector<org::apache::cassandra::Column> getColumns(const std::string &key,
                                                             const std::string &column_family,
                                                             const std::string &super_column_name,
                                                             const org::apache::cassandra::SliceRange &range);
         */
        std::vector<org::apache::cassandra::Column> result_columns;
	
        org::apache::cassandra::SliceRange slice_range;
        slice_range.start = "first";
        slice_range.finish = "third";
	
        // columns = client->getColumns("sarah","Data","",slice_range);
	cout << "Quering using " << ColumnSlicePredicate("first","third") << endl;
        client->get_columns(result_columns, "sarah","Data",ColumnSlicePredicate("first","third"));
	
        cout << "Got " << result_columns.size() << " columns as range query result." << endl;
        for ( std::vector<org::apache::cassandra::Column>::iterator itr = result_columns.begin(); itr != result_columns.end(); itr++ ) {
            cout << "Got column: '" << itr->name << "' : '" << itr->value << "' timestamp: " << itr->timestamp << " ttl: " << itr->ttl << endl;
        }
	cout << "Quering using " << ColumnSlicePredicate("first","third",2) << endl;
        client->get_columns(result_columns, "sarah","Data",ColumnSlicePredicate("first","third",2));
	
        cout << "Got " << result_columns.size() << " columns as range query result." << endl;
        for ( std::vector<org::apache::cassandra::Column>::iterator itr = result_columns.begin(); itr != result_columns.end(); itr++ ) {
            cout << "Got column: '" << itr->name << "' : '" << itr->value << "' timestamp: " << itr->timestamp << " ttl: " << itr->ttl << endl;
        }

    }
    catch (org::apache::cassandra::InvalidRequestException &ire)
    {
        cout << ire.why << endl;
        return 1;
    }

    return 0;
}
