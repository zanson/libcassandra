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
#include <libcassandra/multihost_cassandra.h>

using namespace std;
using namespace libcassandra;

static string host("appserver3.biuro.ant.vpn");
static int port= 9160;
//static int timeout= 5000;

int main()
{
	
	try {
		MultihostCassandra multicassandra("drizzle",10000,3);
		multicassandra.add_cluster_node("appserver3.biuro.ant.vpn",port);
		multicassandra.add_cluster_node("appserver2.biuro.ant.vpn",port);
		
		string res = multicassandra.getColumnValue("sarah", "Data", "first");
		cout << "Value in column retrieved as 1st is: " << res << endl;
		
		std::vector<org::apache::cassandra::Column> result_columns;
		ColumnSlicePredicate pred("first","third");
		
		cout << "Quering using " << pred << endl;
		multicassandra.getColumns(result_columns, "sarah","Data",pred);
		cout << "Got " << result_columns.size() << " columns as range query result." << endl;
		
		for ( std::vector<org::apache::cassandra::Column>::iterator itr = result_columns.begin(); itr != result_columns.end(); itr++ ) {
			cout << "Got column: '" << itr->name << "' : '" << itr->value << "' timestamp: " << itr->timestamp << " ttl: " << itr->ttl << endl;
		}
		pred = ColumnSlicePredicate("first","third",2);
		cout << "Quering using " << pred << endl;
		multicassandra.getColumns(result_columns, "sarah","Data",pred);
		
		cout << "Got " << result_columns.size() << " columns as range query result." << endl;
		for ( std::vector<org::apache::cassandra::Column>::iterator itr = result_columns.begin(); itr != result_columns.end(); itr++ ) {
			cout << "Got column: '" << itr->name << "' : '" << itr->value << "' timestamp: " << itr->timestamp << " ttl: " << itr->ttl << endl;
		}
		
		cout << "Quering about not existing row." << endl;
		try {
			res = multicassandra.getColumnValue("sarah-no-existing-row", "Data", "first");
			cout << "Got unexpected result: " << res << endl;
		} catch (org::apache::cassandra::NotFoundException & nfe ) {
			cout << "NotFoundException: " << nfe.what() << " thrown as expected." << endl;
		}
		
		cout << "Quering about not existing CF." << endl;
		try {
			res = multicassandra.getColumnValue("sarah", "Data-not-existing-CF", "first");
			cout << "Got unexpected result: " << res << endl;
		} catch (org::apache::cassandra::InvalidRequestException & ire ) {
			cout << "InvalidRequestException: " << ire.what() << " thrown as expected." << endl;
		}
		
		cout << "Quering about not existing column." << endl;
		try {
			res = multicassandra.getColumnValue("sarah", "Data", "non-existing-column");
			cout << "Got unexpected result: " << res << endl;
		} catch (org::apache::cassandra::NotFoundException & nfe ) {
			cout << "NotFoundException: " << nfe.what() << " thrown as expected." << endl;
		}
		
		/*
		cout << "Quering using not existing row" << pred << endl;
		multicassandra.getColumns(result_columns, "sarah-not_existing","Data",pred);
		
		cout << "Quering using not existing CF" << pred << endl;
		multicassandra.getColumns(result_columns, "sarah","Data-not_existing",pred);
		*/
		
	} catch (org::apache::cassandra::InvalidRequestException &ire) {
		cout << "org::apache::cassandra::InvalidRequestException: " << ire.why << endl;
		return 1;
	}
	cout << "Example finished." << endl;
	return 0;
}
	
