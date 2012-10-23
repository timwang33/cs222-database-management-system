
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>
#include <cstring>
#include <stdio.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstdlib>

#include "../pf/pf.h"

using namespace std;

// Return code
typedef int RC;

// Record ID
typedef struct {
	unsigned pageNum;
	unsigned slotNum;
} RID;

// Attribute
typedef enum {
	TypeInt = 0,
	TypeReal,
	TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string name; // attribute name
	AttrType type; // attribute type
	AttrLength length; // attribute length
};

struct tableHandle {
	string name; // tablename
	PF_FileHandle * fileHandle;
};
// Comparison Operator
typedef enum {
	EQ_OP = 0, // =
	LT_OP, // <
	GT_OP, // >
	LE_OP, // <=
	GE_OP, // >=
	NE_OP, // !=
	NO_OP
// no condition
} CompOp;


typedef unsigned short positive_twobytes;
typedef short twobytes;

# define catalog_file_name  "catalog.data"
# define column_file_name  "column.data"
# define unit sizeof(short)
# define END_OF_PAGE PF_PAGE_SIZE

# define RM_EOF (-1)  // end of a scan operator
// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

const int zero =0;
class RM_ScanIterator {
public:
	RM_ScanIterator();
	~RM_ScanIterator();

	// "data" follows the same format as RM::insertTuple()
	RC getNextTuple(RID &rid, void *data);
	RC close();
};

// Record Manager
class RM {
public:
	static RM* Instance();

	RC createTable(const string tableName, const vector<Attribute> &attrs);

	RC deleteTable(const string tableName);

	RC getAttributes(const string tableName, vector<Attribute> &attrs);

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
	RC insertTuple(const string tableName, const void *data, RID &rid);

	RC deleteTuples(const string tableName);

	RC deleteTuple(const string tableName, const RID &rid);

	// Assume the rid does not change after update
	RC updateTuple(const string tableName, const void *data, const RID &rid);

	RC readTuple(const string tableName, const RID &rid, void *data);

	RC readAttribute(const string tableName, const RID &rid,
			const string attributeName, void *data);

	RC reorganizePage(const string tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string tableName, const string conditionAttribute,
			const CompOp compOp, // comparision type such as "<" and "="
			const void *value, // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

	RC openTable(const string tableName);

	RC closeTable(const string tableName);
// Extra credit
public:
	RC dropAttribute(const string tableName, const string attributeName);

	RC addAttribute(const string tableName, const Attribute attr);

	RC reorganizeTable(const string tableName);

protected:
	RM();
	~RM();

private:
	static RM *_rm;
	PF_Manager *fileManager;
	PF_FileHandle catalogHandle;
	PF_FileHandle columnHandle;
	vector<Attribute> catalogAttrs;
	vector<Attribute> columnAttrs;
	vector<tableHandle> allTables;

};

#endif

