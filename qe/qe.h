#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>
#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan
using namespace std;

typedef enum {
	MIN = 0, MAX, SUM, AVG, COUNT
} AggregateOp;

// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by the characters

struct Value {
	AttrType type; // type of value
	void *data; // value
};

struct Partition {
	unsigned totalPages;
	PF_FileHandle fHandle;
	void *buffer;
};

struct Record {
	void *data;
	short size;
};
struct Condition {
	string lhsAttr; // left-hand side attribute
	CompOp op; // comparison operator
	bool bRhsIsAttr; // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	string rhsAttr; // right-hand side attribute if bRhsIsAttr = TRUE
	Value rhsValue; // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
	// All the relational operators and access methods are iterators.
public:
	virtual RC getNextTuple(void *data) = 0;
	virtual void getAttributes(vector<Attribute> &attrs) const = 0;
	virtual string getTableName() = 0;
	virtual ~Iterator() {
	}
	;
};

class TableScan: public Iterator {
	// A wrapper inheriting Iterator over RM_ScanIterator
public:
	RM &rm;
	RM_ScanIterator *iter;
	string tablename;
	vector<Attribute> attrs;
	vector<string> attrNames;

	TableScan(RM &rm, const string tablename, const char *alias = NULL) :
			rm(rm) {
		// Get Attributes from RM
		rm.getAttributes(tablename, attrs);

		// Get Attribute Names from RM
		unsigned i;
		for (i = 0; i < attrs.size(); ++i) {
			// convert to char *
			attrNames.push_back(attrs[i].name);
		}
		// Store tablename
				this->tablename = tablename;
				if (alias)
					this->tablename = string(alias);

		// Call rm scan to get iterator
		iter = new RM_ScanIterator();
		rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);


	}
	;

	// Start a new iterator given the new compOp and value
	void setIterator() {
		iter->close();
		delete iter;
		iter = new RM_ScanIterator();
		rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);
	}
	;

	RC getNextTuple(void *data) {
		RID rid;
		return iter->getNextTuple(rid, data);
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tablename;
			tmp += ".";
			tmp += attrs[i].name;
			attrs[i].name = tmp;
		}
	}
	;

	string getTableName() {
		return tablename;
	}
	;

	~TableScan() {
		iter->close();
	}
	;
};

class IndexScan: public Iterator {
	// A wrapper inheriting Iterator over IX_IndexScan
public:
	RM &rm;
	IX_IndexScan *iter;
	IX_IndexHandle handle;
	string tablename;
	vector<Attribute> attrs;

	IndexScan(RM &rm, const IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL) :
			rm(rm) {
		// Get Attributes from RM
		rm.getAttributes(tablename, attrs);

		// Store tablename
		this->tablename = tablename;
		if (alias)
			this->tablename = string(alias);

		// Store Index Handle
		iter = NULL;
		this->handle = indexHandle;
	}
	;

	// Start a new iterator given the new compOp and value
	void setIterator(CompOp compOp, void *value) {
		if (iter != NULL)
		{
			iter->CloseScan();
			delete iter;
		}
		iter = new IX_IndexScan();
		iter->OpenScan(handle, compOp, value);
	}
	;

	RC getNextTuple(void *data) {
		RID rid;
		int rc = iter->GetNextEntry(rid);
		if (rc == 0) {
			rc = rm.readTuple(tablename.c_str(), rid, data);
		}
		return rc;
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tablename;
			tmp += ".";
			tmp += attrs[i].name;
			attrs[i].name = tmp;
		}
	}
	;

	string getTableName() {
		return tablename;
	}
	;
	~IndexScan() {
		iter->CloseScan();
	}
	;
};

class Filter: public Iterator {
	//add fields
	Iterator *input;
	Condition condition;
	vector<Attribute> attrs;
	// Filter operator
public:
	Filter(Iterator *input, const Condition &condition) {
		input->getAttributes(attrs);
		this->input = input;
		this->condition = condition;
	}
	;
	string getTableName() {
		return NULL;
	}
	;
	~Filter();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
	//void getAttributeData(const vector<Attribute> attrs, const string attr, const void *data, void *attrData);
	// void getAttributeFromName(const vector<Attribute> attrs, const string attrName, Attribute attr);
};

class Project: public Iterator {
	//add fields
	Iterator *input;
	vector<string> projectAttrNames;
	vector<Attribute> attrs;

	// Projection operator
public:
	Project(Iterator *input, const vector<string> &attrNames) {
		//attrs contains all Attributes
		input->getAttributes(attrs);
		this->input = input;
		//projectAttrNames contains all project Attribute's Name
		this->projectAttrNames = attrNames;
	}
	string getTableName() {
		return NULL;
	}
	;
	~Project();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

};

class NLJoin: public Iterator {
	// Nested-Loop join operator
public:
	Iterator *leftInput;
	TableScan *rightInput;
	Condition condition;
	vector<Attribute> leftAttrs, rightAttrs;
	void *leftTuple;
	bool endOftable;
	//bool hasReturn;
	void RestartIterator() {
		rightInput->setIterator();
	}
	NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &cond, const unsigned numPages) {
		leftInput = leftIn;
		rightInput = rightIn;
		condition = cond;
		leftIn->getAttributes(leftAttrs);
		rightIn->getAttributes(rightAttrs);
		endOftable = true;
		leftTuple = malloc(200);

	}
	string getTableName() {
		return NULL;
	}
	;
	~NLJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
	// void Restart(TableScan *rightInput);
};

class INLJoin: public Iterator {
	// Index Nested-Loop join operator
public:
	Iterator *leftInput;
	IndexScan *rightInput;
	Condition condition;
	vector<Attribute> leftAttrs, rightAttrs;
	void *leftTuple;
	bool endOftable;

	void RestartIterator() {
		Value value;
		value.type = TypeReal;
		value.data = malloc(200);
		*(float *) value.data = 0.00;
		this->rightInput->setIterator(NO_OP, value.data);

	}
	//set iterator for index
	INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition, const unsigned numPages) {
		this->leftInput = leftIn;
		this->rightInput = rightIn;
		this->condition = condition;
		leftIn->getAttributes(leftAttrs);
		rightIn->getAttributes(rightAttrs);
		this->endOftable = true;
		this->leftTuple = malloc(200);

		RestartIterator();
	}
	string getTableName() {
		return NULL;
	}
	;
	~INLJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

class HashJoin: public Iterator {
	// Hash join operator
public:
	Iterator *leftInput;
	Iterator *rightInput;
	Condition cond;
	unsigned numBuckets;
	vector<Partition> leftPartitions, rightPartitions;
	vector<Attribute> leftAttrs, rightAttrs;
	unsigned currentPartition;
	bool hasHashTable;
	multimap<unsigned, Record> m;
short leftCurrentPage;
short leftCurrentSlot;
short rightIndex;

	HashJoin(Iterator *leftIn, // Iterator of input R
			Iterator *rightIn, // Iterator of input S
			const Condition &condition, // Join condition
			const unsigned numPages // Number of pages can be used to do join (decided by the optimizer)

			) {
		leftInput = leftIn;
		rightInput = rightIn;
		leftIn->getAttributes(leftAttrs);
		rightIn->getAttributes(rightAttrs);
		cond = condition;
		numBuckets = numPages;
		hasHashTable = false;
		currentPartition = 0;
		leftCurrentPage = 0;
		leftCurrentSlot =0;
		rightIndex =0;
		partitionTable(leftInput, leftPartitions, cond.lhsAttr);
		partitionTable(rightInput, rightPartitions, cond.rhsAttr);
	}
	string getTableName() {
		return NULL;
	}
	;
	~HashJoin();

	RC partitionTable(Iterator *input, vector<Partition> &allPartitions, string attr);
	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
};

class Aggregate: public Iterator {
	// Aggregation operator
public:
Attribute aggAttr, gAttr;
	AggregateOp op;
	Iterator *input;
	vector<Attribute> attrs;
	int count;
	float sum, max, min;
	int numberOfParameter;

	void getAggData(const vector<Attribute> attrs, const string attr, const void *data, void *attrData);
	RC Init(Iterator *input, vector<Attribute> attrs, Attribute aggAtt, float *min, float *max);

	Aggregate(Iterator *input, // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			AggregateOp op // Aggregate operation
			)
			{
			this->input = input;
		this->aggAttr = aggAttr;
		this->op = op;
		input->getAttributes(attrs);
		Init(this->input, this->attrs, this->aggAttr, &(this->min), &(this->max));
		sum = min;
		count = 1;
		this->numberOfParameter = 3;
			};

	// Extra Credit
	Aggregate(Iterator *input, // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			Attribute gAttr, // The attribute over which we are grouping the tuples
			AggregateOp op // Aggregate operation
			) {
			 this->input = input;
       this->aggAttr = aggAttr;
       this->op = op;
       this->gAttr = gAttr;
       input->getAttributes(attrs);
       count = 0;
       sum = 0.0;
       max = 0.0;
       min = 0.0;
       this->numberOfParameter = 4;
	   };

	string getTableName() {
		return NULL;
	}
	;
	~Aggregate();

	RC getNextTuple(void *data) {
		return QE_EOF;
	}
	;
	// Please name the output attribute as aggregateOp(aggAttr)
	// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
	// output attrname = "MAX(rel.attr)"
	void getAttributes(vector<Attribute> &attrs) const;
};

#endif
