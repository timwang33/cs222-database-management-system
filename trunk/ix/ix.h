#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <sys/stat.h>

#include "../pf/pf.h"
#include "../rm/rm.h"

#define IX_EOF (-1)  // end of the index scan
//Declare vector leaf entry
typedef struct {
	void* key;
	short page;
	short slot;

} LEAF_ENTRY;

//Declare vector middle and root entry
typedef struct {
	void* key;
	short page;
} NONLEAF_ENTRY;

#define KEY_SIZE 4

#define DEBUG false

typedef enum {
	RootNode = 0, BranchNode, LeafNode, OverflowNode, NonLeafNode
} NodeType;

class IX_IndexHandle;

class IX_Manager {
public:
	static IX_Manager* Instance();

	RC CreateIndex(const string tableName, // create new index
			const string attributeName);
	RC DestroyIndex(const string tableName, // destroy an index
			const string attributeName);
	RC OpenIndex(const string tableName, // open an index
			const string attributeName, IX_IndexHandle &indexHandle);
	RC CloseIndex(IX_IndexHandle &indexHandle); // close index

protected:
	IX_Manager(); // Constructor
	~IX_Manager(); // Destructor

private:
	static IX_Manager *_ix_manager;
	PF_Manager *fileManager;
};

class IX_IndexHandle {
public:
	IX_IndexHandle(); // Constructor
	~IX_IndexHandle(); // Destructor

	// The following two functions are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	RC InsertEntry(void *key, const RID &rid); // Insert new index entry
	RC DeleteEntry(void *key, const RID &rid); // Delete index entry
	RC SearchEntry(const void *key, vector<RID> &result); // Delete index entry
	RC insertEntry(short pageNumber, void *key, const RID rid, NONLEAF_ENTRY &return_Entry, bool &check);
	RC readLeafEntries(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_entries);
	RC readNonLeafEntries(PageNum pageNumber, vector<NONLEAF_ENTRY> &middle_entries);
	RC writeLeafPage(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_Entries, short &neighBour);
	RC writeNonLeafPage(PageNum pageNumber, vector<NONLEAF_ENTRY> &middle_Entries);
	RC WriteNonLeafPage(void *buffer, vector<NONLEAF_ENTRY> &middle_Entries);
	RC searchEntry(short pageNumber, const void *key, vector<RID> &result, bool &check);
	RC deleteEntry(const void *key, const RID &rid, short pageNumber, bool &check);
	RC getMostLeftLeaf(short page, short &leftMostPage);

	PF_FileHandle fileHandle;
	AttrType keyType;
	short root;
};

class IX_IndexScan {
public:
	IX_IndexScan(); // Constructor
	~IX_IndexScan(); // Destructor

	// for the format of "value", please see IX_IndexHandle::InsertEntry()
	RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
			CompOp compOp, void *value);

	RC GetNextEntry(RID &rid); // Get next matching entry
	RC CloseScan(); // Terminate index scan
	RC compare(void *entry);
	RC findKey(PageNum page);
	RC GetLeftMost(PageNum page);

	PF_FileHandle handle;
	CompOp operation;

	void *cond_value;
	short root;
	AttrType keyType;
	short currentPage;
	short currentIndex;
	bool hasStartingPoint;
	RID past_RID;

};

// print out the error message for a given return code
void IX_PrintError(RC rc);

#endif
