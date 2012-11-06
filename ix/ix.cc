#include "ix.h"

using namespace std;

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance() {
	if (!_ix_manager)
		_ix_manager = new IX_Manager();

	return _ix_manager;
}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".idx";

	RC rc = fileManager->CreateFile(fileName.c_str());
	return rc;
}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) {
		return RC_FAIL;
	}
	int val = remove(fileName.c_str());
	if (val != RC_SUCCESS
	)
		return RC_FAIL;
	else
		return RC_SUCCESS;

}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) {
		return RC_FAIL;
	}
	PF_FileHandle handle = indexHandle.GetHandle();

	if (handle.HasHandle()) {
		if (handle.nameOfFile == fileName.c_str())
			return RC_SUCCESS;
		else
			return RC_FAIL;
	}
	RC rc = fileManager->OpenFile(fileName.c_str(), handle);
	if (rc == RC_FAIL) {
		return rc;
	}

}
RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {
	PF_FileHandle handle = indexHandle.GetHandle();
	if (handle.HasHandle()) {
		RC rc = fileManager->CloseFile(handle); // handle is cleared
		return rc;
	} else
		return RC_FAIL;
}

IX_Manager::IX_Manager() { // Constructor
	fileManager = PF_Manager::Instance();
}

IX_Manager::~IX_Manager() { // Destructor

}

IX_IndexHandle::IX_IndexHandle() { // Constructor
	fileHandle.ClearHandle();
}
IX_IndexHandle::~IX_IndexHandle() { // Destructor
}

// The following two functions are using the following format for the passed key value.
//  1) data is a concatenation of values of the attributes
//  2) For int and real: use 4 bytes to store the value;
//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) { // Insert new index entry

}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) { // Delete index entry
}

PF_FileHandle IX_IndexHandle::GetHandle() {
	return fileHandle;
}
IX_IndexScan::IX_IndexScan() { // Constructor
}
IX_IndexScan::~IX_IndexScan() { // Destructor
}

// for the format of "value", please see IX_IndexHandle::InsertEntry()

// Initialize index scan
RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value) {

}

// Get next matching entry
RC IX_IndexScan::GetNextEntry(RID &rid) {

}
RC IX_IndexScan::CloseScan() { // Terminate index scan
}
// print out the error message for a given return code
void IX_PrintError(RC rc) {

}
