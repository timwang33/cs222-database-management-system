#include "ix.h"

using namespace std;

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance() {
	if (!_ix_manager)
		_ix_manager = new IX_Manager();

	return _ix_manager;
}

void GetTotalEntries(void * buffer, short &totalEntries) {
	memcpy(&totalEntries, (char*) buffer + END_OF_PAGE - unit,unit);

}

void WriteTotalEntries(void * buffer, short totalEntries) {
	memcpy((char*) buffer + END_OF_PAGE - unit, &totalEntries,unit);
}

void GetFreeSpace(void * buffer, short &freeSpace) {
	memcpy(&freeSpace, (char*) buffer + END_OF_PAGE - 2 * unit,unit);
}

void WriteFreeSpace(void * buffer, short freeSpace) {
	memcpy((char*) buffer + END_OF_PAGE - 2 * unit, &freeSpace,unit);
}

void WriteStartingPage(void * buffer, short page) {
	memcpy((char*) buffer, &page,unit);
}

void GetStartingPage(void * buffer, short &page) {
	memcpy(&page, (char*) buffer, unit);
}

void CalcPtrOffset(short index, NodeType type, char ptrSize, short &result) {

	if (type == LeafNode) {
		result = (index -1) * (KEY_SIZE+ ptrSize) + KEY_SIZE;
	}
	else {
		result = index * (KEY_SIZE +ptrSize);
	}
}

void CalcKeyOffset(short index, NodeType type, char ptrSize, short &result) {

	if (type == LeafNode) {
		result = (index -1) * (KEY_SIZE+ ptrSize);
	}
	else {
		result = (index-1) * (KEY_SIZE +ptrSize) + ptrSize;
	}
}

void WriteNodeType(void * buffer, NodeType type) {
	memcpy((char*) buffer + END_OF_PAGE - 3*unit -1, (char*) &type, sizeof(char));
}

void GetNodeType(void * buffer, NodeType &type) {
	char result=0;
	memcpy((char*)&result, (char*) buffer + END_OF_PAGE -3*unit-1, sizeof(char));
	type = (NodeType) result;
}

void WritePtrSize(void * buffer, char size) {
	memcpy((char*) buffer + END_OF_PAGE - 4*unit,  &size, sizeof(char));
}

void GetPtrSize(void * buffer, char &ptrSize) {
	memcpy(&ptrSize, (char*) buffer + END_OF_PAGE - 4*unit, sizeof(char));
}

void WriteNextNeighbor(void *buffer, short pageNumber) {
	memcpy((char*) buffer + END_OF_PAGE - 3*unit,  &pageNumber, unit);
}

void GetNextNeighbor(void *buffer, short &pageNumber) {
	memcpy(&pageNumber, (char*) buffer + END_OF_PAGE - 3*unit, unit);
}

void CreateNewPage(void *buffer, NodeType type) {
	memset(buffer,0,PF_PAGE_SIZE);
	WriteNodeType(buffer,type);
	if (type == LeafNode) {
		WritePtrSize(buffer,4);
	} 	else {  // non-leaf node has ptr of size 2: page number
		WritePtrSize(buffer,2);
	}
}


RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

		if (stat(fileName.c_str(), &stFileInfo) == RC_SUCCESS) {
			remove(fileName.c_str());
		}
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

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) {  // file not exist
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
	else { // openFile
		void *buffer = malloc(PF_PAGE_SIZE);
		WriteTotalEntries(buffer, 0);
		WriteFreeSpace(buffer, PF_PAGE_SIZE-4);
		WriteStartingPage(buffer,1);
		handle.WritePage(0,buffer);
		CreateNewPage(buffer,LeafNode);
		handle.WritePage(1,buffer);

		free(buffer);
		return RC_SUCCESS;

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
