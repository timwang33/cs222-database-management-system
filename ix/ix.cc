#include "ix.h"
#include <algorithm>

using namespace std;
bool leafCompare(LEAF_ENTRY leaf1, LEAF_ENTRY leaf2) {
	return (leaf1.key < leaf2.key);
}
bool middleCompare(NONLEAF_ENTRY node1, NONLEAF_ENTRY node2) {
	return (node1.key < node2.key);
}

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

// This can be used for HeaderPage as well as to write Pointer 0
void WriteStartingPage(void * buffer, short page) {
	memcpy((char*) buffer, &page, unit);
}

// This can be used to retrieve pointer 0 as well
void GetStartingPage(void * buffer, short &page) {
	memcpy(&page, (char*) buffer, unit);
}

//Only used in HeaderPage
void WriteKeyType(void * buffer, AttrType type) {
	memcpy((char*) buffer + unit, (char*) &type,sizeof(char));
}

//KeyType is only stored in HeaderPage
void GetKeyType(void * buffer, AttrType &type) {
	char result = 0;
	memcpy(&result, (char*) buffer + unit, sizeof(char));
	type = (AttrType) result;
}

void CalcPtrOffset(short index, NodeType type, char ptrSize, short &result) {
	result = 0;
	if (index == 0)
		return;
	if (type == LeafNode) {
		result = (index - 1) * (KEY_SIZE + ptrSize) + KEY_SIZE;
	} else {
		result = index * (KEY_SIZE + ptrSize);
	}
}

// index must be >0. It is 1-based - No way that we have index=0 as input!!!!
void CalcKeyOffset(short index, NodeType type, char ptrSize, short &result) {

	if (type == LeafNode) {
		result = (index - 1) * (KEY_SIZE + ptrSize);
	} else {
		result = (index - 1) * (KEY_SIZE + ptrSize) + ptrSize;
	}
}

void WriteNodeType(void * buffer, NodeType type) {
	memcpy((char*) buffer + END_OF_PAGE - 3 * unit - 1, (char*) &type, sizeof(char));
}

void GetNodeType(void * buffer, NodeType &type) {
	char result = 0;
	memcpy((char*) &result, (char*) buffer + END_OF_PAGE - 3 * unit - 1, sizeof(char));
	type = (NodeType) result;
}

void WritePtrSize(void * buffer, char size) {
	memcpy((char*) buffer + END_OF_PAGE - 4 * unit, &size, sizeof(char));
}

void GetPtrSize(void * buffer, char &ptrSize) {
	memcpy(&ptrSize, (char*) buffer + END_OF_PAGE - 4 * unit, sizeof(char));
}

void WriteNeighbor(void *buffer, short pageNumber) {
	memcpy((char*) buffer + END_OF_PAGE - 3 * unit, &pageNumber,unit);
}

void GetNeighbor(void *buffer, short &pageNumber) {
	memcpy(&pageNumber, (char*) buffer + END_OF_PAGE - 3 * unit,unit);
}

void CreateNewPage(void *buffer, NodeType type) {
	memset(buffer, 0, PF_PAGE_SIZE);
	WriteNodeType(buffer, type);
	if (type == LeafNode) {
		WritePtrSize(buffer, 4);
	} else { // non-leaf node has ptr of size 2: page number
		WritePtrSize(buffer, 2);
	}
}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == RC_SUCCESS) {
		remove(fileName.c_str());
	}
	RC rc = fileManager->CreateFile(fileName.c_str());
	if (rc == RC_FAIL) {
		return rc;
	} else { // prepare HeaderPage
		PF_FileHandle tempHandle;
		rc = fileManager->OpenFile(fileName.c_str(), tempHandle);

		void *buffer = malloc(PF_PAGE_SIZE);
		WriteTotalEntries(buffer, 0);
		WriteFreeSpace(buffer, PF_PAGE_SIZE - 7);
		WriteStartingPage(buffer, 1);
		RM* rm = RM::Instance();
		vector<Attribute> allAttrs;

		rm->getAttributes(tableName, allAttrs);
		AttrType type = (AttrType) 0;
		for (unsigned i = 0; i < allAttrs.size(); i++) {
			if (strcmp(allAttrs[i].name.c_str(), attributeName.c_str()) == RC_SUCCESS) {
				type = allAttrs[i].type;
				break;
			}
		}

		WriteKeyType(buffer, type);
		tempHandle.WritePage(0, buffer); // end of creating Header Page

		//Root Page
		CreateNewPage(buffer, NonLeafNode);
		WriteStartingPage(buffer, 2); // pointer 0, points to page 2
		WriteFreeSpace(buffer, PF_PAGE_SIZE - 10);
		tempHandle.WritePage(1, buffer);

		//Leaf Page
		CreateNewPage(buffer, LeafNode);
		WriteFreeSpace(buffer, PF_PAGE_SIZE - 8);
		tempHandle.WritePage(2, buffer);

		free(buffer);
		rc = fileManager->CloseFile(tempHandle);
		return rc;
	}

}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) {
		return RC_FAIL;
	}
	int val = remove(fileName.c_str());
	if (val != RC_SUCCESS) {
		return RC_FAIL;
	} else {
		return RC_SUCCESS;
	}

}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle) {
	string fileName = tableName + attributeName + ".idx";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) { // file not exist
		return RC_FAIL;
	}

	if (indexHandle.fileHandle.HasHandle() == RC_SUCCESS) {
		if (strcmp(indexHandle.fileHandle.nameOfFile, fileName.c_str()) == RC_SUCCESS) {
			return RC_SUCCESS;
		} else {
			return RC_FAIL; // already has something
		}
	}

	RC rc = fileManager->OpenFile(fileName.c_str(), indexHandle.fileHandle);
	if (rc == RC_FAIL) {
		return rc;
	} else {
		void *buffer = malloc(PF_PAGE_SIZE);
		indexHandle.fileHandle.ReadPage(0, buffer);
		GetStartingPage(buffer, indexHandle.root);
		GetKeyType(buffer, indexHandle.keyType);
		return RC_SUCCESS;
	}
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {

	if (indexHandle.fileHandle.HasHandle() == RC_SUCCESS) {
		RC rc = fileManager->CloseFile(indexHandle.fileHandle); // handle is cleared
		if (rc == RC_FAIL) {
			return rc;
		} else {
			indexHandle.root = -1;
			return RC_SUCCESS;
		}
	} else {
		return RC_FAIL;
	}
}

IX_Manager::IX_Manager() { // Constructor
	fileManager = PF_Manager::Instance();
}

IX_Manager::~IX_Manager() { // Destructor
	;
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
	NONLEAF_ENTRY return_entry;
	bool has_return;
	insertEntry(root, key, rid, return_entry, has_return);

	if (has_return) {
		short old_root = root;
		// read vector in page root
		void *buffer = malloc(PF_PAGE_SIZE);
		fileHandle.ReadPage(root, buffer);

		short freeSpace=0;

		GetFreeSpace(buffer, freeSpace);

		vector<NONLEAF_ENTRY> nonleaf_entries;
		readNonLeafEntries(root,nonleaf_entries);
		// insert return entry

		nonleaf_entries.push_back(return_entry);
		sort(nonleaf_entries.begin(), nonleaf_entries.end(), middleCompare);

		if (freeSpace >= 6) {
				writeNonLeafPage(root, nonleaf_entries);
				free(buffer);
				return RC_SUCCESS;
		} else {
				short popUpEntry = nonleaf_entries.size() / 2;
				//split to 2 pages,
				// break into 2 subvector
				vector<NONLEAF_ENTRY> subEntriesA, subEntriesB;
				NONLEAF_ENTRY copyNode;

				for (int i = 0; i < (int) popUpEntry; i++) {
						copyNode = nonleaf_entries[i];
						subEntriesA.push_back(copyNode);
					}
					for (int i = popUpEntry + 1; i <= nonleaf_entries.size()  ; i++) {
						copyNode = nonleaf_entries[i];
						subEntriesB.push_back(copyNode);
					}

					short newPage = (short) (fileHandle.GetNumberOfPages());
					// write back to old pages
					writeNonLeafPage(newPage, subEntriesB);
					writeNonLeafPage(root, subEntriesA);


					root = (short) fileHandle.GetNumberOfPages(); // new ROOT
					void *header_page = malloc(PF_PAGE_SIZE);
					fileHandle.ReadPage(0, header_page);
					WriteStartingPage(header_page, root);
					fileHandle.WritePage(0, header_page);

					CreateNewPage(buffer,NonLeafNode);
					WriteStartingPage(buffer,old_root);
					memcpy((char*) buffer +unit,nonleaf_entries[popUpEntry].key,KEY_SIZE);
					memcpy((char*) buffer+unit+KEY_SIZE, &newPage, unit);
					WriteFreeSpace(buffer,PF_PAGE_SIZE-16);
					fileHandle.WritePage(root,buffer);

					free(buffer);
					free (header_page);
					return RC_SUCCESS;

				}
	}
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) { // Delete index entry
//search for it
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

void printLeafEntry(LEAF_ENTRY leaf) {
	cout << "key: " << (int) leaf.key << endl;
	cout << "page: " << leaf.page << endl;
	cout << "slot: " << leaf.slot << endl;
}

void printLeafEntry(LEAF_ENTRY leaf, AttrType key_type) {
	cout << "key: ";
	if (key_type == TypeInt)
		cout << *(int*) leaf.key << endl;
	else if (key_type == TypeReal)
		cout << *(float*) leaf.key << endl;
	cout << "page: " << leaf.page << endl;
	cout << "slot: " << leaf.slot << endl;
	cout << endl;
}
//RC readLeafEntries(IX_IndexHandle &indexHandle, PageNum pageNumber, vector<LEAF_ENTRY> &leaf_entries)
RC IX_IndexHandle::readLeafEntries(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_entries) {

	void* buffer = malloc(PF_PAGE_SIZE);

	//check if function correct
	void *data = malloc(PF_PAGE_SIZE);
	for (int i = 1; i <= 3; i++) {
		short offset = 8 * (i - 1);
		short key = 2 * i;
		short rid = i;
		short page = (short) pageNumber;

		memcpy((char*) data + offset, &key, 4);
		memcpy((char*) data + offset + 4, &rid, 2);
		memcpy((char*) data + offset + 6, &page, 2);
	}

	short testN = 3, freeSpace = PF_PAGE_SIZE - 9 - 8 * 3, neighbor = 1, type = 2, size = 4; //leaf
	memcpy((char*) data + PF_PAGE_SIZE - 2, &testN, 2);
	memcpy((char*) data + PF_PAGE_SIZE - 4, &freeSpace, 2);
	memcpy((char*) data + PF_PAGE_SIZE - 6, &neighbor, 2);
	memcpy((char*) data + PF_PAGE_SIZE - 7, &type, 1);
	memcpy((char*) data + PF_PAGE_SIZE - 9, &size, 2);
	//write to page
	fileHandle.WritePage(pageNumber, data);

	fileHandle.ReadPage(pageNumber, buffer);
	short N; //, freeSpace, nextNeighbor,pointerSize; //number of key in page
	//unsigned nodeType; //0 - root, 1- middle, 2- leaf

	//tao buffer roi read from buffer

	GetTotalEntries(buffer, N);
	//NodeType node_type = LeafNode;
	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	//memcpy(buffer, &freeSpace, unit);
	LEAF_ENTRY entry;
	short offset;

	for (short i = 1; i <= N; i++) {
		CalcKeyOffset(i, LeafNode, ptr_size, offset);
		//offset = 8 * (i - 1);
		memcpy(&entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, LeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		memcpy(&entry.slot, (char*) buffer + +offset + unit,unit);
		leaf_entries.push_back(entry);
		printLeafEntry(entry, this->keyType);
	}

	sort(leaf_entries.begin(), leaf_entries.end(), leafCompare);
	free(buffer);
	free(data);
	return RC_SUCCESS;
}

//RC readNonLeafEntries(IX_IndexHandle &indexHandle, PageNum pageNumber, vector<NONLEAF_ENTRY> &nonleaf_entries)
RC IX_IndexHandle::readNonLeafEntries(PageNum pageNumber, vector<NONLEAF_ENTRY> &nonleaf_entries) {


	void* buffer = malloc(PF_PAGE_SIZE);
	//check if function correct
	void *data = malloc(PF_PAGE_SIZE);

	fileHandle.ReadPage(pageNumber, buffer);
	short N; //, freeSpace, nextNeighbor,pointerSize; //number of key in page
	//unsigned nodeType; //0 - root, 1- middle, 2- leaf

	GetTotalEntries(buffer, N);
	//memcpy(buffer, &freeSpace, unit);
	NONLEAF_ENTRY entry;
	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	short offset;
	short pointer0;
	GetStartingPage(buffer, pointer0);
	for (short i = 1; i <= N; i++) {
		CalcKeyOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(&entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		nonleaf_entries.push_back(entry);
	}

	//reverse(nonleaf_entries.begin(), nonleaf_entries.end());
	sort(nonleaf_entries.begin(), nonleaf_entries.end(), middleCompare);
	free(data);

	free(buffer);
	return RC_SUCCESS;
}

RC IX_IndexHandle::writeLeafPage(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_Entries, short &neighBour) {


	void* buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNumber, buffer);
	char ptr_size = 0;
	GetPtrSize(buffer, ptr_size);
	short i;

	short offset;
	short totals = (short) leaf_Entries.size();
	for (i = 0; i < totals; i++) {

		CalcKeyOffset(i + 1, LeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, leaf_Entries[i].key, KEY_SIZE);
		CalcPtrOffset(i + 1, LeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, &leaf_Entries[i].page, unit);
		memcpy((char*) buffer + offset + unit, &leaf_Entries[i].slot,unit);
	}
	short freeSpace = PF_PAGE_SIZE - 8 * leaf_Entries.size();
	//2 byte N, 2 byte freeSpace, 2 byte neighbor, 1 byte type, 1 byte sizeOfPointer

	freeSpace = freeSpace - 2 - 2 - 2 - 1 - 1;
	//update N and freeSpace
	WriteTotalEntries(buffer, totals);

	WriteFreeSpace(buffer, freeSpace);

	//neighbor doc tu buffer
	short neighBor;
	if (pageNumber == fileHandle.GetNumberOfPages()) {
		neighBor = -1;
	} else {
		neighBor = neighBour;
	}
	//update neighbor
	WriteNeighbor(buffer, neighBor);

	//write back to page
	fileHandle.WritePage(pageNumber, buffer);
	free(buffer);

	return RC_SUCCESS;
}
RC IX_IndexHandle::writeNonLeafPage(PageNum pageNumber, vector<NONLEAF_ENTRY> &nonleaf_entries) {


	void* buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNumber, buffer);
	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	short i;
	short offset;
	short totals = (short) nonleaf_entries.size();
	for (i = 0; i <= totals; i++) {

		CalcKeyOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, nonleaf_entries[i].key, KEY_SIZE);
		CalcPtrOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, &nonleaf_entries[i].page, unit);

	}
	short freeSpace = PF_PAGE_SIZE - (2 + 6 * nonleaf_entries.size());

	//2 byte N, 2 byte freeSpace, 2 byte neighbor, 1 byte type, 2 byte sizeOfPointer
	freeSpace = freeSpace - 2 - 2 - 1 - 2 - 2;
	//update N and freeSpace
	WriteTotalEntries(buffer, totals);
	WriteFreeSpace(buffer, freeSpace);

	//write back to page
	fileHandle.WritePage(pageNumber, buffer);
	free(buffer);
	return RC_SUCCESS;
}

RC IX_IndexHandle::insertEntry(short pageNumber, void * key, const RID rid, NONLEAF_ENTRY &return_Entry, bool &check) {
	void * buffer = malloc(PF_PAGE_SIZE);


	fileHandle.ReadPage(pageNumber, buffer);


	short freeSpace, N, neighbor;
	NodeType node_type;

	GetFreeSpace(buffer, freeSpace);
	GetNodeType(buffer, node_type);
	GetNeighbor(buffer, neighbor);
	/*memcpy(&neighBour, (char*) buffer + PF_PAGE_SIZE - (3 * unit), 2);
	 memcpy(&freeSpace, (char*) buffer + PF_PAGE_SIZE - (2 * unit), 2);
	 memcpy(&N, (char*) buffer + PF_PAGE_SIZE - unit, 2);
	 memcpy(&type, (char*) buffer + PF_PAGE_SIZE - (1 + 3 * unit), 1);*/

	if (node_type == LeafNode) {
		vector<LEAF_ENTRY> leaf_entries;
		LEAF_ENTRY leaf;
		leaf.key = key;
		leaf.page = (short) rid.pageNum;
		leaf.slot = (short) rid.slotNum;
		readLeafEntries(pageNumber, leaf_entries);

		leaf_entries.push_back(leaf);
		sort(leaf_entries.begin(), leaf_entries.end(), leafCompare);

		if (freeSpace >= 8) {
			writeLeafPage(pageNumber, leaf_entries, neighbor);
			free(buffer);
			check = false; // no split
			return RC_SUCCESS;
		} else {
			check = true; //split

			//push new entry first, after that divide page to 2 pages
			short copyUpEntry = leaf_entries.size() / 2;

			void* copyUpKey; // middle value of vector

			return_Entry.key = leaf_entries[copyUpEntry].key;
			return_Entry.page = pageNumber;

			// create 2 new pages
			vector<LEAF_ENTRY> subEntriesA, subEntriesB;
			LEAF_ENTRY copyLeaf;
			for (int i = 0; i < (int) copyUpEntry; i++) {
				copyLeaf = leaf_entries[i];
				subEntriesA.push_back(copyLeaf);
			}
			for (int i = (int)copyUpEntry; i < (int)leaf_entries.size(); i++) {
				copyLeaf = leaf_entries[i];
				subEntriesB.push_back(copyLeaf);
			}

			short newPage = (short) fileHandle.GetNumberOfPages();
			// write back to old leaf page
			writeLeafPage(newPage, subEntriesB, neighbor);

			writeLeafPage(pageNumber, subEntriesA, newPage);
			free(buffer);
			return RC_SUCCESS;
		}

	} else {
		vector<NONLEAF_ENTRY> nonleaf_entries;

		readNonLeafEntries(pageNumber, nonleaf_entries);

		// xu ly node middle
		//compare key with page entries
		unsigned index = 0;

		while (index < nonleaf_entries.size()) {
			if ((int) nonleaf_entries[index].key < (int) key)
				index++;
			else
				break;
		}

		short nextPage;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonleaf_entries[index - 1].page;
		}
		insertEntry(nextPage, key, rid, return_Entry, check);
		if (check) //have to split
		{
			nonleaf_entries.push_back(return_Entry);
			sort(nonleaf_entries.begin(), nonleaf_entries.end(), middleCompare);
			if (freeSpace >= 6) {
				writeNonLeafPage(pageNumber, nonleaf_entries);
				free(buffer);
				check = false; // no split
				return RC_SUCCESS;
			} else {

				short popUpEntry = nonleaf_entries.size() / 2;

				//split to 2 pages, neu co tach thi o day se cap nhat return = popEntry
				vector<NONLEAF_ENTRY> subEntriesA, subEntriesB;
				NONLEAF_ENTRY copyNode;

				for (int i = 0; i < (int) popUpEntry; i++) {
					copyNode = nonleaf_entries[i];
					subEntriesA.push_back(copyNode);
				}
				for (int i = popUpEntry + 1; i < nonleaf_entries.size(); i++) {
					copyNode = nonleaf_entries[i];
					subEntriesB.push_back(copyNode);
				}

				short newPage = (short) (fileHandle.GetNumberOfPages());
				// write back to old leaf page
				writeNonLeafPage(newPage, subEntriesB);
				writeNonLeafPage(pageNumber, subEntriesA);

				//cap nhat return_Entry, popEntry se tro toi newPage, subEntriesB
				return_Entry.key = nonleaf_entries[popUpEntry].key;
				return_Entry.page = newPage;
				check = true;
				free(buffer);
				return RC_SUCCESS;
			}
		}
		//not split
		return RC_SUCCESS;
	}

	return RC_FAIL;

}

