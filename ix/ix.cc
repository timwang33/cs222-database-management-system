#include "ix.h"
#include <algorithm>

using namespace std;

bool leafCompareInt(LEAF_ENTRY leaf1, LEAF_ENTRY leaf2) {
	if ((*(int*) leaf1.key == *(int*) leaf2.key)) {
		if (leaf1.page == leaf2.page)
			return (leaf1.slot < leaf2.slot);
		else
			return (leaf1.page < leaf2.page);
	} else
		return (*(int*) leaf1.key < *(int*) leaf2.key);

}

bool leafCompareReal(LEAF_ENTRY leaf1, LEAF_ENTRY leaf2) {
	if ((*(float*) leaf1.key == *(float*) leaf2.key)) {
		if (leaf1.page == leaf2.page)
			return (leaf1.slot < leaf2.slot);
		else
			return (leaf1.page < leaf2.page);
	} else
		return (*(float*) leaf1.key < *(float*) leaf2.key);

}

bool nonLeafCompareInt(NONLEAF_ENTRY node1, NONLEAF_ENTRY node2) {
	return (*(int*) node1.key < *(int*) node2.key);

}

bool nonLeafCompareReal(NONLEAF_ENTRY node1, NONLEAF_ENTRY node2) {
	return (*(float*) node1.key < *(float*) node2.key);

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

void GetOverflowPage(void * buffer, short &overflow_page) {
	memcpy(&overflow_page, (char*) buffer + END_OF_PAGE - 5 * unit,unit);
}

void WriteOverflowPage(void * buffer, short overflow_page) {
	memcpy((char*) buffer + END_OF_PAGE - 5 * unit, &overflow_page,unit);
}

void CreateNewPage(void *buffer, NodeType type) {
	memset(buffer, 0, PF_PAGE_SIZE);
	WriteNodeType(buffer, type);
	if (type == LeafNode || type == OverflowNode) {
		WritePtrSize(buffer, 4);
		WriteFreeSpace(buffer, PF_PAGE_SIZE - 8);
	} else { // non-leaf node has ptr of size 2: page number
		WritePtrSize(buffer, 2);
		WriteFreeSpace(buffer, PF_PAGE_SIZE - 10);
	}

}

RC GetLeafEntries(void* buffer, vector<LEAF_ENTRY> &leaf_entries) {

	short N = 0;
	GetTotalEntries(buffer, N);

	char ptr_size;
	GetPtrSize(buffer, ptr_size);

	short offset = 0;

	for (short i = 1; i <= N; i++) {
		LEAF_ENTRY entry;
		entry.key = malloc(KEY_SIZE);
		CalcKeyOffset(i, LeafNode, ptr_size, offset);
		memcpy(entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, LeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		memcpy(&entry.slot, (char*) buffer + +offset + unit,unit);
		leaf_entries.push_back(entry);
	}

	return RC_SUCCESS;
}

RC GetNonLeafEntries(void* buffer, vector<NONLEAF_ENTRY> &nonleaf_entries) {

	short N = 0;
	GetTotalEntries(buffer, N);

	char ptr_size;
	GetPtrSize(buffer, ptr_size);

	short offset = 0;

	for (short i = 1; i <= N; i++) {
		NONLEAF_ENTRY entry;
		entry.key = malloc(KEY_SIZE);
		CalcKeyOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		nonleaf_entries.push_back(entry);
	}

	return RC_SUCCESS;
}

RC IX_Manager::CreateIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".data";
	struct stat FileInfo;

	if (stat(fileName.c_str(), &FileInfo) == RC_SUCCESS) {
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
		if (type == TypeVarChar) {
			cout << "Unsupported key " << endl;
			return RC_FAIL;
		}
		WriteKeyType(buffer, type);
		tempHandle.WritePage(0, buffer); // end of creating Header Page

		//Root Page
		CreateNewPage(buffer, NonLeafNode);
		WriteStartingPage(buffer, 2); // pointer 0, points to page 2

		tempHandle.WritePage(1, buffer);

		//Leaf Page
		CreateNewPage(buffer, LeafNode);

		tempHandle.WritePage(2, buffer);

		free(buffer);
		rc = fileManager->CloseFile(tempHandle);
		return rc;
	}

}

RC IX_Manager::DestroyIndex(const string tableName, const string attributeName) {
	string fileName = tableName + attributeName + ".data";
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) != RC_SUCCESS) {
		return RC_SUCCESS;
	}
	int val = remove(fileName.c_str());
	if (val != RC_SUCCESS) {
		return RC_FAIL;
	} else {
		return RC_SUCCESS;
	}

}

RC IX_Manager::OpenIndex(const string tableName, const string attributeName, IX_IndexHandle &indexHandle) {
	string fileName = tableName + attributeName + ".data";
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

	bool has_return = false;
	if (key == NULL || root == -1) {

		return RC_FAIL;
	}
// this got to return RC_FAIL if there is already an entry for (key, rid)
	if (DEBUG == true) {
		cout << endl << "INSERTING key: ";
		if (keyType == TypeInt)
			cout << *(int*) key;
		else if (keyType == TypeReal)
			cout << *(float*) key;
		cout << endl;
	}
	RC rc = insertEntry(root, key, rid, return_entry, has_return);
	if (rc == RC_FAIL) {

		return rc;
	}
	if (has_return == true) {
		short old_root = root;
		root = (short) fileHandle.GetNumberOfPages(); // new ROOT

		void *header_page = malloc(PF_PAGE_SIZE);
		fileHandle.ReadPage(0, header_page);
		WriteStartingPage(header_page, root);
		fileHandle.WritePage(0, header_page);
		free(header_page);

		vector<NONLEAF_ENTRY> nonleaf_vector;
		nonleaf_vector.push_back(return_entry);
		writeNonLeafPage(root, nonleaf_vector);

		void *buffer = malloc(PF_PAGE_SIZE);
		fileHandle.ReadPage(root, buffer);
		WriteStartingPage(buffer, old_root);
		fileHandle.WritePage(root, buffer);
		free(buffer);
		free(return_entry.key);
		return RC_SUCCESS;

	}

	return RC_SUCCESS;
}

RC isEqual(void* left, void *right, AttrType type) {
	if (type == TypeInt) {
		if (*(int*) left == *(int*) right)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	} else if (type == TypeReal) {
		if (*(float*) left == *(float*) right)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	}
	return RC_SUCCESS;
}

RC isLess(const void* left, void *right, AttrType type) {
	if (type == TypeInt) {
		if (*(int*) left < *(int*) right)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	} else if (type == TypeReal) {
		if (*(float*) left < *(float*) right)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	}
	return RC_SUCCESS;
}

RC isKeyEqualLeaf(const void* key, LEAF_ENTRY &entry, AttrType type) {
	if (type == TypeInt) {
		if (*(int*) key == *(int*) entry.key)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	} else if (type == TypeReal) {
		if (*(float*) key == *(float*) entry.key)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	}
	return RC_SUCCESS;
}

RC isKeyEqualNonLeaf(const void* key, NONLEAF_ENTRY &entry, AttrType type) {
	if (type == TypeInt) {
		if (*(int*) key == *(int*) entry.key)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	} else if (type == TypeReal) {
		if (*(float*) key == *(float*) entry.key)
			return RC_SUCCESS;
		else
			return RC_FAIL;
	}
	return RC_SUCCESS;
}

RC IX_IndexHandle::getMostLeftLeaf(short page, short &leftMostPage) {
	void * buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(page, buffer);

	NodeType node_type;

	GetNodeType(buffer, node_type);

	if (node_type == NonLeafNode) {
		short nextPage = 0;
		GetStartingPage(buffer, nextPage);

		free(buffer);
		return getMostLeftLeaf(nextPage, leftMostPage);
	} else { // leaf node has been return
		leftMostPage = page;
		return RC_SUCCESS;
	}
	return RC_FAIL;
}

void CalOffsetOfEmptyPage(short index, short &offset) {
	offset = PF_PAGE_SIZE - (index + 2) * unit;

}
//
void writeEmptyPageToHeader(void * buffer, short pageNumber, short index) {
	short offset;

	CalOffsetOfEmptyPage(index, offset);

	memcpy((char*) buffer + offset, &pageNumber, unit);

}

void readEmptyPageFromHeader(void * buffer, short &pageNumber, short index) {
	short offset;

	CalOffsetOfEmptyPage(index, offset);

	memcpy(&pageNumber, (char*) buffer + offset, unit);

}

void printLeafEntry(LEAF_ENTRY &leaf, AttrType key_type) {

	cout << "key: ";
	if (key_type == TypeInt)
		cout << *(int*) leaf.key;
	else if (key_type == TypeReal)
		cout << *(float*) leaf.key;
	cout << " page: " << leaf.page;
	cout << " slot: " << leaf.slot;
	cout << endl;

}

void printNonLeafEntry(NONLEAF_ENTRY &nonleaf, AttrType key_type) {

	cout << "key: ";
	if (key_type == TypeInt)
		cout << *(int*) nonleaf.key;
	else if (key_type == TypeReal)
		cout << *(float*) nonleaf.key;
	cout << " page: " << nonleaf.page;
	cout << endl;

}
RC IX_IndexHandle::deleteEntry(const void *key, const RID &rid, short pageNumber, bool &check) {

	void * buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNumber, buffer);

	short freeSpace, neighbor;
	NodeType node_type;
	char ptr_size = 0;
	GetFreeSpace(buffer, freeSpace);
	GetNodeType(buffer, node_type);

	GetPtrSize(buffer, ptr_size);
	if (node_type == LeafNode || node_type == OverflowNode) {
		vector<LEAF_ENTRY> leaf_entries;
		GetLeafEntries(buffer, leaf_entries);
		GetNeighbor(buffer, neighbor);
		if (DEBUG == true) {
			cout << "Reading leaf Page: " << pageNumber << endl;
			LEAF_ENTRY entry;
			for (unsigned i = 0; i < leaf_entries.size(); i++) {
				entry = leaf_entries[i];
				printLeafEntry(entry, keyType);
			}
		}
		unsigned index = 0;

		while (index < leaf_entries.size()) {
			if (isEqual((void*) key, leaf_entries[index].key, keyType) == RC_SUCCESS) {
				break;
			} else {
				index++;
			}
		}

		short offset = 0;
		short val = 0;
		bool done = false;
		while (index < leaf_entries.size() && !done) {
			if (isEqual((void*) key, leaf_entries[index].key, keyType) == RC_SUCCESS) {
				CalcPtrOffset(index + 1, LeafNode, ptr_size, offset);
				memcpy(&val, (char*) buffer + offset, unit);
				if ((PageNum) val == rid.pageNum) {
					memcpy(&val, (char*) buffer + offset + unit,unit);
					if ((PageNum) val == rid.slotNum) {
						//delete leafentries[index]
						free(leaf_entries[index].key);
						leaf_entries.erase(leaf_entries.begin() + index);
						if (leaf_entries.size() >= 1) {
							writeLeafPage((PageNum) pageNumber, leaf_entries, neighbor);
							for (unsigned j = 0; j < leaf_entries.size(); j++) {
								free(leaf_entries[j].key);
							}
							free(buffer);
							return RC_SUCCESS;
						} else { // empty page
							// get left most page
							short leftMostPage = 0;
							getMostLeftLeaf(root, leftMostPage);

							// traverse to find the page before this page
							short temp_Neighbor, previousPage; //tim trang truoc pageNumber
							temp_Neighbor = leftMostPage; //chinh bang no
							previousPage = leftMostPage;
							void *temp = malloc(PF_PAGE_SIZE);
							while (temp_Neighbor != pageNumber) {
								previousPage = temp_Neighbor;
								fileHandle.ReadPage(previousPage, temp);
								GetNeighbor(temp, temp_Neighbor);
							}
							if (previousPage != pageNumber) {
								// update that newly found page's neighbor
								fileHandle.ReadPage(previousPage, temp);
								WriteNeighbor(temp, neighbor);
								fileHandle.WritePage(previousPage, temp);
							}
							CreateNewPage(buffer, LeafNode);
							fileHandle.WritePage(pageNumber, buffer);

							free(temp);
							if (leftMostPage != pageNumber || neighbor > 0) {
								void * header_page = malloc(PF_PAGE_SIZE);
								fileHandle.ReadPage(0, header_page);
								short N;
								GetTotalEntries(header_page, N);
								N++;
								writeEmptyPageToHeader(header_page, pageNumber, N);
								WriteTotalEntries(header_page, N);
								fileHandle.WritePage(0, header_page);
								free(header_page);
							}
							check = true;
							free(buffer);
							return RC_SUCCESS;
						}
						done = true;

					} else
						index++;
				} else
					index++;
			} else
				break;
		}
		free(buffer);
		if (done == false) {
			// check overflowpage;
			void *overflow = malloc(PF_PAGE_SIZE);
			NodeType nodeType;
			if (neighbor > 0) {
				fileHandle.ReadPage(neighbor, overflow);
				GetNodeType(overflow, nodeType);
				if (nodeType == OverflowNode) {
					RC rc = deleteEntry(key, rid, neighbor, check);
					free(overflow);
					return rc;

				} else {
					free(overflow);
					return RC_FAIL;
				}
			} else {
				free(overflow);
				return RC_FAIL;
			}
		} else
			return RC_SUCCESS;

	}

	else //Ko phai leafNode
	if (node_type == NonLeafNode) {

		vector<NONLEAF_ENTRY> nonleaf_entries;
		GetNonLeafEntries(buffer, nonleaf_entries);
		if (DEBUG == true) {
			cout << "Reading nonleaf Page: " << pageNumber << endl;

			NONLEAF_ENTRY entry;
			for (unsigned i = 0; i < nonleaf_entries.size(); i++) {
				entry = nonleaf_entries[i];
				printNonLeafEntry(entry, keyType);
			}
		}
		//compare key with page entries
		unsigned index = 0;

		while (index < nonleaf_entries.size()) {
			if (isLess(nonleaf_entries[index].key, (void*) key, keyType) == RC_SUCCESS) {
				index++;
			} else {
				break;
			}

		}

		short nextPage;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonleaf_entries[index - 1].page;
		}
		RC rc = deleteEntry(key, rid, nextPage, check);
		if (rc == RC_FAIL) {
			free(buffer);
			return rc;
		}
		if (check == true) { // leaf page empty and has been deleted
			if (index == 0) {
				if (nonleaf_entries.size() > 0)
					WriteStartingPage(buffer, nonleaf_entries[index].page);
			}

			bool erased = false;
			if (nonleaf_entries.size() > 0) {
				nonleaf_entries.erase(nonleaf_entries.begin() + index);
				erased = true;
			}

			if (nonleaf_entries.size() == 0 && erased) {
				check = true;
			} else {
				check = false;
			}

			WriteNonLeafPage(buffer, nonleaf_entries);
			fileHandle.WritePage(pageNumber, buffer);
			free(buffer);
			return RC_SUCCESS;

		} else {
			free(buffer);
			return RC_SUCCESS;
		}
	}
	return RC_SUCCESS;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) { // Delete index entry
	bool change = false;
	if (DEBUG == true) {
		cout << endl << "DELETING key: ";
		if (keyType == TypeInt)
			cout << *(int*) key;
		else if (keyType == TypeReal)
			cout << *(float*) key;
		cout << endl;
	}
	RC rc = deleteEntry(key, rid, root, change);
	return rc;
	// check changed????
}

RC IX_IndexHandle::SearchEntry(const void *key, vector<RID> &result) { // Delete index entry
	bool found;
	searchEntry(root, key, result, found);

	if (found == true) {
		return RC_SUCCESS;
	} else {
		return RC_FAIL;

	}
}

RC IX_IndexHandle::searchEntry(short pageNumber, const void * key, vector<RID> &result, bool &check) {
	void * buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(pageNumber, buffer);

	unsigned index = 0;

	NodeType node_type;

	GetNodeType(buffer, node_type);

	if (node_type == NonLeafNode) {
		vector<NONLEAF_ENTRY> nonLeaf_entries;
		GetNonLeafEntries(buffer, nonLeaf_entries);

		while (index < nonLeaf_entries.size()) {
			if (isLess(nonLeaf_entries[index].key, (void*) key, keyType) == RC_SUCCESS
			)
				index++;
			else
				break;
		}
		short nextPage = 0;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonLeaf_entries[index - 1].page;
		}

		return searchEntry(nextPage, key, result, check);

	} else { // search leaf node

		short freeSpace = 0;
		GetFreeSpace(buffer, freeSpace);

		vector<LEAF_ENTRY> leaf_entries;
		GetLeafEntries(buffer, leaf_entries);

		while (index < leaf_entries.size()) {
			if (isEqual(leaf_entries[index].key, (void*) key, keyType) != RC_SUCCESS
			)
				index++;
			else
				break;
		}

		if (index == leaf_entries.size()) { // khong tim thay
			check = false;
			return RC_FAIL;
		} else {
			//index = entry dau tien can tim

			while (isEqual(leaf_entries[index].key, (void*) key, keyType) == RC_SUCCESS && index < leaf_entries.size()) {
				RID rid;
				rid.pageNum = (unsigned) leaf_entries[index].page;
				rid.slotNum = (unsigned) leaf_entries[index].slot;
				result.push_back(rid);
				index++;
				check = true;
			}

			return RC_SUCCESS;
		}
	}
}
IX_IndexScan::IX_IndexScan() { // Constructor

}
IX_IndexScan::~IX_IndexScan() { // Destructor

}

// for the format of "value", please see IX_IndexHandle::InsertEntry()

// Initialize index scan
RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value) {
	this->operation = compOp;
	if (value != NULL) {
		cond_value = malloc(KEY_SIZE);
		memcpy(cond_value, value, KEY_SIZE);
	} else
		cond_value = NULL;

	this->handle = indexHandle.fileHandle;
	if (this->handle.HasHandle() != RC_SUCCESS
	)
		return RC_FAIL;
	this->root = indexHandle.root;
	this->keyType = indexHandle.keyType;
	if (compOp == NE_OP)
		return RC_FAIL;
	this->hasStartingPoint = false;
	this->currentPage = 0;
	this->currentIndex = 0;
	return RC_SUCCESS;
}

RC IX_IndexScan::compare(void *entry) {
	if (operation != NO_OP && cond_value == NULL) {
		return RC_FAIL;
	}
	switch (keyType) {
	case TypeInt:
		int int1;
		memcpy(&int1, (char*) entry, KEY_SIZE);
		int int2;
		if (cond_value != NULL
		)
			memcpy(&int2, (char*) cond_value, KEY_SIZE);
		switch (operation) {
		case EQ_OP:
			if (int1 == int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case LT_OP:
			if (int1 < int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case GT_OP:
			if (int1 > int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case LE_OP:
			if (int1 <= int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case GE_OP:
			if (int1 >= int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case NO_OP:
			return RC_SUCCESS;

		case NE_OP:
			return RC_FAIL;
		}
		break;

	case TypeReal:
		float real1;
		memcpy(&real1, (char*) entry, KEY_SIZE);
		float real2;
		if (cond_value != NULL
		)
			memcpy(&real2, (char*) cond_value, KEY_SIZE);
		switch (operation) {
		case EQ_OP:
			if (real1 == real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case LT_OP:
			if (real1 < real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case GT_OP:
			if (real1 > real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case LE_OP:
			if (real1 <= real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case GE_OP:
			if (real1 >= real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;

		case NO_OP:
			return RC_SUCCESS;

		case NE_OP:
			return RC_FAIL;
		}
		break;

	case TypeVarChar:
		return RC_FAIL;
	}
	return RC_SUCCESS;
}

RC IX_IndexScan::findKey(PageNum page) {

	void * buffer = malloc(PF_PAGE_SIZE);
	handle.ReadPage(page, buffer);

	unsigned index = 0;

	NodeType node_type;

	GetNodeType(buffer, node_type);

	if (node_type == NonLeafNode) {
		vector<NONLEAF_ENTRY> nonLeaf_entries;
		GetNonLeafEntries(buffer, nonLeaf_entries);

		while (index < nonLeaf_entries.size()) {

			if (isLess(nonLeaf_entries[index].key, cond_value, keyType) == RC_SUCCESS
			)
				index++;
			else
				break;
		}

		short nextPage = 0;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonLeaf_entries[index - 1].page;
		}
		PageNum next = (PageNum) nextPage;
		free(buffer);
		return findKey(next);
	} else { // search leaf node

		vector<LEAF_ENTRY> leaf_entries;
		GetLeafEntries(buffer, leaf_entries);
		bool found = false;
		index = 0;
		while (index < leaf_entries.size()) {
			if (compare(leaf_entries[index].key) == RC_FAIL) {
				index++;
			} else {
				found = true;
				break;
			}
		}

		if (found == true) {
			currentPage = (short) page;
			currentIndex = index + 1;
			free(buffer);
			return RC_SUCCESS;
		} else {
			free(buffer);
			return RC_FAIL;
			/*
			 short nextPage = 0;
			 short totals = 0;
			 do {
			 GetNeighbor(buffer, nextPage);
			 handle.ReadPage((PageNum) nextPage, buffer);
			 GetTotalEntries(buffer, totals);
			 } while (totals != 0 && nextPage != 0);
			 free(buffer);
			 return findKey((PageNum) nextPage);*/
		}

	}
}

RC IX_IndexScan::GetLeftMost(PageNum page) {

	void * buffer = malloc(PF_PAGE_SIZE);
	handle.ReadPage(page, buffer);

	NodeType node_type;

	GetNodeType(buffer, node_type);

	if (node_type == NonLeafNode) {
		short nextPage = 0;
		GetStartingPage(buffer, nextPage);

		free(buffer);
		return GetLeftMost((PageNum) nextPage);
	} else { // search leaf node

		short totals = 0;
		GetTotalEntries(buffer, totals);

		if (totals != 0) {
			currentPage = (short) page;
			currentIndex = 1;
			free(buffer);
			return RC_SUCCESS;
		} else {
			free(buffer);
			return RC_FAIL;/*
			 short nextPage = 0;
			 GetNeighbor(buffer, nextPage);
			 if (nextPage == 0) {
			 free(buffer);
			 return RC_FAIL;
			 } else {
			 free(buffer);
			 return GetLeftMost((PageNum) nextPage);
			 }*/
		}
	}
}

// Get next matching entry
RC IX_IndexScan::GetNextEntry(RID &rid) {
	RC rc;
	if (hasStartingPoint == false) {
		if (operation == LT_OP || operation == LE_OP || operation == NO_OP) {
			rc = GetLeftMost(root);

		} else {
			rc = findKey(root);

		}
		if (rc == RC_SUCCESS) {
			hasStartingPoint = true;
			past_RID.pageNum = 0;
			past_RID.slotNum = 0;
		}
	}

	if (hasStartingPoint == false)
		return IX_EOF;

	void *buffer = malloc(PF_PAGE_SIZE);
	bool pass = false;
	char ptr_size = 0;
	short offset = 0;
	while (pass == false) {
		handle.ReadPage((PageNum) currentPage, buffer); // that page must be a LEAF
		GetPtrSize(buffer, ptr_size);
		short totals = 0;
		GetTotalEntries(buffer, totals);

		//check to see if entries sequence are still intact?
		if (past_RID.pageNum != 0 && past_RID.slotNum != 0) {
			short val;
			CalcPtrOffset(currentIndex - 1, LeafNode, ptr_size, offset);
			memcpy(&val, (char*) buffer + offset, unit);
			bool okay = false;
			if (past_RID.pageNum == (PageNum) val) {
				memcpy(&val, (char*) buffer + offset + unit,unit);
				if (past_RID.slotNum == (PageNum) val) {
					okay = true;
				}
			}
			if (okay == false)
				currentIndex--;
		}
		if (currentIndex > totals) {
			GetNeighbor(buffer, currentPage);
			if (currentPage <= 0)
				return IX_EOF;
			currentIndex = 1;
			past_RID.pageNum = 0;
			past_RID.slotNum = 0;

		} else
			pass = true;
	}

	CalcKeyOffset(currentIndex, LeafNode, ptr_size, offset);
	void *key_value = malloc(KEY_SIZE);
	memcpy(key_value, (char*) buffer + offset, KEY_SIZE);
	if (compare(key_value) == RC_SUCCESS) {
		short val;
		CalcPtrOffset(currentIndex, LeafNode, ptr_size, offset);
		memcpy(&val, (char*) buffer + offset, unit);
		rid.pageNum = (unsigned) val;
		past_RID.pageNum = (unsigned) val;
		memcpy(&val, (char*) buffer + offset + unit,unit);
		rid.slotNum = (unsigned) val;
		past_RID.slotNum = (unsigned) val;
		currentIndex++;
		free(buffer);
		return RC_SUCCESS;
	} else {
		free(buffer);
		return IX_EOF;
	}

}

RC IX_IndexScan::CloseScan() { // Terminate index scan
	hasStartingPoint = false;
	currentPage = 0;
	currentIndex = 0;
	if (cond_value != NULL) {
		free(cond_value);
	}
	past_RID.pageNum = 0;
	past_RID.slotNum = 0;
	return RC_SUCCESS;
}

// print out the error message for a given return code
void IX_PrintError(RC rc) {

	if (rc == RC_SUCCESS
	)
		cout << "success" << endl;
	else if (rc == RC_FAIL
	)
		cout << "fail" << endl;
	else
		cout << "unknow error" << endl;

}

//RC readLeafEntries(IX_IndexHandle &indexHandle, PageNum pageNumber, vector<LEAF_ENTRY> &leaf_entries)
RC IX_IndexHandle::readLeafEntries(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_entries) {

	void* buffer = malloc(PF_PAGE_SIZE);

	fileHandle.ReadPage(pageNumber, buffer);
	short N;

//tao buffer roi read from buffer

	GetTotalEntries(buffer, N);

	char ptr_size;
	GetPtrSize(buffer, ptr_size);

	LEAF_ENTRY entry;
	short offset;

	for (short i = 1; i <= N; i++) {
		CalcKeyOffset(i, LeafNode, ptr_size, offset);

		entry.key = malloc(KEY_SIZE);
		memcpy(entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, LeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		memcpy(&entry.slot, (char*) buffer + +offset + unit,unit);
		leaf_entries.push_back(entry);
//printLeafEntry(entry, this->keyType);
	}

	if (leaf_entries.size() > 1) {
		if (keyType == TypeInt)
			sort(leaf_entries.begin(), leaf_entries.end(), leafCompareInt);
		else if (keyType == TypeReal)
			sort(leaf_entries.begin(), leaf_entries.end(), leafCompareReal);
	}
	free(buffer);

	return RC_SUCCESS;
}

RC IX_IndexHandle::readNonLeafEntries(PageNum pageNumber, vector<NONLEAF_ENTRY> &nonleaf_entries) {

	void* buffer = malloc(PF_PAGE_SIZE);

	fileHandle.ReadPage(pageNumber, buffer);
	short N;

	GetTotalEntries(buffer, N);

	NONLEAF_ENTRY entry;
	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	short offset;
	short pointer0;
	GetStartingPage(buffer, pointer0);
	for (short i = 1; i <= N; i++) {
		CalcKeyOffset(i, NonLeafNode, ptr_size, offset);
		entry.key = malloc(KEY_SIZE);
		memcpy(entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		nonleaf_entries.push_back(entry);
	}

	free(buffer);
	return RC_SUCCESS;
}

RC IX_IndexHandle::writeLeafPage(PageNum pageNumber, vector<LEAF_ENTRY> &leaf_Entries, short &neighBour) {

	void* buffer = malloc(PF_PAGE_SIZE);
	if (pageNumber < fileHandle.GetNumberOfPages()) {
		fileHandle.ReadPage(pageNumber, buffer);
	} else {
		CreateNewPage(buffer, LeafNode);
	}
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

RC IX_IndexHandle::WriteNonLeafPage(void *buffer, vector<NONLEAF_ENTRY> &nonleaf_entries) {

	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	short i;
	short offset;
	short totals = (short) nonleaf_entries.size();
	for (i = 0; i < totals; i++) {

		CalcKeyOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, nonleaf_entries[i].key, KEY_SIZE);
		CalcPtrOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, &nonleaf_entries[i].page, unit);

	}
	short freeSpace = PF_PAGE_SIZE - (2 + 6 * nonleaf_entries.size());

//2 byte N, 2 byte freeSpace, 2 byte neighbor, 1 byte type, 1 byte sizeOfPointer
	freeSpace = freeSpace - 2 - 2 - 2 - 1 - 1;

//update N and freeSpace
	WriteTotalEntries(buffer, totals);
	WriteFreeSpace(buffer, freeSpace);

	return RC_SUCCESS;
}
RC IX_IndexHandle::writeNonLeafPage(PageNum pageNumber, vector<NONLEAF_ENTRY> &nonleaf_entries) {

	void* buffer = malloc(PF_PAGE_SIZE);
	if (pageNumber < fileHandle.GetNumberOfPages()) {
		fileHandle.ReadPage(pageNumber, buffer);
	} else {
		CreateNewPage(buffer, NonLeafNode);
	}
	char ptr_size;
	GetPtrSize(buffer, ptr_size);
	short i;
	short offset;
	short totals = (short) nonleaf_entries.size();
	for (i = 0; i < totals; i++) {

		CalcKeyOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, nonleaf_entries[i].key, KEY_SIZE);
		CalcPtrOffset(i + 1, NonLeafNode, ptr_size, offset);
		memcpy((char*) buffer + offset, &nonleaf_entries[i].page, unit);

	}
	short freeSpace = PF_PAGE_SIZE - (2 + 6 * nonleaf_entries.size());

//2 byte N, 2 byte freeSpace, 2 byte neighbor, 1 byte type, 1 byte sizeOfPointer
	freeSpace = freeSpace - 2 - 2 - 2 - 1 - 1;

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

	short freeSpace, neighbor;
	NodeType node_type;

	GetFreeSpace(buffer, freeSpace);
	GetNodeType(buffer, node_type);
	GetNeighbor(buffer, neighbor);

	if (node_type == LeafNode || node_type == OverflowNode) {
		vector<LEAF_ENTRY> leaf_entries;
		GetLeafEntries(buffer, leaf_entries);
		if (DEBUG == true) {
			cout << "Reading leaf Page: " << pageNumber << endl;
			LEAF_ENTRY entry;
			for (unsigned i = 0; i < leaf_entries.size(); i++) {
				entry = leaf_entries[i];
				printLeafEntry(entry, keyType);
			}
		}
		for (unsigned i = 0; i < leaf_entries.size(); i++) {
			if (isEqual(key, leaf_entries[i].key, keyType) == RC_SUCCESS) {
				if (rid.pageNum == (unsigned) leaf_entries[i].page && rid.slotNum == (unsigned) leaf_entries[i].slot) {

					free(buffer);
					return RC_FAIL;
				}
			} else if (isLess(key, leaf_entries[i].key, keyType) == RC_SUCCESS) {
				break;
			}
		}

		LEAF_ENTRY leaf;
		leaf.key = malloc(KEY_SIZE);
		memcpy(leaf.key, key, KEY_SIZE);
		leaf.page = (short) rid.pageNum;
		leaf.slot = (short) rid.slotNum;
		//printLeafEntry(leaf, this->keyType);
		// some cases require free(leaf.key)
		if (freeSpace >= 8) {
			leaf_entries.push_back(leaf);
			if (leaf_entries.size() > 1) {
				if (keyType == TypeInt)
					sort(leaf_entries.begin(), leaf_entries.end(), leafCompareInt);
				else if (keyType == TypeReal)
					sort(leaf_entries.begin(), leaf_entries.end(), leafCompareReal);
			}

			// print
			//LEAF_ENTRY entry;
			//for (unsigned i = 0; i < leaf_entries.size(); i++) {
			//	entry = leaf_entries[i];
			//	printLeafEntry(entry, keyType);
			//}

			writeLeafPage(pageNumber, leaf_entries, neighbor);
			free(buffer);

			for (unsigned i = 0; i < leaf_entries.size(); i++) {
				free(leaf_entries[i].key);
			}
			return RC_SUCCESS;
		} else {

			void *minKey = malloc(KEY_SIZE);
			void *maxKey = malloc(KEY_SIZE);

			//push new entry first, after that divide page to 2 pages
			int position = 0;
			unsigned N = leaf_entries.size();
			short direction = 0;

			memcpy(minKey, leaf_entries[0].key, KEY_SIZE);
			memcpy(maxKey, leaf_entries[N - 1].key, KEY_SIZE);

			if (isLess(minKey, maxKey, keyType) == RC_SUCCESS) {
				void * midKey = malloc(KEY_SIZE);
				leaf_entries.push_back(leaf);
				N++;
				if (leaf_entries.size() > 1) {
					if (keyType == TypeInt)
						sort(leaf_entries.begin(), leaf_entries.end(), leafCompareInt);
					else if (keyType == TypeReal)
						sort(leaf_entries.begin(), leaf_entries.end(), leafCompareReal);
				}
				memcpy(minKey, leaf_entries[0].key, KEY_SIZE);
				memcpy(maxKey, leaf_entries[N - 1].key, KEY_SIZE);
				memcpy(midKey, leaf_entries[N / 2].key, KEY_SIZE);

				if (isEqual(midKey, maxKey, keyType) == RC_SUCCESS) {
					direction = -1;
				} else
					direction = 1;

				position = N / 2 + direction;
				void *nextKey = malloc(KEY_SIZE);
				while (position >= 0 && position < (int) N) {
					memcpy(nextKey, leaf_entries[position].key, KEY_SIZE);
					if (isEqual(nextKey, midKey, keyType) == RC_SUCCESS) {
						position += direction;
					} else {
						break;
					}
				}

				if (direction == 1) {
					position--;
				}
				short copyUpEntry = (short) position;

				// create 2 new pages
				vector<LEAF_ENTRY> subEntriesA, subEntriesB;
				LEAF_ENTRY copyLeaf;
				for (int i = 0; i <= (int) copyUpEntry; i++) {
					copyLeaf = leaf_entries[i];
					subEntriesA.push_back(copyLeaf);
				}
				for (int i = (int) copyUpEntry + 1; i < (int) leaf_entries.size(); i++) {
					copyLeaf = leaf_entries[i];
					subEntriesB.push_back(copyLeaf);
				}

				short newPage = 0;
				//check HeaderPage to see if any old empty page is available
				void* header_page = malloc(PF_PAGE_SIZE);
				fileHandle.ReadPage(0, header_page);
				short total;
				GetTotalEntries(header_page, total);
				if (total > 0) {
					readEmptyPageFromHeader(header_page, newPage, total);
					writeEmptyPageToHeader(header_page, 0, total);
					WriteTotalEntries(header_page, total - 1);
					fileHandle.WritePage(0, header_page);
				} else
					newPage = (short) fileHandle.GetNumberOfPages();
				// write back to old leaf page
				writeLeafPage(newPage, subEntriesB, neighbor);

				writeLeafPage(pageNumber, subEntriesA, newPage);
				check = true; // has Return_Entry
				return_Entry.key = malloc(KEY_SIZE);
				memcpy(return_Entry.key, leaf_entries[copyUpEntry].key, KEY_SIZE);
				return_Entry.page = newPage;
				free(buffer);
				free(minKey);
				free(maxKey);
				free(midKey);
				free(nextKey);
				free(header_page);
				for (unsigned i = 0; i < leaf_entries.size(); i++) {
					free(leaf_entries[i].key);
				}
				return RC_SUCCESS;
			} else if (isEqual(minKey, maxKey, keyType) == RC_SUCCESS) {

				if (isEqual(minKey, key, keyType) == RC_SUCCESS) {
					// deal with overflow!

					void *overflow_page = malloc(PF_PAGE_SIZE);
					bool create_new = false;
					if (neighbor > 0) {
						fileHandle.ReadPage(neighbor, overflow_page);
						NodeType page_type;
						GetNodeType(overflow_page, page_type);
						if (page_type != OverflowNode)
							create_new = true;
					} else
						create_new = true;
					if (create_new == true) {
						CreateNewPage(overflow_page, OverflowNode);

						short overflow_pagenumber = 0;
						//check HeaderPage to see if any old empty page is available
						void* header_page = malloc(PF_PAGE_SIZE);
						fileHandle.ReadPage(0, header_page);
						short total;
						GetTotalEntries(header_page, total);
						if (total > 0) {
							readEmptyPageFromHeader(header_page, overflow_pagenumber, total);
							writeEmptyPageToHeader(header_page, 0, total);
							WriteTotalEntries(header_page, total - 1);
							fileHandle.WritePage(0, header_page);
						} else
							overflow_pagenumber = (short) fileHandle.GetNumberOfPages();

						WriteNeighbor(overflow_page, neighbor);
						WriteNeighbor(buffer, overflow_pagenumber);
						fileHandle.WritePage(pageNumber, buffer);
						fileHandle.WritePage(overflow_pagenumber, overflow_page);

						RC rc = insertEntry(overflow_pagenumber, key, rid, return_Entry, check);

						free(buffer);
						free(minKey);
						free(maxKey);

						free(header_page);
						free(overflow_page);
						if (rc == RC_FAIL) {
							return rc;
						}
						return RC_SUCCESS;
					}

					else {
						RC rc = insertEntry(neighbor, key, rid, return_Entry, check);
						free(buffer);
						free(minKey);
						free(maxKey);

						free(overflow_page);
						if (rc == RC_FAIL) {
							return rc;
						}
						return RC_SUCCESS;
					}
				}

				else {
					check = true;
					void *new_page = malloc(PF_PAGE_SIZE);
					CreateNewPage(new_page, LeafNode);
					// push KEY up!
					// tracing the link of overflowpages
					void *temp_page = malloc(PF_PAGE_SIZE);
					short next_page = 0;
					next_page = neighbor;
					bool end_of_link = false;
					NodeType temp_type;
					short previous_page = pageNumber;
					while (next_page > 0 && end_of_link == false) {
						fileHandle.ReadPage(next_page, temp_page);
						GetNodeType(temp_page, temp_type);
						if (temp_type == LeafNode) {
							end_of_link = true;
							break;
						}
						previous_page = next_page;
						GetNeighbor(temp_page, next_page);
					}

					short newleaf_pagenumber = 0;
					//check HeaderPage to see if any old empty page is available
					void* header_page = malloc(PF_PAGE_SIZE);
					fileHandle.ReadPage(0, header_page);
					short total;
					GetTotalEntries(header_page, total);
					if (total > 0) {
						readEmptyPageFromHeader(header_page, newleaf_pagenumber, total);
						writeEmptyPageToHeader(header_page, 0, total);
						WriteTotalEntries(header_page, total - 1);
						fileHandle.WritePage(0, header_page);
					} else
						newleaf_pagenumber = (short) fileHandle.GetNumberOfPages();

					WriteNeighbor(new_page, next_page);
					fileHandle.ReadPage(previous_page, temp_page);
					WriteNeighbor(temp_page, newleaf_pagenumber);
					fileHandle.WritePage(previous_page, temp_page);
					fileHandle.WritePage(newleaf_pagenumber, new_page);

					RC rc = insertEntry(newleaf_pagenumber, key, rid, return_Entry, check);
					return_Entry.key = malloc(KEY_SIZE);
					memcpy(return_Entry.key, maxKey, KEY_SIZE);
					return_Entry.page = newleaf_pagenumber;
					free(buffer);
					free(minKey);
					free(maxKey);
					free(temp_page);
					free(new_page);
					free(header_page);

					if (rc == RC_FAIL
					)
						return rc;
					return RC_SUCCESS;
				}
			}

		}

	} //end of LEAF_NODE

	else if (node_type == NonLeafNode) {
		vector<NONLEAF_ENTRY> nonleaf_entries;
		GetNonLeafEntries(buffer, nonleaf_entries);
		if (DEBUG == true) {
			cout << "Reading nonleaf Page: " << pageNumber << endl;
			NONLEAF_ENTRY entry;
			for (unsigned i = 0; i < nonleaf_entries.size(); i++) {
				entry = nonleaf_entries[i];
				printNonLeafEntry(entry, keyType);
			}
		}
		//compare key with page entries
		unsigned index = 0;
		while (index < nonleaf_entries.size()) {
			if (isLess(nonleaf_entries[index].key, key, keyType) == RC_SUCCESS
			)
				index++;
			else
				break;

		}

		short nextPage = 0;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonleaf_entries[index - 1].page;
		}
		RC rc = insertEntry(nextPage, key, rid, return_Entry, check);
		if (rc == RC_FAIL) {
			return rc;
		}
		if (check == true) { //have Return_entry

			nonleaf_entries.push_back(return_Entry);
			if (nonleaf_entries.size() > 1) {
				if (keyType == TypeInt)
					sort(nonleaf_entries.begin(), nonleaf_entries.end(), nonLeafCompareInt);
				else if (keyType == TypeReal)
					sort(nonleaf_entries.begin(), nonleaf_entries.end(), nonLeafCompareReal);
			}
			if (freeSpace >= 6) {
				writeNonLeafPage(pageNumber, nonleaf_entries);
				free(buffer);
				//free(return_Entry.key);
				for (unsigned i = 0; i < nonleaf_entries.size(); i++) {
					free(nonleaf_entries[i].key);
				}
				check = false; // no return_entry
				return RC_SUCCESS;
			} else {

				short popUpEntry = (short) (nonleaf_entries.size() / 2);

				//split to 2 pages, neu co tach thi o day se cap nhat return = popEntry
				vector<NONLEAF_ENTRY> subEntriesA, subEntriesB;
				NONLEAF_ENTRY copyNode;

				for (int i = 0; i < (int) popUpEntry; i++) {
					copyNode = nonleaf_entries[i];
					subEntriesA.push_back(copyNode);
				}
				for (int i = popUpEntry + 1; i < (int) nonleaf_entries.size(); i++) {
					copyNode = nonleaf_entries[i];
					subEntriesB.push_back(copyNode);
				}

				short newPage = (short) (fileHandle.GetNumberOfPages());
				// write back to old leaf page
				writeNonLeafPage(newPage, subEntriesB);
				writeNonLeafPage(pageNumber, subEntriesA);

				//cap nhat return_Entry, popEntry se tro toi newPage, subEntriesB
				return_Entry.key = malloc(KEY_SIZE);
				memcpy(return_Entry.key, nonleaf_entries[popUpEntry].key, KEY_SIZE);
				return_Entry.page = newPage;
				check = true;
				free(buffer);

				for (unsigned i = 0; i < nonleaf_entries.size(); i++) {
					free(nonleaf_entries[i].key);
				}
				return RC_SUCCESS;
			}
		}
		//not split
		return RC_SUCCESS;
	} // end of NONLEAF_NODE

	return RC_FAIL;

}

