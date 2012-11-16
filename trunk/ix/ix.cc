#include "ix.h"
#include <algorithm>

using namespace std;

bool leafCompareInt(LEAF_ENTRY leaf1, LEAF_ENTRY leaf2) {

	return (*(int*) leaf1.key < *(int*) leaf2.key);

}

bool leafCompareReal(LEAF_ENTRY leaf1, LEAF_ENTRY leaf2) {

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

void CreateNewPage(void *buffer, NodeType type) {
	memset(buffer, 0, PF_PAGE_SIZE);
	WriteNodeType(buffer, type);
	if (type == LeafNode) {
		WritePtrSize(buffer, 4);
	} else { // non-leaf node has ptr of size 2: page number
		WritePtrSize(buffer, 2);
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
	return_entry.key = malloc(KEY_SIZE);
	bool has_return;
// this got to return RC_FAIL if there is already an entry for (key, rid)
	insertEntry(root, key, rid, return_entry, has_return);

	if (has_return) {
		short old_root = root;
		// read vector in page root
		void *buffer = malloc(PF_PAGE_SIZE);
		fileHandle.ReadPage(root, buffer);

		short freeSpace = 0;

		GetFreeSpace(buffer, freeSpace);

		vector<NONLEAF_ENTRY> nonleaf_entries;
		GetNonLeafEntries(buffer, nonleaf_entries);
		// insert return entry

		nonleaf_entries.push_back(return_entry);
		if (nonleaf_entries.size() > 1) {
			if (keyType == TypeInt)
				sort(nonleaf_entries.begin(), nonleaf_entries.end(), nonLeafCompareInt);
			else if (keyType == TypeReal)
				sort(nonleaf_entries.begin(), nonleaf_entries.end(), nonLeafCompareReal);
		}
		if (freeSpace >= 6) {
			writeNonLeafPage(root, nonleaf_entries);
			free(buffer);
			for (unsigned i = 0; i < nonleaf_entries.size(); i++)
				free(nonleaf_entries[i].key);
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
			for (int i = popUpEntry + 1; i <= nonleaf_entries.size(); i++) {
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

			CreateNewPage(buffer, NonLeafNode);
			WriteStartingPage(buffer, old_root);
			memcpy((char*) buffer + unit,nonleaf_entries[popUpEntry].key,KEY_SIZE);
			memcpy((char*) buffer + unit + KEY_SIZE, &newPage,unit);
			WriteFreeSpace(buffer, PF_PAGE_SIZE - 16);
			fileHandle.WritePage(root, buffer);

			free(buffer);
			free(header_page);
			for (unsigned i = 0; i < nonleaf_entries.size(); i++)
				free(nonleaf_entries[i].key);

			return RC_SUCCESS;

		}
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
}

RC IX_IndexHandle::getMostLeftLeaf(short page, short &leftMostPage){
	void * buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(page, buffer);

	NodeType node_type;

	GetNodeType(buffer, node_type);

	if (node_type == NonLeafNode) {
		short nextPage = 0;
		GetStartingPage(buffer, nextPage);

		free(buffer);
		return getMostLeftLeaf(nextPage, leftMostPage);
	}
	else
	{ // leaf node has been return
		leftMostPage = page;
		return RC_SUCCESS;
	}
	return RC_FAIL;
}

RC CalOffsetOfEmptyPage(short index, short &offset)
{
	offset = PF_PAGE_SIZE - 2*unit - unit*index;
}
//
void writeEmptyPageToHeader(void * buffer, short pageNumber, short index)
{
	short offset;

	CalOffsetOfEmptyPage(index, offset);

	memcpy((char*)buffer + offset, &pageNumber, unit);

}

RC readEmptyPageFromHeader(void * buffer, short &pageNumber, short index) {
	short offset;

	CalOffsetOfEmptyPage(index, offset);

	memcpy(&pageNumber, (char*)buffer + offset, unit);

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
	if (node_type == LeafNode) {
		vector<LEAF_ENTRY> leaf_entries;
		GetLeafEntries(buffer, leaf_entries);
		GetNeighbor(buffer, neighbor);

		unsigned index = 0, firstIndex = 0, lastIndex = 0;

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
				CalcPtrOffset(index, LeafNode, ptr_size, offset);
				memcpy(&val, (char*) buffer + offset, unit);
				if ((PageNum) val == rid.pageNum) {
					memcpy(&val, (char*) buffer + offset + unit,unit);
					if ((PageNum) val == rid.slotNum) {
						//delete leafentries[index]
						free(leaf_entries[index].key);
						leaf_entries.erase(leaf_entries.begin() + index);
						if (leaf_entries.size() >= 1) {
							writeLeafPage((PageNum) pageNumber, leaf_entries, neighbor);
							free(buffer);
							return RC_SUCCESS;
						} else { // empty page
							// get left most page
							short leftMostPage = 0;
							getMostLeftLeaf(root, leftMostPage);

							if (leftMostPage == pageNumber) {
								//check = true;
								//chi viec cap nhat no trong header page

							} else {
								// traverse to find the page before this page
								short temp_Neighbor = 0, previousPage = 0; //tim trang truoc pageNumber
								temp_Neighbor = leftMostPage; //chinh bang no
								//previousPage = leftMostPage;
								void *temp = malloc(PF_PAGE_SIZE);
								while (temp_Neighbor != pageNumber) {
									previousPage = temp_Neighbor;

									fileHandle.ReadPage(previousPage, temp);
									GetNeighbor(temp, temp_Neighbor);

								}


									// update that newly found page's neighbor
									fileHandle.ReadPage(previousPage, temp);
									WriteNeighbor(temp, neighbor);
									fileHandle.WritePage(previousPage, temp);
									memset(buffer, 0, PF_PAGE_SIZE);
									fileHandle.WritePage(pageNumber, buffer);
									free(temp);
							}



						void * header_page = malloc(PF_PAGE_SIZE);
													fileHandle.ReadPage(0,header_page);
													short N;
													GetTotalEntries(header_page,N);
													N++;
													writeEmptyPageToHeader(header_page,pageNumber, N);
													WriteTotalEntries(header_page,N);
													fileHandle.WritePage(0,header_page);
													free(header_page);
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
			return RC_FAIL;
		} else
			return RC_SUCCESS;

	}

	else //Ko phai leafNode
	{

		vector<NONLEAF_ENTRY> nonleaf_entries;
		GetNonLeafEntries(buffer, nonleaf_entries);
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
		deleteEntry(key, rid, nextPage, check);

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

			if (nonleaf_entries.size() == 0 && erased)
				check = true;
			writeNonLeafPage(pageNumber, nonleaf_entries);
			return RC_SUCCESS;

		} else
			return RC_SUCCESS;

	}
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) { // Delete index entry
	bool change = false;
	deleteEntry(key, rid, root, change);
	//change root???
	return RC_SUCCESS;
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
	}
	this->handle = indexHandle.fileHandle;
	this->root = indexHandle.root;
	this->keyType = indexHandle.keyType;
	if (compOp == NE_OP)
		return RC_FAIL;

	return RC_SUCCESS;
}

RC IX_IndexScan::compare(void *entry) {
	if (operation != NO_OP && cond_value == NULL
	)
		return RC_FAIL;
	switch (keyType) {
	case TypeInt:
		int int1;
		memcpy(&int1, (char*) entry, KEY_SIZE);
		int int2;
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

		}
		break;

	case TypeReal:
		float real1;
		memcpy(&real1, (char*) entry, KEY_SIZE);
		float real2;
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
	}
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

		if (keyType == TypeInt) {
			while (index < nonLeaf_entries.size()) {
				if (*(int*) nonLeaf_entries[index].key < *(int*) cond_value)
					index++;
				else
					break;
			}
		} else if (keyType == TypeReal) {
			while (index < nonLeaf_entries.size()) {
				if (*(float*) nonLeaf_entries[index].key < *(float*) cond_value)
					index++;
				else
					break;
			}
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
			currentIndex = index;
			free(buffer);
			return RC_SUCCESS;
		} else {
			short nextPage = 0;
			short totals = 0;
			do {
				GetNeighbor(buffer, nextPage);
				handle.ReadPage((PageNum) nextPage, buffer);
				GetTotalEntries(buffer, totals);
			} while (totals != 0 && nextPage != 0);
			free(buffer);
			return findKey((PageNum) nextPage);
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

			short nextPage = 0;
			GetNeighbor(buffer, nextPage);
			if (nextPage == 0) {
				free(buffer);
				return RC_FAIL;
			} else {
				free(buffer);
				return GetLeftMost((PageNum) nextPage);
			}
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
		if (rc == RC_SUCCESS
		)
			hasStartingPoint = true;
	}

	if (hasStartingPoint == false)
		return IX_EOF;

	void *buffer = malloc(PF_PAGE_SIZE);
	bool pass = false;
	while (pass == false) {
		handle.ReadPage(currentPage, buffer); // that page must be a LEAF
		short totals = 0;
		GetTotalEntries(buffer, totals);
		if (currentIndex > totals) {
			GetNeighbor(buffer, currentPage);
			currentIndex = 1;
		} else
			pass = true;
	}

	short offset = 0;
	char ptr_size = 0;
	GetPtrSize(buffer, ptr_size);
	CalcKeyOffset(currentIndex, LeafNode, ptr_size, offset);
	void *key_value = malloc(KEY_SIZE);
	memcpy(key_value, (char*) buffer + offset, KEY_SIZE);
	if (compare(key_value) == RC_SUCCESS) {
		short val;
		CalcPtrOffset(currentIndex, LeafNode, ptr_size, offset);
		memcpy(&val, (char*) buffer + offset, unit);
		rid.pageNum = (unsigned) val;
		memcpy(&val, (char*) buffer + offset + unit,unit);
		rid.slotNum = (unsigned) val;
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
}

// print out the error message for a given return code
void IX_PrintError(RC rc) {

}

void printLeafEntry(LEAF_ENTRY &leaf, AttrType key_type) {
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
	/*
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
	 */
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
//free(data);
	return RC_SUCCESS;
}

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
		entry.key = malloc(KEY_SIZE);
		memcpy(entry.key, (char*) buffer + offset, KEY_SIZE);
		CalcPtrOffset(i, NonLeafNode, ptr_size, offset);
		memcpy(&entry.page, (char*) buffer + offset, unit);
		nonleaf_entries.push_back(entry);
	}

//sort(nonleaf_entries.begin(), nonleaf_entries.end(), middleCompare);
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
	if (pageNumber == fileHandle.GetNumberOfPages()) { // never true
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

	short freeSpace, neighbor;
	NodeType node_type;

	GetFreeSpace(buffer, freeSpace);
	GetNodeType(buffer, node_type);
	GetNeighbor(buffer, neighbor);

	if (node_type == LeafNode) {
		vector<LEAF_ENTRY> leaf_entries;
		LEAF_ENTRY leaf;
		leaf.key = malloc(KEY_SIZE);
		memcpy(leaf.key, key, KEY_SIZE);
		leaf.page = (short) rid.pageNum;
		leaf.slot = (short) rid.slotNum;
		printLeafEntry(leaf, this->keyType);
		GetLeafEntries(buffer, leaf_entries);

		leaf_entries.push_back(leaf);
		if (leaf_entries.size() > 1) {
			if (keyType == TypeInt)
				sort(leaf_entries.begin(), leaf_entries.end(), leafCompareInt);
			else if (keyType == TypeReal)
				sort(leaf_entries.begin(), leaf_entries.end(), leafCompareReal);
		}

// print
		LEAF_ENTRY entry;
		for (unsigned i = 0; i < leaf_entries.size(); i++) {
			entry = leaf_entries[i];
			printLeafEntry(entry, keyType);
		}

		if (freeSpace >= 8) {
			writeLeafPage(pageNumber, leaf_entries, neighbor);
			free(buffer);
			check = false; // no Return_Entry

			for (unsigned i = 0; i < leaf_entries.size(); i++) {
				free(leaf_entries[i].key);
			}
			return RC_SUCCESS;
		} else {
			check = true; // has Return_Entry

			//push new entry first, after that divide page to 2 pages

			unsigned N = leaf_entries.size();
			unsigned position = 0;
			short direction = 0;
			if (this->keyType == TypeInt) {
				int minKey, maxKey, midKey;

				memcpy(&minKey, leaf_entries[0].key, KEY_SIZE);
				memcpy(&maxKey, leaf_entries[N - 1].key, KEY_SIZE);
				memcpy(&midKey, leaf_entries[N / 2].key, KEY_SIZE);
				if (minKey == maxKey) {
					cerr << "Leaf Node full of same-key entries - Illegal" << endl;
					assert(0==1);
				} else {
					if (midKey == maxKey) {
						direction = -1;
					} else {
						direction = 1;
					}

					position = N / 2 + direction;
					int next = 0;
					while (position >= 0 && position < N) {
						memcpy(&next, leaf_entries[position].key, KEY_SIZE);
						if (next != midKey) {
							break;
						} else {
							position += direction;
						}

					}
				}
			} else if (this->keyType == TypeReal) {
				float minKey, maxKey, midKey;

				memcpy(&minKey, leaf_entries[0].key, KEY_SIZE);
				memcpy(&maxKey, leaf_entries[N - 1].key, KEY_SIZE);
				memcpy(&midKey, leaf_entries[N / 2].key, KEY_SIZE);
				if (minKey == maxKey) {
					cerr << "Leaf Node full of same-key entries - Illegal" << endl;
					assert(0==1);
				} else {
					if (midKey == maxKey) {
						direction = -1;
					} else {
						direction = 1;
					}

					position = N / 2 + direction;
					float next = 0;
					while (position >= 0 && position < N) {
						memcpy(&next, leaf_entries[position].key, KEY_SIZE);
						if (next != midKey) {
							break;
						} else {
							position += direction;
						}

					}
				}
			}

			if (direction == 1)
				position--;
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

			short newPage = (short) fileHandle.GetNumberOfPages();
			// write back to old leaf page
			writeLeafPage(newPage, subEntriesB, neighbor);

			writeLeafPage(pageNumber, subEntriesA, newPage);

			memcpy(return_Entry.key, leaf_entries[copyUpEntry].key, KEY_SIZE);
			return_Entry.page = newPage;
			free(buffer);

			for (unsigned i = 0; i < leaf_entries.size(); i++) {
				free(leaf_entries[i].key);
			}
			return RC_SUCCESS;
		}

	} //end of LEAF_NODE

	else {
		vector<NONLEAF_ENTRY> nonleaf_entries;

		GetNonLeafEntries(buffer, nonleaf_entries);

//compare key with page entries
		unsigned index = 0;

		if (keyType == TypeInt) {
			while (index < nonleaf_entries.size()) {

				if (*(int*) nonleaf_entries[index].key < *(int*) key)
					index++;
				else
					break;

			}
		} else if (keyType == TypeReal) {
			while (index < nonleaf_entries.size()) {

				if (*(float*) nonleaf_entries[index].key < *(float*) key)
					index++;
				else
					break;

			}
		}

		short nextPage;
		if (index == 0) {
			GetStartingPage(buffer, nextPage);
		} else {
			nextPage = nonleaf_entries[index - 1].page;
		}
		insertEntry(nextPage, key, rid, return_Entry, check);

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
				free(return_Entry.key);
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
				for (int i = popUpEntry + 1; i < nonleaf_entries.size(); i++) {
					copyNode = nonleaf_entries[i];
					subEntriesB.push_back(copyNode);
				}

				short newPage = (short) (fileHandle.GetNumberOfPages());
				// write back to old leaf page
				writeNonLeafPage(newPage, subEntriesB);
				writeNonLeafPage(pageNumber, subEntriesA);

				//cap nhat return_Entry, popEntry se tro toi newPage, subEntriesB
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
