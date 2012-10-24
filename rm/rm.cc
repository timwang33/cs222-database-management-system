#include "rm.h"
#include<algorithm>

using namespace std;
RM* RM::_rm = 0;

RM* RM::Instance() {
	if (!_rm)
		_rm = new RM();

	return _rm;
}

//Yen

/*void prepareColumnTuple(unsigned schema, const string name,
 const string columnName, AttrType type, AttrLength length,
 unsigned position, void *buffer, short *tuple_size) {
 short offset = 0;

 memcpy((char*) buffer + offset, &schema, sizeof(unsigned));
 offset += sizeof(unsigned);

 int string_length = name.length();
 memcpy((char *) buffer + offset, &string_length, sizeof(int));
 offset += sizeof(int);

 memcpy((char *) buffer + offset, name.c_str(), string_length);
 offset += string_length;

 string_length = columnName.length();
 memcpy((char *) buffer + offset, &string_length, sizeof(int));
 offset += sizeof(int);

 memcpy((char *) buffer + offset, columnName.c_str(), string_length);
 offset += string_length;

 memcpy((char*) buffer + offset, &type, sizeof(AttrType));
 offset += sizeof(AttrType);

 memcpy((char*) buffer + offset, &length, sizeof(AttrLength));
 offset += sizeof(AttrLength);

 memcpy((char*) buffer + offset, &position, sizeof(unsigned));
 offset += sizeof(unsigned);

 *tuple_size = offset;
 }*/

void prepareColumnTuple(unsigned schema, const string name, Attribute attr,
		unsigned position, void *buffer, short *tuple_size) {
	short offset = 0;

	memcpy((char*) buffer + offset, &schema, sizeof(unsigned));
	offset += sizeof(unsigned);

	int string_length = attr.name.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, attr.name.c_str(), string_length);
	offset += string_length;

	string_length = name.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, name.c_str(), string_length);
	offset += string_length;

	memcpy((char*) buffer + offset, &(attr.type), sizeof(AttrType));
	offset += sizeof(AttrType);

	memcpy((char*) buffer + offset, &(attr.length), sizeof(AttrLength));
	offset += sizeof(AttrLength);

	memcpy((char*) buffer + offset, &position, sizeof(unsigned));
	offset += sizeof(unsigned);

	*tuple_size = offset;
}

//buffer contain only column name, type, length and position
//RC getColumnTuple(void *buffer,short length, unsigned *schema, string tableName, Attribute &attr, unsigned *position)
RC getColumnTuple(unsigned *schema, const string tableName, Attribute *attr,
		unsigned *position, void *buffer, short lengthTuple) {
	short offset = 0;
	string columnName;
	AttrType columnType;
	AttrLength columnLength;
	string table_Name;

	memcpy(schema, (char*) buffer, sizeof(unsigned));
	offset += sizeof(unsigned);

	int string_length;
	memcpy(&string_length, (char*) buffer + offset, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) table_Name.c_str(), (char*) buffer + offset, string_length);
	offset += string_length;

	//check if this column belong to tableName table

	if (!strcmp(table_Name.c_str(), tableName.c_str()))
		return 1;
	else {
		memcpy(&string_length, (char*) buffer + offset, sizeof(int));
		offset += sizeof(int);

		//get columnName
		memcpy((char*) columnName.c_str(), (char*) buffer + offset,
				string_length);
		offset += string_length;
		(*attr).name = columnName;

		//get column type
		memcpy(&columnType, (char*) buffer + offset, sizeof(AttrType));
		offset += sizeof(AttrType);
		(*attr).type = columnType;

		//get column length
		memcpy(&columnLength, (char*) buffer + offset, sizeof(AttrLength));
		offset += sizeof(AttrLength);
		(*attr).length = columnLength;

		//get position
		memcpy(&position, (char*) buffer + offset, sizeof(unsigned));
		offset += sizeof(unsigned);

		//check if read buffer correctly
		if (offset == lengthTuple)
			return 0;
	}
	return -1;
}

//Hien tai van chua su dung schema khi getAttribute, moi chi dua tren tableName
RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {

	//buffer contain page i starts from page 1
	//offset of directory
	int offset = PF_PAGE_SIZE;
	short N, length, offsetColumnTuple, freeSpace;
	RC rc = -1;
	int lastTuple;
	Attribute attr;
	unsigned schema_Column = 0, old_Schema_Column, position_Column = 0;

	void *buffer;
	buffer = malloc(PF_PAGE_SIZE);
	fileManager->OpenFile(column_file_name, columnHandle);
	PageNum pageNum = columnHandle.GetNumberOfPages();

	//traverse bottom-up
	for (unsigned i = pageNum - 1; i > 0; i--) {

		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i
		memcpy(&N, (char*) buffer + offset - sizeof(short), sizeof(short));
		offset = offset - sizeof(short);

		//freeSpace of page i
		memcpy(&freeSpace, (char*) buffer + offset - sizeof(short),
				sizeof(short));
		offset = offset - sizeof(short);

		//get last tuple of table Name from page i to have initial schema, it may not be N tuple
		while (rc != RC_SUCCESS) {
			lastTuple = N;
			offset = offset - 4 * (lastTuple - 1);

			memcpy(&length, (char*) buffer + offset - sizeof(short),
					sizeof(short));
			offset = offset - sizeof(short);

			//read offset of tuple run
			memcpy(&offsetColumnTuple, (char*) buffer + offset - sizeof(short),
					sizeof(short));
			offset = offset - sizeof(short);

			//read tuple column N
			void * data;
			data = malloc(100);
			memcpy(data, (char*) buffer + offsetColumnTuple, length);

			//data contains sequences byte of attribute
			rc = getColumnTuple(&schema_Column, tableName, &attr,
					&position_Column, data, length);
			attrs.push_back(attr);
			// update Schema
			old_Schema_Column = schema_Column;
		}
		rc = -1;
		int j = lastTuple;
		while ((old_Schema_Column == schema_Column) & (rc != RC_SUCCESS)) {
			j = j - 1;
			offset = offset - 4 * (j - 1);

			memcpy(&length, (char*) buffer + offset - sizeof(short),
					sizeof(short));
			offset = offset - sizeof(short);

			//read offset of tuple j
			memcpy(&offsetColumnTuple, (char*) buffer + offset - sizeof(short),
					sizeof(short));
			offset = offset - sizeof(short);

			//read a tuple column
			void * data;
			data = malloc(100);
			memcpy(data, (char*) buffer + offsetColumnTuple, length);

			//data contains sequences byte of attribute
			getColumnTuple(&schema_Column, tableName, &attr, &position_Column,
					data, length);
			attrs.push_back(attr);
		}
		//next schema or next table             //inverse attrs and return
		reverse(attrs.begin(), attrs.end());
		return 0;
	}
	return -1;
}

void prepareCatalogTuple(const string name, const string filename, void *buffer,
		short *tuple_size) {
	short offset = 0;

	int string_length = name.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, name.c_str(), string_length);
	offset += string_length;

	string_length = filename.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, filename.c_str(), string_length);
	offset += string_length;

	*tuple_size = offset;
}

void getTotalEntries(void * buffer, int* totalEntries) {
	memcpy(totalEntries, (char *) buffer + END_OF_PAGE - unit,unit);

}

void writeTotalEntries(void * buffer, int* totalEntries) {
	memcpy((char*) buffer + END_OF_PAGE - unit, totalEntries,unit);
}

void getFreeSpace(void * buffer, int* freeSpace) {
	memcpy(freeSpace, (char *) buffer + END_OF_PAGE - 2 * unit,unit);
}

void writeFreeSpace(void * buffer, int* freeSpace) {
	memcpy((char*) buffer + END_OF_PAGE - 2 * unit, freeSpace,unit);
}

void writeSpecificOffset(void * buffer, int position, int * value) {
	int offset = (position << 2) + unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, value, unit);
}

void writeSpecificLength(void * buffer, int position, int* value) {
	int offset = (position << 2) + 2 * unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, value, unit);
}

void getSpecificOffset(void * buffer, int position, int * result) {
	int offset = (position << 2) + unit;

	memcpy(result, (char*) buffer + END_OF_PAGE - offset, unit);

}

void getSpecificLength(void * buffer, int position, int * result) {
	int offset = (position << 2) + 2 * unit;

	memcpy(result, (char*) buffer + END_OF_PAGE - offset, unit);
}

void writeNewestOffset(void * buffer) {
	int position;
	getTotalEntries(buffer, &position);
	int value;

	int lastOffset;
	int lastLength;
	getSpecificOffset(buffer, position, &lastOffset);
	getSpecificLength(buffer, position, &lastLength);
	value = lastOffset + lastLength;

	writeSpecificOffset(buffer, position + 1, &value);

}

void writeNewestLength(void * buffer, int* value) {
	int position;
	getTotalEntries(buffer, &position);
	int offset = ((position + 1) << 2) + 2 * unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, value, unit);
}

void getLastOffset(void * buffer, int * result) {
	int position;
	getTotalEntries(buffer, &position);
	if (position == 0) {
		*result = 0;
		return;
	}
	int offset = (position << 2) + unit;

	memcpy(result, (char*) buffer + END_OF_PAGE - offset, unit);

}

void getLastLength(void * buffer, int * result) {
	int position;
	getTotalEntries(buffer, &position);
	if (position == 0) {
		*result = 0;
		return;
	}

	int offset = (position << 2) + 2 * unit;

	memcpy(result, (char*) buffer + END_OF_PAGE - offset, unit);
}

void writeTo(void * dest, void* source, int source_len) {
	int lastOffset;
	int lastLength;
	getLastOffset(dest, &lastOffset);
	getLastLength(dest, &lastLength);
	int offset = lastOffset + lastLength;

	memcpy((char*) dest + offset, source, source_len);

	//update directory:
	int total;
	getTotalEntries(dest, &total);
	total++;
	writeSpecificOffset(dest, total, &offset);
	writeSpecificLength(dest, total, &source_len);
	writeTotalEntries(dest, &total);
	int free_space;
	getFreeSpace(dest, &free_space);
	free_space = free_space - source_len - 4;
	writeFreeSpace(dest, &free_space);
}

void writeTo(void* ptr, int offset, void* tuple, short *tuple_size) {
	memcpy((char *) ptr + offset, tuple, *tuple_size);
}
/*void writeTo(void* ptr, int offset, positive_twobytes value) {
 memcpy ((char *) ptr +offset, &value, sizeof(short));
 }*/

void readFrom(void* ptr, int offset, positive_twobytes value) {
	memcpy(&value, (char *) ptr + offset, sizeof(short));
}

void readFrom(void* ptr, int offset, void* value) {
	memcpy(value, (char *) ptr + offset, sizeof(short));
}

void writeFreeSpaceInHeaderPage(void * buffer, int index, int* value) {
	int offset = index << 1;

	memcpy((char*) buffer + offset, value, unit);
}

void getFreeSpaceInHeaderPage(void * buffer, int index, int * value) {
	int offset = index << 1;

	memcpy(value, (char*) buffer + offset, unit);
}



RC RM::openTable(const string tableName, PF_FileHandle &tableHdl) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())) {
			return RC_SUCCESS;
		}
	}

	fileManager->OpenFile(tableName.c_str(), tableHdl);

	tableHandle newTable;
	newTable.name = tableName;
	newTable.fileHandle = tableHdl;
	allTables.push_back(newTable);

	return RC_SUCCESS;
}


RC RM::openTable(const string tableName) {
	PF_FileHandle tableHdl;
	return openTable(tableName, tableHdl);
}

RC RM::getTableHandle(const string tableName, PF_FileHandle &handle) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())) {
			handle = allTables[i].fileHandle;
			return RC_SUCCESS;
		}
	}
	return RC_FAIL;

}
RC RM::closeTable(const string tableName) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())) {
			fileManager->CloseFile(allTables[i].fileHandle);
			allTables.erase(allTables.begin() + i);
			return RC_SUCCESS;
		}
	}
	return RC_FAIL;
}

RM::RM() {

	fileManager = PF_Manager::Instance();
	remove(catalog_file_name);
	remove(column_file_name);
	//open catalog files
	struct stat stFileInfo;

	if (stat(catalog_file_name, &stFileInfo) != 0) { //catalog not exist
		fileManager->CreateFile(catalog_file_name);
		fileManager->OpenFile(catalog_file_name, catalogHandle);

		short tuple_size = 0;
		void *tuple = malloc(100);

		prepareCatalogTuple("Catalog", catalog_file_name, tuple, &tuple_size);

		void * buffer = malloc(PF_PAGE_SIZE);
		int uno = 0;
		int free_space = PF_PAGE_SIZE - 4;
		writeTotalEntries(buffer, &uno);
		writeFreeSpace(buffer, &free_space);

		/*short offsetFreeSpace = 0;
		 short total_entry = 0;
		 short length;
		 short offsetTuple;
		 total_entry = 1;
		 length = tuple_size;
		 offset*/
		//writeTo(buffer,tuple,tuple_size);
		//short offset = 0;
		//memcpy((char *) buffer, tuple, tuple_size);
		//int offset = PF_PAGE_SIZE;
		//offset = offset + tuple_size;
		//writeTo(buffer, offset - sizeof(short) - sizeof(int),(positive_twobytes)0);
		//writeTo(buffer, offset - sizeof(int) - sizeof(int),tuple_size);
		//writeTo(buffer, 0, tuple);
		prepareCatalogTuple("Column", column_file_name, tuple, &tuple_size);
		//free_space = free_space - tuple_size;
		writeTo(buffer, tuple, tuple_size);
		//offset = offset + tuple_size;
		//cout << " offset is " << offset << endl;

		//readFrom(buffer,offset-sizeof(int) - sizeof(int), &offset);

		//cout<< "offset: "<<offset <<endl;
		//writeTo(buffer,offset,tuple);

		/*writeTo(buffer, offset - sizeof(short) - sizeof(int),(positive_twobytes)0);
		 writeTo(buffer, offset - sizeof(int) - sizeof(int),tuple_size);
		 unsigned short total_entry = 2;

		 cout <<" total entry " << total_entry << endl;

		 memcpy((char *) buffer + PF_PAGE_SIZE - sizeof(short), &total_entry,
		 sizeof(short));

		 unsigned short num = 0;

		 memcpy((char *) buffer + PF_PAGE_SIZE - sizeof(short) - sizeof(short),
		 &free_space, sizeof(short));
		 memcpy(
		 (char *) buffer + PF_PAGE_SIZE - sizeof(short) - sizeof(short)
		 - sizeof(short), &num, sizeof(short));

		 memcpy(
		 (char *) buffer + PF_PAGE_SIZE - sizeof(short) - sizeof(short)
		 - sizeof(short) - sizeof(short), &tuple_size,
		 sizeof(short));
		 */

		catalogHandle.WritePage(0, buffer);

		fileManager->CloseFile(catalogHandle);
		free(tuple);
		free(buffer);
	}

	fileManager->OpenFile(catalog_file_name, catalogHandle);

	if (stat(column_file_name, &stFileInfo) != 0) { //column not exist
		fileManager->CreateFile(column_file_name);
		fileManager->OpenFile(column_file_name, columnHandle);

		void * buffer = malloc(PF_PAGE_SIZE);
		void *data = malloc(100);
		short tuple_size;
		//short offset = 0;

		int initFree = PF_PAGE_SIZE - unit;
		writeFreeSpaceInHeaderPage(buffer, 0, &initFree);
		columnHandle.WritePage(0, buffer);

		int uno = 0;
		int free_space = PF_PAGE_SIZE - 4;
		writeTotalEntries(buffer, &uno);
		writeFreeSpace(buffer, &free_space);

		Attribute attr;

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		prepareColumnTuple(1, "Catalog", attr, 1, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;
		catalogAttrs.push_back(attr);

		attr.name = "FileName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 50;
		catalogAttrs.push_back(attr);
		prepareColumnTuple(1, "Catalog", attr, 2, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "Schema";
		attr.type = TypeInt;
		attr.length = 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 1, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "ColName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 20;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 2, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 3, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "Type";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 4, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "Length";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 5, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "Position";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 6, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		getFreeSpace(buffer, &free_space);

		columnHandle.WritePage(1, buffer);

		columnHandle.ReadPage(0, buffer);
		writeFreeSpaceInHeaderPage(buffer, 1, &free_space);
		columnHandle.WritePage(0, buffer);
		/*short freespace = PF_PAGE_SIZE - offset;
		 cout<< " free space is: "<< freespace <<endl;*/
		fileManager->CloseFile(columnHandle);
		free(buffer);
		free(data);
	}
	fileManager->OpenFile(column_file_name, columnHandle);

	//Test getAttributes
	vector<Attribute> attrs;
	RC rc = getAttributes("Catalog", attrs);
	cout << " RC : " << rc << endl;

}
void printDataFromAttribute(void *data, Attribute attr) {
	int length_var_char;
	int int_value;
	float float_value;
	string string_value;

	if (attr.type == TypeInt) {
		memcpy(&int_value, (char*) data, 4);
		cout << int_value << endl;
	} else if (attr.type == TypeReal) {
		memcpy(&float_value, (char*) data, 4);
		cout << float_value << endl;
	} else {
		memcpy(&length_var_char, (char*) data, 4);
		memcpy((char*) string_value.c_str(), (char*) data, length_var_char);
		cout << string_value << endl;
	}
}

RM::~RM() {
	fileManager->CloseFile(columnHandle);
	fileManager->CloseFile(catalogHandle);
}

void createNewPage(void * buffer) {

	int uno = 0;
	memset(buffer, uno, PF_PAGE_SIZE);
	int free_space = PF_PAGE_SIZE - 4;
	writeFreeSpace(buffer, &free_space);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {

	void *data = malloc(100);
	void *catalog_buffer = malloc(PF_PAGE_SIZE);
	void *column_buffer = malloc(PF_PAGE_SIZE);
	string fileName = tableName + ".data";
	fileManager->CreateFile(fileName.c_str());

	openTable(tableName);

	short tuple_size;
	int freeSpace;
	PageNum currentPage = 0;
	catalogHandle.ReadPage(currentPage, catalog_buffer);
	prepareCatalogTuple(tableName, fileName, data, &tuple_size);
	bool pass = false;
	do {

		getFreeSpace(catalog_buffer, &freeSpace);
		if (freeSpace >= tuple_size + 4) { // enough space to write
			writeTo(catalog_buffer, data, tuple_size);
			pass = true;
		} else {
			//catalogHandle.WritePage(currentPage, catalog_buffer);
			currentPage++;
			catalogHandle.ReadPage(currentPage, catalog_buffer);
			if (currentPage == catalogHandle.GetNumberOfPages())
				createNewPage(catalog_buffer);
		}
	} while (!pass);
	catalogHandle.WritePage(currentPage, catalog_buffer);

	void *header_buffer = malloc(PF_PAGE_SIZE);
	columnHandle.ReadPage(0, header_buffer);
	PageNum N = columnHandle.GetNumberOfPages();

	currentPage = 1;
	bool dirty = false;

	for (int i = 0; i < (int) attrs.size(); i++) {

		prepareColumnTuple(1, tableName, attrs[i], i + 1, data, &tuple_size);
		pass = false;
		do {
			if (currentPage == N) {
				if (dirty) {
					columnHandle.WritePage(currentPage - 1, column_buffer);
				}
				createNewPage(column_buffer);
				freeSpace = PF_PAGE_SIZE - 4;
				writeFreeSpaceInHeaderPage(header_buffer, currentPage,
						&freeSpace);
			}
			getFreeSpaceInHeaderPage(header_buffer, currentPage, &freeSpace);
			if (freeSpace < tuple_size + 4) {
				if (dirty) {
					columnHandle.WritePage(currentPage, column_buffer);
				}
				currentPage++;

			} else {
				pass = true;
			}
		} while (!pass);
		freeSpace = freeSpace - tuple_size - 4;
		writeFreeSpaceInHeaderPage(header_buffer, currentPage, &freeSpace);
		columnHandle.ReadPage(currentPage, column_buffer);
		writeTo(column_buffer, data, tuple_size);
		dirty = true;
	}
	columnHandle.WritePage(0, header_buffer);
	columnHandle.WritePage(currentPage, column_buffer);

	free(data);
	free(catalog_buffer);
	free(column_buffer);
	free(header_buffer);
	return 0;
}

RC RM::deleteTable(const string tableName) {
	return 0;
}

//  Format of the data passed into the function is the following:
//  1) data is a concatenation of values of the attributes
//  2) For int and real: use 4 bytes to store the value;
//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
RC RM::insertTuple(const string tableName, const void *data, RID &rid) {

	return RC_SUCCESS;
}

RC RM::deleteTuples(const string tableName) {
	return 0;
}

RC RM::deleteTuple(const string tableName, const RID &rid) {
	return 0;
}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	return 0;
}

RC RM::readAttribute(const string tableName, const RID &rid,
		const string attributeName, void *data) {
	return 0;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
	return 0;
}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RM::scan(const string tableName, const string conditionAttribute,
		const CompOp compOp, // comparision type such as "<" and "="
		const void *value, // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RM_ScanIterator &rm_ScanIterator) {
	return 0;
}

// Extra credit

RC RM::dropAttribute(const string tableName, const string attributeName) {

	return 0;
}

RC RM::addAttribute(const string tableName, const Attribute attr) {
	return 0;
}

RC RM::reorganizeTable(const string tableName) {
	return 0;
}

RM_ScanIterator::RM_ScanIterator() {

}

RM_ScanIterator::~RM_ScanIterator() {

}

// "data" follows the same format as RM::insertTuple()

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return RM_EOF;
}

RC RM_ScanIterator::close() {
	return -1;
}

