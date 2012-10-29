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

void printAttribute(Attribute att) {
	cout << "NAME = " << att.name << " ";
	cout << "Type = " << att.type << " ";
	cout << "Length = " << att.length << endl;

}
void printAttributes(const vector<Attribute> &list) {
	for (unsigned i = 0; i < list.size(); i++)
		printAttribute(list[i]);

}

void prepareColumnTuple(unsigned schema, const string tableName, Attribute attr,
		unsigned position, void *buffer, short *tuple_size) {
	short offset = 0;

	memcpy((char*) buffer + offset, &schema, sizeof(unsigned));
	offset += sizeof(unsigned);
	int string_length;

	string_length = tableName.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, tableName.c_str(), string_length);
	offset += string_length;

	string_length = attr.name.length();
	memcpy((char *) buffer + offset, &string_length, sizeof(int));
	offset += sizeof(int);

	memcpy((char *) buffer + offset, attr.name.c_str(), string_length);
	offset += string_length;

	memcpy((char*) buffer + offset, &(attr.type), sizeof(AttrType));
	offset += sizeof(AttrType);

	memcpy((char*) buffer + offset, &(attr.length), sizeof(AttrLength));
	offset += sizeof(AttrLength);

	memcpy((char*) buffer + offset, &position, sizeof(unsigned));
	offset += sizeof(unsigned);

	*tuple_size = offset;
}

RC getCatalogTuple(string &tableName, string &fileName, void *buffer) {
	int string_length;
	short offset = 0;

	//get length of tableName
	memcpy(&string_length, (char*) buffer, sizeof(int));
	offset += sizeof(int);
	char* test1 = (char*) malloc(string_length);
	memcpy(test1, (char*) buffer + offset, string_length);
	test1[string_length] = '\0';
	tableName = test1;
	offset += string_length;

	//get length of fileName
	memcpy(&string_length, (char*) buffer + offset, sizeof(int));
	offset += sizeof(int);
	char* test2 = (char*) malloc(string_length);
	memcpy(test2, (char*) buffer + offset, string_length);
	test2[string_length] = '\0';
	fileName = test2;

	return 0;
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

	char *name = (char *) malloc(100);
	memcpy(name, (char *) buffer + offset, string_length);
	name[string_length] = '\0';
	offset += string_length;

	//check if this column belong to tableName table

	if (memcmp(name, tableName.c_str(), tableName.size()) != 0) {
		free(name);
		return RC_FAIL;
	}

	else {
		memcpy(&string_length, (char*) buffer + offset, sizeof(int));
		offset += sizeof(int);

		//get columnName
		memcpy(name, (char*) buffer + offset, string_length);
		name[string_length] = '\0';
		offset += string_length;
		(*attr).name = name;

		//get column type
		memcpy(&columnType, (char*) buffer + offset, sizeof(AttrType));
		offset += sizeof(AttrType);
		(*attr).type = columnType;

		//get column length
		memcpy(&columnLength, (char*) buffer + offset, sizeof(AttrLength));
		offset += sizeof(AttrLength);
		(*attr).length = columnLength;

		//get position
		memcpy(position, (char*) buffer + offset, sizeof(unsigned));
		offset += sizeof(unsigned);
		(*attr).position = *position;

		//check if read buffer correctly
		if (offset == lengthTuple) {
			free(name);
			return RC_SUCCESS;
		}

	}
	free(name);
	return EOF;
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

void getTotalEntries(void * buffer, short &totalEntries) {
	//totalEntries = *((short *) buffer + END_OF_PAGE - unit);
	memcpy(&totalEntries, (char*) buffer + END_OF_PAGE - unit,unit);

}

void writeTotalEntries(void * buffer, short totalEntries) {

	//*((short*) buffer + END_OF_PAGE - unit) = totalEntries;
	memcpy((char*) buffer + END_OF_PAGE - unit, &totalEntries,unit);
}

void getFreeSpace(void * buffer, short &freeSpace) {
	//freeSpace = *((short*) buffer + END_OF_PAGE - 2 * unit);
	memcpy(&freeSpace, (char*) buffer + END_OF_PAGE - 2 * unit,unit);

}

void writeFreeSpace(void * buffer, short freeSpace) {
	//*((short*) buffer + END_OF_PAGE - 2 * unit) = freeSpace;
	memcpy((char*) buffer + END_OF_PAGE - 2 * unit, &freeSpace,unit);
}

void writeSpecificOffset(void * buffer, short position, short value) {
	short offset = (position << 2) + unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, &value, unit);
}

void writeSpecificLength(void * buffer, short position, short value) {
	short offset = (position << 2) + 2 * unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, &value, unit);
}

void getSpecificOffset(void * buffer, short position, short &result) {
	short offset = (position << 2) + unit;
	memcpy(&result, (char*) buffer + END_OF_PAGE - offset, unit);
}

void getSpecificLength(void * buffer, short position, short &result) {
	short offset = (position << 2) + 2 * unit;
	memcpy(&result, (char*) buffer + END_OF_PAGE - offset, unit);
}

void writeNewestOffset(void * buffer) {
	short position;
	getTotalEntries(buffer, position);
	short value;

	short lastOffset;
	short lastLength;
	getSpecificOffset(buffer, position, lastOffset);
	getSpecificLength(buffer, position, lastLength);
	value = lastOffset + lastLength;

	writeSpecificOffset(buffer, position + 1, value);

}

void writeNewestLength(void * buffer, int &value) {
	short position;
	getTotalEntries(buffer, position);
	int offset = ((position + 1) << 2) + 2 * unit;
	memcpy((char *) buffer + END_OF_PAGE - offset, &value, unit);

	//remember to write totalEntries +1 after this
}

void getLastOffset(void * buffer, short &result) {
	short position;
	getTotalEntries(buffer, position);
	if (position == 0) {
		result = 0;
		return;
	}
	short offset = (position << 2) + unit;

	memcpy(&result, (char*) buffer + END_OF_PAGE - offset, unit);

}

RC RM::getAttributesAndSchema(const string tableName, vector<Attribute> &attrs,
		unsigned &schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	short offset = 0;
	short length;
	short N;
	short freeSpace;
	RC rc = -1;

	Attribute attr;
	unsigned schema_Column = 0, position_Column = 0;
	unsigned old_Schema = 32767;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	short index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i > 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, N);

		index = N;
		while (index > 0 || !finished) {

			getSpecificOffset(buffer, index, offset);
			getSpecificLength(buffer, index, length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			rc = getColumnTuple(&schema_Column, tableName, &attr,
					&position_Column, tuple, length);

			if (rc == RC_SUCCESS) {
				if (old_Schema == 32767) {
					old_Schema = schema_Column;
					schema = schema_Column;
				}
				if (old_Schema != schema_Column)
					finished = true;
				else
					attrs.push_back(attr);
			} else {
				if (attrs.size() >= 1)
					finished = true;
			}
			index--;

		}

	}
	reverse(attrs.begin(), attrs.end());
	return RC_SUCCESS;

}

RC RM::getAttributesOfSchema(const string tableName, vector<Attribute> &attrs,
		unsigned schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	short offset = 0;
	short length;
	short N;
	short freeSpace;
	RC rc = -1;

	Attribute attr;
	unsigned schema_Column = 0, position_Column = 0;
	unsigned old_Schema = 32767;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	short index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i > 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, N);

		index = N;
		while (index > 0 || !finished) {

			getSpecificOffset(buffer, index, offset);
			getSpecificLength(buffer, index, length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			unsigned version = 0;
			memcpy(&version, (char*) tuple, sizeof(unsigned));
			if (version == schema) {

				rc = getColumnTuple(&schema_Column, tableName, &attr,
						&position_Column, tuple, length);

				if (rc == RC_SUCCESS) {
					if (old_Schema == 32767) {
						old_Schema = schema_Column;
						attrs.push_back(attr);
					}
					if (old_Schema != schema_Column)
						finished = true;
				} else {
					if (attrs.size() >= 1)
						finished = true;
				}
			}
			index--;

		}

	}
	reverse(attrs.begin(), attrs.end());
	return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {

	//buffer contain page i starts from page 1
	//offset of directory
	short offset = PF_PAGE_SIZE;
	short length;
	short N;
	short freeSpace;
	RC rc = -1;

	Attribute attr;
	unsigned schema_Column = 0, position_Column = 0;
	unsigned old_Schema = 32767;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	int index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i > 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, N);

		//freeSpace of page i
		getFreeSpace(buffer, freeSpace);

		index = N;
		while (index > 0 && !finished) {
			//get last tuple of table Name from page i to have initial schema, it may not be N tuple
			getSpecificOffset(buffer, index, offset);
			getSpecificLength(buffer, index, length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			rc = getColumnTuple(&schema_Column, tableName, &attr,
					&position_Column, tuple, length);

			if (rc == RC_SUCCESS) {
				if (old_Schema == 32767) {
					old_Schema = schema_Column;
				}
				if (old_Schema != schema_Column)
					finished = true;
				else
					attrs.push_back(attr);
			} else {
				if (attrs.size() >= 1)
					finished = true;
			}
			index--;
		}

	}
	reverse(attrs.begin(), attrs.end());

	return 0;

}
RC RM::getLatestSchema(const string tableName, short &result) {
//buffer contain page i starts from page 1
//offset of directory

	void *buffer = malloc(PF_PAGE_SIZE);
	void * data = malloc(PF_PAGE_SIZE);
	char *name = (char *) malloc(100);

	short offset;
	short length;
	short index;
	bool finished = false;
	short temp_schema;
	PageNum pageNum = columnHandle.GetNumberOfPages();

	//traverse bottom-up
	for (unsigned i = pageNum - 1; i > 0; i--) {
		if (finished)
			break;

		//read page i
		columnHandle.ReadPage(i, buffer);

		//read number of tuples on page i
		getTotalEntries(buffer, index);

		//get last tuple of tableName from page i to get latest schema

		while (index > 0 && !finished) {
			getSpecificOffset(buffer, index, offset);
			getSpecificLength(buffer, index, length);

			//read tuple column N

			memcpy(data, (char*) buffer + offset, length);

			offset = 0;
			memcpy(&temp_schema, (char*) data, sizeof(unsigned));
			offset += sizeof(unsigned);

			int string_length;
			memcpy(&string_length, (char*) data + offset, sizeof(int));
			offset += sizeof(int);

			memcpy(name, (char *) buffer + offset, string_length);
			name[string_length] = '\0';
			offset += string_length;

			//check if this column belong to tableName table

			if (memcmp(name, tableName.c_str(), tableName.size()) == 0) {
				result = temp_schema;
				finished = true;
			}

			index--;
		}
	}

	free(buffer);
	free(name);
	free(data);
	if (finished)
		return RC_SUCCESS;
	else
		return RC_FAIL;
}

// used in data file, use only 2 bytes for schema for efficiency
void insertSchema(void * tuple, int schema, int tuple_size) {
	void *temp = malloc(100);

//write schema to the begin of temp
	memcpy((char*) temp, (short*) &schema, unit);
//write temp_data to temp, from byte 5th to (byte (5 + tuple_size)th
	memcpy((char*) temp + unit, (char*) tuple, tuple_size);

	memcpy((char*) tuple, (char*) temp, tuple_size + unit);

	free(temp);
}

void getDataSize(const void *data, vector<Attribute> attrs, int *tuple_size,
		bool hasSchema) {

	int length_var_char;
	int offset;

	if (hasSchema) {
		offset = 2;

	} else {
		offset = 0;

	}

	for (unsigned i = 0; i < attrs.size(); i++) {
		switch (attrs[i].type) {
		case TypeInt:
		case TypeReal:
			offset += 4;
			break;
		case TypeVarChar:
			memcpy(&length_var_char, (char*) data + offset, 4);
			offset += 4 + length_var_char;
			break;
		default:
			cerr << "Error decoding attrs[i]" << endl;
			break;
		}
	}

	*tuple_size = offset;
}

void getLastLength(void * buffer, short &result) {
	short position;
	getTotalEntries(buffer, position);
	if (position == 0) {
		result = 0;
		return;
	}

	short offset = (position << 2) + 2 * unit;

	memcpy(&result, (char*) buffer + END_OF_PAGE - offset, unit);
}

void writeTo(void * dest, void* source, short source_len) {
	short lastOffset;
	short lastLength;
	getLastOffset(dest, lastOffset);
	getLastLength(dest, lastLength);
	short offset = lastOffset + lastLength;

	memcpy((char*) dest + offset, (char*) source, source_len);

//update directory:
	short total;
	short test;
	getTotalEntries(dest, total);
	total++;
	writeSpecificOffset(dest, total, offset);
	writeSpecificLength(dest, total, source_len);
	writeTotalEntries(dest, total);
	getTotalEntries(dest, test);

	short free_space;
	getFreeSpace(dest, free_space);
	free_space = free_space - source_len - 4;
	writeFreeSpace(dest, free_space);
	getFreeSpace(dest, test);

}

//this vector<Attribute> has only regular column name
//thus the size to be put in the column file is
// 4 bytes of schema + VarChar attribute name + VarChar tableName + 4 for Type + 4 for Length + 4 for position + 4 for directory
// for each attrs[i] => 20+ bytes for attribute name
void getSizeOfVectorAttributeForColumnFile(string tableName,
		const vector<Attribute> &attrs, short &vector_size) {

	short tNameLen = 4 + tableName.length();

	short total = 0;

	for (unsigned i = 0; i < attrs.size(); i++) {
		//total = total + 4 + 4 + attrs[i].name.length() + tNameLen + 4 + 4 + 4 + 4;
		total += 24 + attrs[i].name.length() + tNameLen;
	}
	vector_size = total;
}

RC getSizeOfAttribute(void *data, Attribute attr, short &size) {
	int count = 0;
	int length_var_char;
	switch (attr.type) {
	case TypeInt:
	case TypeReal:
		count += 4;
		break;
	case TypeVarChar:
		memcpy(&length_var_char, (char*) data, 4);
		count = count + 4 + length_var_char;
		break;
	default:
		cerr << "Error decoding attrs[i]" << endl;
		break;
	}
	size = count;
	return 0;
}
void readFrom(void* ptr, int offset, positive_twobytes value) {
	memcpy(&value, (char *) ptr + offset, sizeof(short));
}

void readFrom(void* ptr, int offset, void* value) {
	memcpy(value, (char *) ptr + offset, sizeof(short));
}

void writeFreeSpaceInHeaderPage(void * buffer, short index, short value) {
	short offset = index << 1;

	memcpy((char*) buffer + offset, &value, unit);
	//*((short*) buffer + offset) = value;
}

void getFreeSpaceInHeaderPage(void * buffer, short index, short &value) {
	short offset = index << 1;

	memcpy(&value, (char*) buffer + offset, unit);
	//value = *((short*) buffer + offset);
}
RC RM::openTable(const string tableName, PF_FileHandle &tableHdl) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())
				== RC_SUCCESS) {
			if (allTables[i].fileHandle.HasHandle()) {
				tableHdl = allTables[i].fileHandle;
				return RC_SUCCESS;
			} else {
				fileManager->OpenFile(tableName.c_str(), tableHdl);
				return RC_SUCCESS;
			}
		}
	}

	string fileName = tableName + ".data";
	fileManager->OpenFile(fileName.c_str(), tableHdl);

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
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())
				== RC_SUCCESS) {
			handle = allTables[i].fileHandle;
			return RC_SUCCESS;
		}
	}

	return RC_FAIL;

}
RC RM::closeTable(const string tableName) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str())
				== RC_SUCCESS) {
			fileManager->CloseFile(allTables[i].fileHandle); // fileHandle got cleared
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
		short uno;
		short free_space = PF_PAGE_SIZE - 4;
		writeTotalEntries(buffer, 0);
		writeFreeSpace(buffer, free_space);
		getTotalEntries(buffer, uno);
		short testfrees;
		getFreeSpace(buffer, testfrees);
		/*short offsetFreeSpace = 0;
		 short total_entry = 0;
		 short length;
		 short offsetTuple;
		 total_entry = 1;
		 length = tuple_size;
		 offset*/
		writeTo(buffer, tuple, tuple_size);
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

	bool column_new = false;
	if (stat(column_file_name, &stFileInfo) != 0) { //column not exist
		column_new = true;
		fileManager->CreateFile(column_file_name);
		fileManager->OpenFile(column_file_name, columnHandle);

		void * buffer = malloc(PF_PAGE_SIZE);
		void *data = malloc(100);
		short tuple_size;
		//short offset = 0;

		short initFree = PF_PAGE_SIZE - unit;
		writeFreeSpaceInHeaderPage(buffer, 0, initFree);
		columnHandle.WritePage(0, buffer);

		short uno = 0;
		short free_space = PF_PAGE_SIZE - 4;
		writeTotalEntries(buffer, uno);
		writeFreeSpace(buffer, free_space);

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

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 2, data, &tuple_size);
		writeTo(buffer, data, tuple_size);
		//writeTo(buffer, offset, data, &tuple_size);
		//offset += tuple_size;

		attr.name = "ColName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 20;
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

		getFreeSpace(buffer, free_space);

		columnHandle.WritePage(1, buffer);

		columnHandle.ReadPage(0, buffer);
		writeFreeSpaceInHeaderPage(buffer, 1, free_space);
		columnHandle.WritePage(0, buffer);
		/*short freespace = PF_PAGE_SIZE - offset;
		 cout<< " free space is: "<< freespace <<endl;*/
		fileManager->CloseFile(columnHandle);
		free(buffer);
		free(data);
	}
	fileManager->OpenFile(column_file_name, columnHandle);

	if (!column_new) {
		getAttributes("Catalog", catalogAttrs);
		getAttributes("Column", columnAttrs);
		printAttributes(catalogAttrs);
		cout << endl;
		printAttributes(columnAttrs);
	}

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
		memcpy((char*) string_value.c_str(), (char*) data + 4, length_var_char);
		cout << string_value << endl;
	}
}

void printDataFromAttributes(void *data, vector<Attribute> attrs) {
	int length_var_char;
	int int_value;
	float float_value;
	string string_value;
	unsigned i = 0;
	short offset;

	for (i = 0; i <= attrs.size(); i++) {
		offset = 0;
		switch (attrs[i].type) {
		case TypeInt:
			memcpy(&int_value, (char*) data + offset, 4);
			cout << int_value << endl;
			offset += 4;
			break;
		case TypeReal:
			memcpy(&float_value, (char*) data + offset, 4);
			cout << float_value << endl;
			offset += 4;
			break;
		case TypeVarChar:
			memcpy(&length_var_char, (char*) data + offset, 4);
			memcpy((char*) string_value.c_str(), (char*) data + 4 + offset,
					length_var_char);
			cout << string_value << endl;
			offset += 4 + length_var_char;
			break;
		default:
			cerr << "Error @ printDataFromAttributes" << endl;
			break;
		}
	}
}

RM::~RM() {
	fileManager->CloseFile(columnHandle);
	fileManager->CloseFile(catalogHandle);
}

void createNewPage(void * buffer) {

	int uno = 0;
	memset(buffer, uno, PF_PAGE_SIZE);
	short free_space = PF_PAGE_SIZE - 4;
	writeFreeSpace(buffer, free_space);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {

	printAttributes(attrs);
	void *data = malloc(100);
	void *catalog_buffer = malloc(PF_PAGE_SIZE);
	void *column_buffer = malloc(PF_PAGE_SIZE);
	string fileName = tableName + ".data";
	fileManager->CreateFile(fileName.c_str());

	PF_FileHandle tableFileHandle;
	openTable(tableName, tableFileHandle);
	void *datafile_buffer = malloc(PF_PAGE_SIZE);

	writeFreeSpaceInHeaderPage(datafile_buffer, 0, PF_PAGE_SIZE - 4);
	writeFreeSpaceInHeaderPage(datafile_buffer, 1, PF_PAGE_SIZE - 4);
	tableFileHandle.WritePage(0, datafile_buffer);
	createNewPage(datafile_buffer);
	tableFileHandle.WritePage(1, datafile_buffer);
	free(datafile_buffer);

	short test;
	short tuple_size;
	short freeSpace;
	PageNum currentPage = 0;
	catalogHandle.ReadPage(currentPage, catalog_buffer);
	prepareCatalogTuple(tableName, fileName, data, &tuple_size);
	bool pass = false;
	do {

		getFreeSpace(catalog_buffer, freeSpace);
		if (freeSpace >= tuple_size + 4) { // enough space to write
			writeTo(catalog_buffer, data, tuple_size);
			pass = true;
		} else {
			//catalogHandle.WritePage(currentPage, catalog_buffer);
			currentPage++;
			if (currentPage == catalogHandle.GetNumberOfPages())
				createNewPage(catalog_buffer);
			else
				catalogHandle.ReadPage(currentPage, catalog_buffer);

		}
	} while (!pass);

	catalogHandle.WritePage(currentPage, catalog_buffer);

	void *header_buffer = malloc(PF_PAGE_SIZE);
	columnHandle.ReadPage(0, header_buffer);
	PageNum N = columnHandle.GetNumberOfPages();

	/*short vector_length;

	 getSizeOfVectorAttributeForColumnFile(tableName, attrs, vector_length);
	 for(unsigned i = N-1; i> 0; i--) {
	 getFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);

	 }*/
	currentPage = N - 1;
	bool dirty = false;

	for (unsigned i = 0; i < attrs.size(); i++) {

		prepareColumnTuple(1, tableName, attrs[i], i + 1, data, &tuple_size);
		printAttribute(attrs[i]);
		pass = false;
		do {
			if (currentPage == columnHandle.GetNumberOfPages()) {
				if (dirty) {
					columnHandle.WritePage(currentPage - 1, column_buffer);
				}
				createNewPage(column_buffer);
				freeSpace = PF_PAGE_SIZE - 4;
				writeFreeSpaceInHeaderPage(header_buffer, currentPage,
						freeSpace);
			}
			getFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
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
		writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
		if (dirty == false)
			columnHandle.ReadPage(currentPage, column_buffer);
		writeTo(column_buffer, data, tuple_size);
		dirty = true;
	}
	columnHandle.WritePage(0, header_buffer);
	if (dirty == true)
		columnHandle.WritePage(currentPage, column_buffer);

	free(data);
	free(catalog_buffer);
	free(column_buffer);
	free(header_buffer);
	return RC_SUCCESS;
}

RC RM::deleteTable(const string tableName) {
	//delete file tableName.data
	closeTable(tableName);
	string fileName = tableName + ".data";
	fileManager->DestroyFile(fileName.c_str());

	void *buffer = malloc(PF_PAGE_SIZE);

	void *tuple = malloc(PF_PAGE_SIZE);
	short N, freeSpace, offset, length;
	short deletedOffset = -1;

	bool finished = false;

	//delete table name in catalogTable and column name from columnTable

	//delete in catalogTable
	PageNum pageNum = catalogHandle.GetNumberOfPages();
	for (unsigned i = pageNum - 1; i >= 0; i--) {
		if (finished)
			break;
		//read page i
		catalogHandle.ReadPage(i, buffer);
		//read number of tuple on page i
		getTotalEntries(buffer, N);

		for (short index = 1; index <= N; index++) {

			//get last tuple of table Name from page i to have initial schema, it may not be N tuple
			getSpecificOffset(buffer, index, offset);
			getSpecificLength(buffer, index, length);

			//get tuple
			memcpy((char*) tuple, (char*) buffer + offset, length);
			string table_Name, file_Name;
			getCatalogTuple(table_Name, file_Name, tuple);

			//find tableName in Catalog
			if (strcmp(table_Name.c_str(), tableName.c_str()) == 0) {
				//delete tuple in Catalog
				writeSpecificOffset(buffer, index, deletedOffset);
				//memcpy((char*) buffer + offset, (char*) tuple, length); // why????
				catalogHandle.WritePage(i, buffer);
				//thoat ra khoi ca 2 vong lap
				finished = true;
				index = N + 1;
			}
		}

	}

	// Xoa bang column tableName
	unsigned schema_Column, position_Column;
	Attribute attr;
	finished = false;
	short tuple_offset;
	pageNum = columnHandle.GetNumberOfPages();
	for (PageNum i = pageNum - 1; i > 0; i--) {

		//read page i
		columnHandle.ReadPage(i, buffer);

		//read number of column tuple on page i
		getTotalEntries(buffer, N);

		for (short index = 1; index <= N; index++) {

			getSpecificOffset(buffer, index, offset);
			if (offset == deletedOffset)
				continue;
			getSpecificLength(buffer, index, length);

			//get tuple
			memcpy((char*) tuple, (char*) buffer + offset, length);

			tuple_offset = sizeof(unsigned);

			int string_length;
			memcpy(&string_length, (char*) tuple + offset, sizeof(int));
			tuple_offset += sizeof(int);

			char *name = (char *) malloc(100);
			memcpy(name, (char *) tuple + tuple_offset, string_length);
			name[string_length] = '\0';

			//check if this column belong to tableName table

			if (memcmp(name, tableName.c_str(), tableName.size()) == 0) {
				{
					writeSpecificOffset(buffer, index, deletedOffset);

				}

			}
		}
		// Het trang i
		columnHandle.WritePage(i, buffer);

	}
	free(buffer);
	free(tuple);
	return RC_SUCCESS;
}

//  Format of the data passed into the function is the following:
//  1) data is a concatenation of values of the attributes
//  2) For int and real: use 4 bytes to store the value;
//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
RC RM::insertTuple(const string tableName, const void *data, RID &rid) {

	PF_FileHandle tableFileHandle;

	getTableHandle(tableName, tableFileHandle);

	void *content_buffer = malloc(PF_PAGE_SIZE);
	void *header_buffer = malloc(PF_PAGE_SIZE);
	void *temp_data = malloc(PF_PAGE_SIZE);

	tableFileHandle.ReadPage(0, header_buffer);

	PageNum N = tableFileHandle.GetNumberOfPages();
	PageNum currentPage;

	unsigned latest_schema;
	vector<Attribute> tableAttributes;

	// get latest attributes and latest schema
	getAttributesAndSchema(tableName, tableAttributes, latest_schema);

	int tuple_size = 0;

	getDataSize((void*) data, tableAttributes, &tuple_size, false);

	short schema = (short) latest_schema;
	//write schema to the begin of temp_data, which takes 2 bytes
	memcpy((char*) temp_data, &schema, unit);
	//write data to temp_data, from byte 3th to (byte (3 + tuple_size)th
	memcpy((char*) temp_data + unit, (char*) data, tuple_size);

	tuple_size += 2;
	currentPage = 1;
	bool dirty = false;
	bool pass = false;
	short freeSpace;

	do {
		if (currentPage == N) {
			createNewPage(content_buffer);
			freeSpace = PF_PAGE_SIZE - 4;
			writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
		}
		getFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
		if (freeSpace < tuple_size + 4) {
			currentPage++;
		} else {
			pass = true;
		}
	} while (!pass);

	freeSpace = freeSpace - tuple_size - 4;
	writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
	tableFileHandle.ReadPage(currentPage, content_buffer);
	writeTo(content_buffer, temp_data, tuple_size);

	short entries;
	getTotalEntries(content_buffer, entries);
	rid.slotNum = entries;
	rid.pageNum = currentPage;

	tableFileHandle.WritePage(0, header_buffer);
	tableFileHandle.WritePage(currentPage, content_buffer);

	free(temp_data);
	free(header_buffer);
	free(content_buffer);

	return RC_SUCCESS;
}

RC RM::deleteTuples(const string tableName) {
	//delete all tuples
	void * buffer = malloc(PF_PAGE_SIZE);
	PF_FileHandle fileHandle;
	short offset = -1;
	short N;

	getTableHandle(tableName, fileHandle);
	PageNum pageNum = fileHandle.GetNumberOfPages();

	//traverse bottom-up
	for (unsigned i = pageNum - 1; i > 0; i--) {
		//read page i
		fileHandle.ReadPage(i, buffer);
		//read number of column tuple on page i
		getTotalEntries(buffer, N);
		//memcpy(&N, (char*) buffer + offset - sizeof(short), sizeof(short));
		//offset = offset - sizeof(short);
		//write -1 to all offset
		for (int i = 1; i <= N; i++) {
			writeSpecificOffset(buffer, i, offset);
		}
		fileHandle.WritePage(i,buffer);
	}
	return 0;
}

RC RM::deleteTuple(const string tableName, const RID &rid) {
	void * buffer = malloc(PF_PAGE_SIZE);
	PF_FileHandle fileHandle;
	short offset = -1;

	getTableHandle(tableName, fileHandle);

//read page rid.pageName contain tuple rid
	fileHandle.ReadPage(rid.pageNum, buffer);

//get tuple rid.slotNum and write -1 to offset of rid.slotNum
//offset of the slotNum's offset
	writeSpecificOffset(buffer, (short) rid.slotNum, offset);

//Nhu vay la da dat offset cua slotNum bang -1

	fileHandle.WritePage(rid.pageNum, buffer);

	free(buffer);
	return 0;
}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {

	PF_FileHandle fileHandle;
	getTableHandle(tableName, fileHandle);

	void * buffer;
	buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(rid.pageNum, buffer);
	void *temp_data = malloc(PF_PAGE_SIZE);
	short length, offset, size, freeSpace, lastOffset, lastLength;
	getSpecificOffset(buffer, rid.slotNum, offset);
	getSpecificLength(buffer, rid.slotNum, length);
	vector<Attribute> attrs;
	unsigned temp_schema;
	short schema;
	getAttributesAndSchema(tableName, attrs, temp_schema);
	schema = (short) temp_schema;
	int tuple_size;
	getDataSize(data, attrs, &tuple_size, false);

	//write schema to the begin of temp_data, which takes 2 bytes
	memcpy((char*) temp_data, &schema, unit);
	//write data to temp_data, from byte 3th to (byte (3 + tuple_size)th
	memcpy((char*) temp_data + unit, (char*) data, tuple_size);
	tuple_size += 2;
	if (tuple_size <= length) {
		//Chi viec ghi thay vao tuple
		//writeTo(buffer, data, size);
		//copy data vao buffer
		writeSpecificLength(buffer, rid.slotNum, (short) tuple_size);
		memcpy((char*) buffer + offset, temp_data, tuple_size);
		fileHandle.WritePage(rid.pageNum, buffer);
		free(buffer);
		return RC_SUCCESS;
	} else {
		getFreeSpace(buffer, freeSpace);
		if (tuple_size <= freeSpace)
		//Chi can <= freeSpace, ko can + 4 vi ghi vao slot cu
				{
			//tao them mot tuple moi tuong ung voi data
			getLastOffset(buffer, lastOffset);
			getLastLength(buffer, lastLength);
			//ghi data vao lastOffset
			memcpy((char*) buffer + lastOffset + lastLength, temp_data,
					tuple_size);
			//update directory, write offset and length
			writeSpecificOffset(buffer, rid.slotNum, lastOffset + lastLength);
			writeSpecificLength(buffer, rid.slotNum, tuple_size);
			fileHandle.WritePage(rid.pageNum, buffer);
			free(buffer);
			return RC_SUCCESS;
		} else {

			RID newRID;

			insertTuple(tableName, data, newRID);
			writeSpecificLength(buffer, rid.slotNum, -6);
			memcpy((char*) buffer + offset, (short*) &newRID.pageNum, unit);
			memcpy((char*) buffer + offset + unit,(short*) &newRID.slotNum
					,unit);
			fileHandle.WritePage(rid.pageNum, buffer);
			free(buffer);
			return RC_SUCCESS;
		}
	}
	free(buffer);
	return RC_FAIL;
}
/*
 //Find page to store the updated tuple;
 void * header_buffer = malloc(PF_PAGE_SIZE);
 void * temp_buffer = malloc(PF_PAGE_SIZE);
 fileHandle.ReadPage(0,header_buffer);
 bool pass = false;
 bool newPage = false;
 short currentPage=1;
 do {
 if ((unsigned)currentPage == fileHandle.GetNumberOfPages()) {
 createNewPage(temp_buffer);
 newPage = true;
 writeFreeSpaceInHeaderPage(header_buffer,currentPage,PF_PAGE_SIZE-4);
 }
 getFreeSpaceInHeaderPage(header_buffer,currentPage,freeSpace);
 if ((short) (tuple_size+4) <= freeSpace)
 pass= true;
 else
 currentPage ++;
 }while (!pass);

 if (!newPage)
 fileHandle.ReadPage(currentPage,temp_buffer);
 void * temp_data = malloc()
 writeTo(temp_buffer, data,(short)tuple_size);
 short entries;
 getTotalEntries(temp_buffer,entries);

 //change noi dung cua rid tuple, noi dung cua no se chua rid cua data
 short page_data, slot_data;
 page_data = fileHandle.GetNumberOfPages();
 slot_data = 1;

 //cap nhat noi dung cua

 memcpy((char*) buffer + offset, &page_data, 2);
 memcpy((char*) buffer + offset + 2, &slot_data, 2);
 fileHandle.WritePage(rid.pageNum, buffer);
 //Nhu vay noi dung cua record cu se chua 4 byte dau la rid cua data trong trang moi
 }
 }
 */

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	PF_FileHandle fileHandle;
	getTableHandle(tableName, fileHandle);

	short N, freeSpace, length, offset;
	void * buffer = malloc(PF_PAGE_SIZE);

	fileHandle.ReadPage(rid.pageNum, buffer);
	getTotalEntries(buffer, N);

	getSpecificOffset(buffer, rid.slotNum, offset);

	getSpecificLength(buffer, rid.slotNum, length);
	if (offset == EOF) { // tuple got marked deleted
		memset(data, 0, length);
		return RC_FAIL;
	}

	if (length == -6) {

		short temp;
		RID newRID;
		memcpy(&temp, (char*) buffer + offset, unit);
		newRID.pageNum = (unsigned) temp;
		memcpy(&temp, (char*) buffer + offset + unit,unit);
		newRID.slotNum = (unsigned) temp;
		return readTuple(tableName, newRID, data);
	}
	//chop off 2 bytes of schema
	offset += unit;
	length -= unit;
	memcpy(data, (char*) buffer + offset, length);
	free(buffer);
	return RC_SUCCESS;

}

RC RM::readAttribute(const string tableName, const RID &rid,
		const string attributeName, void *data) {
	//get TableHAndle (tableName)
	PF_FileHandle fileHandle;
	vector<Attribute> attrs;
	Attribute attr;
	short offsetofAttribute = 0, lengthOfAttribute;
	short offset, length, N, freeSpace;

	void * buffer = malloc(PF_PAGE_SIZE);
	void *temp = malloc(200);

	//get attributeName
	getTableHandle(tableName, fileHandle);
	fileHandle.ReadPage(rid.pageNum, buffer);

	getSpecificOffset(buffer, rid.slotNum, offset);
	getSpecificLength(buffer, rid.slotNum, length);
	memcpy(temp, (char*) buffer + offset, length);
	short schema;
	memcpy(&schema, (char*) buffer, unit);
	unsigned temp_sch;

	getAttributesAndSchema(tableName, attrs, temp_sch);
	offset = 2;
	if (schema == (short) temp_sch) {
		int index;
		int length_var_char;
		for (int i = 0; i < (int) attrs.size(); i++) {
			if (strcmp(attrs[i].name.c_str(), attributeName.c_str())
					== RC_SUCCESS) {
				switch (attrs[i].type) {
				case TypeInt:
				case TypeReal:
					memcpy((char*) data, (char*)temp +offset,4);
					break;
				case TypeVarChar:
					memcpy(&length_var_char, (char*) temp + offset, 4);
					memcpy((char*) data, (char*)temp +offset,length_var_char+4);
					break;
				default:
					cerr << "Error decoding attrs[i]" << endl;
					break;
				}
				index = i;
				break;
			} else {

				switch (attrs[i].type) {
				case TypeInt:
				case TypeReal:
					offset += 4;
					break;
				case TypeVarChar:
					memcpy(&length_var_char, (char*) temp + offset, 4);
					offset = offset + 4 + length_var_char;
					break;
				default:
					cerr << "Error decoding attrs[i]" << endl;
					break;
				}

			}
		}
	}
	// if schema is not the latest schema, do something else
	free(buffer);
	free(temp);

	return 0;

}

	/*
	getTotalEntries(buffer, N);
	//freeSpace of page i
	getFreeSpace(buffer, freeSpace);
	//or

	// find out offset of this Attribute in temp bytesequence
	short size = 0;
	for (int i = 0; i < index; i++) {
		getSizeOfAttribute((char*) temp + offsetofAttribute, attrs[i], size);
		offsetofAttribute += size;
	}

	// find out length of this Attribute in temp bytesequence
	getSizeOfAttribute((char*) temp + offsetofAttribute, attrs[index],
			lengthOfAttribute);

	memcpy((char*) data, (char*) temp + offsetofAttribute, lengthOfAttribute);
	// read tuple rid
	//get Vector<Attribute>
	// compare with attributeName, find the offset & size of attribute
	// memcpy(data, tuple+offset,size
	 * */



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

	PF_FileHandle fileHandle;
	RM* RecordManager = RM::Instance();
	RecordManager->getTableHandle(tableName, fileHandle);

	void * buffer = malloc(PF_PAGE_SIZE);
	//fileHandle.ReadPage(, buffer);
	int offset, length;
	//getSpecificOffset(buffer,rid.slotNum,&offset);
	//getSpecificLength(buffer,rid.slotNum,&length);

	void * tuple = malloc(PF_PAGE_SIZE);

	short schema;

	vector<Attribute> attributes;

	//RecordManager->getAttributesOfSchema(tableName,attributes,schema);
	return RM_EOF;
}

RC RM_ScanIterator::close() {
	return -1;
}

