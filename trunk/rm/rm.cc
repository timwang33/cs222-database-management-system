#include "rm.h"
#include<algorithm>

using namespace std;
RM* RM::_rm = 0;

RM* RM::Instance() {

	if (!_rm)
		_rm = new RM();

	return _rm;
}

bool IsFileExists(string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

void printAttribute(Attribute att) {
	cout << "NAME = " << att.name << " ";
	cout << "Type = " << att.type << " ";
	cout << "Length = " << att.length << endl;

}
void printAttributes(const vector<Attribute> &list) {
	for (unsigned i = 0; i < list.size(); i++)
		printAttribute(list[i]);

}

void prepareColumnTuple(unsigned schema, const string tableName, Attribute attr, unsigned position, void *buffer, short *tuple_size) {
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
RC getColumnTuple(unsigned *schema, const string tableName, Attribute *attr, unsigned *position, void *buffer, short lengthTuple) {
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

	if (strcmp(name, tableName.c_str()) != 0) {
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

void prepareCatalogTuple(const string name, const string filename, void *buffer, short *tuple_size) {
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

RC RM::getAttributesAndSchema(const string tableName, vector<Attribute> &attrs, unsigned &schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	short offset = 0;
	short length;
	short N;

	RC rc = -1;

	Attribute attr;
	attrs.clear();
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
		while (index > 0 && !finished) {

			getSpecificOffset(buffer, index, offset);
			if (offset != -1) {
				getSpecificLength(buffer, index, length);

				//get tuple

				memcpy((char*) tuple, (char*) buffer + offset, length);

				rc = getColumnTuple(&schema_Column, tableName, &attr, &position_Column, tuple, length);

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
			}
			index--;

		}

	}
	reverse(attrs.begin(), attrs.end());
	return RC_SUCCESS;

}

RC RM::getAttributesOfSchema(const string tableName, vector<Attribute> &attrs, unsigned schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	short offset = 0;
	short length;
	short N;

	RC rc = -1;

	Attribute attr;
	attrs.clear();
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
			if (offset != -1) {
				getSpecificLength(buffer, index, length);

				//get tuple

				memcpy((char*) tuple, (char*) buffer + offset, length);

				unsigned version = 0;
				memcpy(&version, (char*) tuple, sizeof(unsigned));
				if (version == schema) {

					rc = getColumnTuple(&schema_Column, tableName, &attr, &position_Column, tuple, length);

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
	attrs.clear();
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
			if (offset != -1) {

				getSpecificLength(buffer, index, length);

				//get tuple

				memcpy((char*) tuple, (char*) buffer + offset, length);

				rc = getColumnTuple(&schema_Column, tableName, &attr, &position_Column, tuple, length);

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

void getDataSize(const void *data, vector<Attribute> attrs, int *tuple_size, bool hasSchema) {

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

	if (result == TOMBSTONE
	)
		result = LENGTH_TOMBSTONE;

}

void writeTo(void * dest, void* source, short source_len) { //source must already have schema

	short lastOffset = 0;
	short lastLength = 0;

	short total = 0;
	short tempTotal = 0;
	getTotalEntries(dest, total);
	if (total != 0) {
		tempTotal = total;

		bool pass = false;
		do {
			getSpecificOffset(dest, tempTotal, lastOffset);
			if (lastOffset == -1) {
				tempTotal -= 1;
			} else {
				getSpecificLength(dest, tempTotal, lastLength);
				if (lastLength == TOMBSTONE) {
					lastLength = LENGTH_TOMBSTONE;
				} else if (lastLength < 0) {
					lastLength *= -1;
				}
				pass = true;
			}

		} while (!pass);
	}
	short offset = lastOffset + lastLength;

	memcpy((char*) dest + offset, (char*) source, source_len);

//update directory:

	short freeing_size = 0;
	short tempLength = 0;
	for (short i = tempTotal + 1; i <= total; i++) {
		getSpecificLength(dest, i, tempLength);
		freeing_size += tempLength + 4;
	}

	total = tempTotal;
	total++;
	writeSpecificOffset(dest, total, offset);
	writeSpecificLength(dest, total, source_len);
	writeTotalEntries(dest, total);

	short free_space;
	getFreeSpace(dest, free_space);
	free_space = free_space + freeing_size - source_len - 4;
	writeFreeSpace(dest, free_space);

}

//this vector<Attribute> has only regular column name
//thus the size to be put in the column file is
// 4 bytes of schema + VarChar attribute name + VarChar tableName + 4 for Type + 4 for Length + 4 for position + 4 for directory
// for each attrs[i] => 20+ bytes for attribute name
void getSizeOfVectorAttributeForColumnFile(string tableName, const vector<Attribute> &attrs, short &vector_size) {

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

void createNewPage(void * buffer) {

	memset(buffer, 0, PF_PAGE_SIZE);
	short free_space = PF_PAGE_SIZE - 4;
	writeFreeSpace(buffer, free_space);
}
RC RM::hasTable(const string tableName) {
	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str()) == RC_SUCCESS && allTables[i].fileHandle.HasHandle()) {
			return RC_SUCCESS;
		}

	}
	return RC_FAIL;
}
RC RM::openTable(const string tableName, PF_FileHandle &tableHdl) {
	string fileName = tableName + ".data";
	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str()) == RC_SUCCESS) {
			if (allTables[i].fileHandle.HasHandle()) {
				tableHdl = allTables[i].fileHandle;
				return RC_SUCCESS;
			} else {
				fileManager->OpenFile(fileName.c_str(), tableHdl);
				return RC_SUCCESS;
			}
		}
	}

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
		if (strcmp(tableName.c_str(), allTables[i].name.c_str()) == RC_SUCCESS) {
			handle = allTables[i].fileHandle;
			return RC_SUCCESS;
		}
	}

	return RC_FAIL;

}
RC RM::closeTable(const string tableName) {

	for (int i = 0; i < (int) allTables.size(); i++) {
		if (strcmp(tableName.c_str(), allTables[i].name.c_str()) == RC_SUCCESS) {
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

		void * buffer = malloc(PF_PAGE_SIZE);
		createNewPage(buffer);

		prepareCatalogTuple("Catalog", catalog_file_name, tuple, &tuple_size);
		writeTo(buffer, tuple, tuple_size);

		prepareCatalogTuple("Column", column_file_name, tuple, &tuple_size);
		writeTo(buffer, tuple, tuple_size);

		catalogHandle.WritePage(0, buffer);

		fileManager->CloseFile(catalogHandle);
		free(tuple);
		free(buffer);
	}

	fileManager->OpenFile(catalog_file_name, catalogHandle);

	//bool column_new = false;
	if (stat(column_file_name, &stFileInfo) != 0) { //column not exist
		//column_new = true;
		fileManager->CreateFile(column_file_name);
		fileManager->OpenFile(column_file_name, columnHandle);

		void * buffer = malloc(PF_PAGE_SIZE);
		void *data = malloc(100);
		short tuple_size;
		//short offset = 0;

		short initFree = PF_PAGE_SIZE - unit;
		writeFreeSpaceInHeaderPage(buffer, 0, initFree);
		columnHandle.WritePage(0, buffer);

		createNewPage(buffer);
		short free_space;
		Attribute attr;

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		prepareColumnTuple(1, "Catalog", attr, 1, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		catalogAttrs.push_back(attr);

		attr.name = "FileName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 50;
		catalogAttrs.push_back(attr);
		prepareColumnTuple(1, "Catalog", attr, 2, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "Schema";
		attr.type = TypeInt;
		attr.length = 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 1, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 2, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "ColName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 20;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 3, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "Type";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 4, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "Length";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 5, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		attr.name = "Position";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
		prepareColumnTuple(1, "Column", attr, 6, data, &tuple_size);
		writeTo(buffer, data, tuple_size);

		getFreeSpace(buffer, free_space);

		columnHandle.WritePage(1, buffer);

		columnHandle.ReadPage(0, buffer);
		writeFreeSpaceInHeaderPage(buffer, 1, free_space);
		columnHandle.WritePage(0, buffer);

		fileManager->CloseFile(columnHandle);
		free(buffer);
		free(data);
	}
	fileManager->OpenFile(column_file_name, columnHandle);

	//if (!column_new) {
	//getAttributes("Catalog", catalogAttrs);
	//getAttributes("Column", columnAttrs);
	//}

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

RC RM::printDataFromAttributes(void *data, vector<Attribute> &attrs) {
	int length_var_char = 0;
	int int_value = 0;
	float float_value = 0.0;
	char* string_value = (char*) malloc(100);
	unsigned i = 0;
	short offset;
	offset = 0;
	for (i = 0; i < attrs.size(); i++) {

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
			memcpy(string_value, (char*) data + 4 + offset, length_var_char);
			string_value[length_var_char] = '\0';
			cout << string_value << endl;
			offset += 4 + length_var_char;
			break;
		default:
			cerr << "Error @ printDataFromAttributes" << endl;
			break;
		}
	}
	free(string_value);
	return RC_SUCCESS;
}

RM::~RM() {
	for (unsigned i = 0; i < allTables.size(); i++) {
		if (allTables[i].fileHandle.HasHandle())
			fileManager->CloseFile(allTables[i].fileHandle);
	}
	allTables.clear();

	fileManager->CloseFile(columnHandle);
	fileManager->CloseFile(catalogHandle);

}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {

	//printAttributes(attrs);
	RC rc;

	void *data = malloc(100);
	void *catalog_buffer = malloc(PF_PAGE_SIZE);
	void *column_buffer = malloc(PF_PAGE_SIZE);
	void *tuple = malloc(DATA_SIZE);
	short total;
	short offset, length;
	catalogHandle.ReadPage(0, catalog_buffer);
	getTotalEntries(catalog_buffer, total);
	string fileName = tableName + ".data";
	for (short index = 1; index <= total; index++) {

		getSpecificOffset(catalog_buffer, index, offset);
		if (offset == -1)
			continue;
		getSpecificLength(catalog_buffer, index, length);

		//get tuple
		memcpy((char*) tuple, (char*) catalog_buffer + offset, length);
		string table_Name, file_Name;
		getCatalogTuple(table_Name, file_Name, tuple);

		//find tableName in Catalog
		if (strcmp(table_Name.c_str(), tableName.c_str()) == 0) {
			if (IsFileExists(fileName) == true) {
				if (hasTable(tableName) == RC_SUCCESS) {
					cerr << "table included in database" << endl;
					return RC_FAIL;
				} else {
					openTable(tableName);
					return RC_SUCCESS;
				}

			} else {
				rc = fileManager->CreateFile(fileName.c_str());

				if (rc == RC_FAIL) {
					//cerr << "error create file" << endl;
					return RC_FAIL;
				}
				openTable(tableName);
				return RC_SUCCESS;
			}
		}
	}

	if (IsFileExists(fileName) == true)
	 rc = remove(fileName.c_str());
	if (rc == RC_FAIL) 	cerr << "Problem deleting "  << tableName << endl;
	rc = fileManager->CreateFile(fileName.c_str());

	if (rc == RC_FAIL) {
		//cerr << "error create file" << endl;
		return RC_FAIL;
	}

	PF_FileHandle tableFileHandle;
	openTable(tableName, tableFileHandle);

	void *datafile_buffer = malloc(PF_PAGE_SIZE);
	writeFreeSpaceInHeaderPage(datafile_buffer, 0, PF_PAGE_SIZE - 4);
	writeFreeSpaceInHeaderPage(datafile_buffer, 1, PF_PAGE_SIZE - 4);
	tableFileHandle.WritePage(0, datafile_buffer);
	createNewPage(datafile_buffer);
	tableFileHandle.WritePage(1, datafile_buffer);
	free(datafile_buffer);

	short tuple_size;
	short freeSpace;
	PageNum currentPage = 0;

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
		//printAttribute(attrs[i]);
		pass = false;
		do {
			if (currentPage == columnHandle.GetNumberOfPages()) {
				if (dirty) {
					columnHandle.WritePage(currentPage - 1, column_buffer);
				}
				createNewPage(column_buffer);
				freeSpace = PF_PAGE_SIZE - 4;
				writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
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
	short N, offset, length;
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
			if (offset == -1)
				continue;
			getSpecificLength(buffer, index, length);

			//get tuple
			memcpy((char*) tuple, (char*) buffer + offset, length);
			string table_Name, file_Name;
			getCatalogTuple(table_Name, file_Name, tuple);

			//find tableName in Catalog
			if (strcmp(table_Name.c_str(), tableName.c_str()) == 0) {
				//delete tuple in Catalog
				writeSpecificOffset(buffer, index, deletedOffset);

				catalogHandle.WritePage(i, buffer);
				//exit
				finished = true;
				index = N + 1;
			}
		}

	}

	// Xoa bang column tableName

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
			memcpy(&string_length, (char*) tuple + tuple_offset, sizeof(int));
			tuple_offset += sizeof(int);

			char *name = (char *) malloc(100);
			memcpy(name, (char *) tuple + tuple_offset, string_length);
			name[string_length] = '\0';

			//check if this column belong to tableName table

			if (strcmp(name, tableName.c_str()) == 0) {
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

	PF_FileHandle myFileHandle;

	getTableHandle(tableName, myFileHandle);

	void *content_buffer = malloc(PF_PAGE_SIZE);
	void *header_buffer = malloc(PF_PAGE_SIZE);
	void *temp_data = malloc(PF_PAGE_SIZE);

	myFileHandle.ReadPage(0, header_buffer);

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

	bool pass = false;
	short freeSpace;

	do {
		//cout << " Number of pages=" ;
		//cout << tableFileHandle.GetNumberOfPages() << endl;
		if (currentPage == myFileHandle.GetNumberOfPages()) {
			createNewPage(content_buffer);
			freeSpace = PF_PAGE_SIZE - 4;
			myFileHandle.WritePage(currentPage, content_buffer);
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
	myFileHandle.ReadPage(currentPage, content_buffer);
	writeTo(content_buffer, temp_data, tuple_size);

	short entries;
	getTotalEntries(content_buffer, entries);
	rid.slotNum = entries;
	rid.pageNum = currentPage;

	myFileHandle.WritePage(0, header_buffer);
	myFileHandle.WritePage(currentPage, content_buffer);

	free(temp_data);
	free(header_buffer);
	//free(content_buffer);

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
	for (PageNum i = pageNum - 1; i > 0; i--) {
		//read page i
		fileHandle.ReadPage(i, buffer);
		//read number of column tuple on page i
		getTotalEntries(buffer, N);
		//write -1 to all offset
		for (short j = 1; j <= N; j++) {
			writeSpecificOffset(buffer, j, offset);
		}
		fileHandle.WritePage(i, buffer);
	}
	return 0;
}

RC RM::deleteTuple(const string tableName, const RID &rid) {
	void * buffer = malloc(PF_PAGE_SIZE);
	PF_FileHandle fileHandle;
	short offset, length;
	short slot = (short) rid.slotNum;

	getTableHandle(tableName, fileHandle);

//read page rid.pageName contain tuple rid
	fileHandle.ReadPage(rid.pageNum, buffer);

	getSpecificOffset(buffer, slot, offset);
	if (offset == -1) {
		cerr << "already deleted" << endl;
		return RC_FAIL;
	}
	getSpecificLength(buffer, slot, length);
	if (length == TOMBSTONE) {
		RID newRID;
		short tempNum;
		memcpy(&tempNum, (char*) buffer + offset + unit,unit);
		newRID.pageNum = (unsigned) tempNum;
		memcpy(&tempNum, (char*) buffer + offset + unit + unit,unit);
		newRID.slotNum = (unsigned) tempNum;
		return deleteTuple(tableName, newRID);
	} else if (length < 0) {
		cerr << "not the real record" << endl;
		return RC_FAIL;
	}

//get tuple rid.slotNum and write -1 to offset of rid.slotNum
//offset of the slotNum's offset
	writeSpecificOffset(buffer, slot, -1);

//Nhu vay la da dat offset cua slotNum bang -1
	fileHandle.WritePage(rid.pageNum, buffer);

	free(buffer);
	return RC_SUCCESS;
}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {

	PF_FileHandle fileHandle;
	getTableHandle(tableName, fileHandle);

	void * buffer;
	buffer = malloc(PF_PAGE_SIZE);
	fileHandle.ReadPage(rid.pageNum, buffer);

	void *temp_data = malloc(PF_PAGE_SIZE);
	short length, offset, freeSpace, lastOffset, lastLength;

	getSpecificOffset(buffer, (short) rid.slotNum, offset);
	if (offset == -1) {

		return RC_FAIL;
	}
	getSpecificLength(buffer, (short) rid.slotNum, length);
	if (length == TOMBSTONE) {

		short temp;
		RID cloneRID;
		memcpy(&temp, (char*) buffer + offset + unit,unit);
		cloneRID.pageNum = (unsigned) temp;
		memcpy(&temp, (char*) buffer + offset + unit + unit,unit);
		cloneRID.slotNum = (unsigned) temp;
		return updateTuple(tableName, data, cloneRID);
	} else if (length < 0) {
		return RC_FAIL;
	}
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
		//copy data vao buffer
		writeSpecificLength(buffer, (short) rid.slotNum, (short) tuple_size);
		memcpy((char*) buffer + offset, temp_data, tuple_size);
		fileHandle.WritePage(rid.pageNum, buffer);
		free(buffer);
		free(temp_data);
		return RC_SUCCESS;
	} else {
		getFreeSpace(buffer, freeSpace);
		if (tuple_size <= freeSpace)
		//Chi can <= freeSpace, ko can + 4 vi ghi vao slot cu
				{
			//tao them mot tuple moi tuong ung voi data
			short total = 0;
			short tempTotal = 0;
			lastOffset = 0;
			lastLength = 0;
			getTotalEntries(buffer, total);
			if (total != 0) {
				tempTotal = total;

				bool pass = false;
				do {
					getSpecificOffset(buffer, tempTotal, lastOffset);
					if (lastOffset == -1) {
						tempTotal -= 1;
					} else {
						getSpecificLength(buffer, tempTotal, lastLength);
						if (lastLength == TOMBSTONE
						)
							lastLength = LENGTH_TOMBSTONE;
						pass = true;
					}

				} while (!pass);
			}
			short newOffset = lastOffset + lastLength;
			//ghi data vao lastOffset
			memcpy((char*) buffer + newOffset, temp_data, tuple_size);
			//update directory, write offset and length
			writeSpecificOffset(buffer, (short) rid.slotNum, newOffset);
			writeSpecificLength(buffer, (short) rid.slotNum, tuple_size);
			fileHandle.WritePage(rid.pageNum, buffer);
			free(buffer);
			free(temp_data);
			return RC_SUCCESS;
		} else {

			RID newRID;

			insertTuple(tableName, data, newRID);
			writeSpecificLength(buffer, (short) rid.slotNum, TOMBSTONE);
			memcpy((char*) buffer + offset + unit, (short*) &newRID.pageNum,unit);
			memcpy((char*) buffer + offset + unit + unit,(short*) &newRID.slotNum,unit);
			fileHandle.WritePage(rid.pageNum, buffer);
			fileHandle.ReadPage(newRID.pageNum, buffer);
			short slot = (short) newRID.slotNum;
			getSpecificLength(buffer, slot, length);
			length = length * (-1);
			writeSpecificLength(buffer, slot, length);
			fileHandle.WritePage(newRID.pageNum, buffer);
			free(buffer);
			free(temp_data);
			return RC_SUCCESS;
		}
	}
	free(buffer);
	free(temp_data);
	return RC_FAIL;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	PF_FileHandle fileHandle;
	getTableHandle(tableName, fileHandle);

	short length, offset;
	void * buffer = malloc(PF_PAGE_SIZE);

	fileHandle.ReadPage(rid.pageNum, buffer);

	getSpecificOffset(buffer, (short) rid.slotNum, offset);
	if (offset == -1) {
		memset(data, 0, 100);
		free(buffer);
		return RC_FAIL;
	} else {
		getSpecificLength(buffer, (short) rid.slotNum, length);

		//chop off 2 bytes of schema
		offset += unit;
		if (length == -6) {

			short temp;
			RID newRID;
			memcpy(&temp, (char*) buffer + offset, unit);
			newRID.pageNum = (unsigned) temp;
			offset += unit;
			memcpy(&temp, (char*) buffer + offset, unit);
			newRID.slotNum = (unsigned) temp;
			return readTuple(tableName, newRID, data);
		} else if (length < 0) {
			length = length * (-1);
			length -= unit;
			memcpy(data, (char*) buffer + offset, length);
			free(buffer);
			return RC_SUCCESS;
		}

		length -= unit;
		memcpy(data, (char*) buffer + offset, length);
		free(buffer);
		return RC_SUCCESS;
	}

}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data) {
	//get TableHAndle (tableName)
	PF_FileHandle fileHandle;
	vector<Attribute> attrs;
	Attribute attr;

	short offset, length;

	void * buffer = malloc(PF_PAGE_SIZE);
	void *temp = malloc(200);

	//get attributeName
	getTableHandle(tableName, fileHandle);
	fileHandle.ReadPage(rid.pageNum, buffer);

	getSpecificOffset(buffer, rid.slotNum, offset);
	if (offset == -1)
		return RC_FAIL;
	getSpecificLength(buffer, rid.slotNum, length);
	if (length == TOMBSTONE) {
		RID newRID;
		short tempNum;
		memcpy(&tempNum, (char*) buffer + offset + unit,unit);
		newRID.pageNum = (unsigned) tempNum;
		memcpy(&tempNum, (char*) buffer + offset + unit + unit,unit);
		newRID.slotNum = (unsigned) tempNum;
		return readAttribute(tableName, newRID, attributeName, data);
	}
	memcpy(temp, (char*) buffer + offset, length);
	short schema;
	memcpy(&schema, (char*) buffer, unit);
	unsigned temp_sch;

	getAttributesAndSchema(tableName, attrs, temp_sch);
	offset = 2;
	if (schema == (short) temp_sch) {

		int length_var_char;
		for (int i = 0; i < (int) attrs.size(); i++) {
			if (strcmp(attrs[i].name.c_str(), attributeName.c_str()) == RC_SUCCESS) {
				switch (attrs[i].type) {
				case TypeInt:
				case TypeReal:
					memcpy(data, (char*) temp + offset, 4);
					break;
				case TypeVarChar:
					memcpy(&length_var_char, (char*) temp + offset, 4);
					memcpy(data, (char*) temp + offset, length_var_char + 4);
					break;
				default:
					cerr << "Error decoding attrs[i]" << endl;
					break;
				}

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
	} else { // if schema is not the latest schema, do something else

	}
	free(buffer);
	free(temp);

	return RC_SUCCESS;

}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
	if (pageNumber == 0)
		return RC_FAIL;

	//read currentPage
	PF_FileHandle fileHandle;
	getTableHandle(tableName, fileHandle);

	void * buffer = malloc(PF_PAGE_SIZE);
	void * new_buffer = malloc(PF_PAGE_SIZE);
	void * data = malloc(DATA_SIZE);
	createNewPage(new_buffer);
	fileHandle.ReadPage(pageNumber, buffer);

	short N;

	short offset;

	short length;
	short freeSpace;

	//get total of entries
	getTotalEntries(buffer, N);
	for (unsigned i = 1; i <= (unsigned) N; i++) {

		getSpecificOffset(buffer, (short) i, offset);
		//check if it is not deleted
		if (offset != -1) {

			getSpecificLength(buffer, i, length);

			if (length != TOMBSTONE) {
				if (length < 0) {
					length = length * (-1);
				}
				memcpy(data, (char*) buffer + offset, length);
				writeTo(new_buffer, data, length);

			}

		}
	}
	getFreeSpace(new_buffer, freeSpace);
	fileHandle.WritePage(pageNumber, new_buffer);
	fileHandle.ReadPage(0, buffer);
	writeFreeSpaceInHeaderPage(buffer, (short) pageNumber, freeSpace);
	fileHandle.WritePage(0, buffer);

	free(buffer);
	free(new_buffer);
	free(data);

	return RC_SUCCESS;
}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value, // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RM_ScanIterator &rm_ScanIterator) {

	openTable(tableName, rm_ScanIterator.dataFileHandle);
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	memcpy((char*) rm_ScanIterator.tableName.c_str(), (char*) tableName.c_str(), tableName.length());
	rm_ScanIterator.tableName[tableName.length()] = '\0';

	rm_ScanIterator.cond_position = 0;
	bool out = false;
	int size;
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (!out && strcmp(attrs[i].name.c_str(), conditionAttribute.c_str()) == RC_SUCCESS) {
			rm_ScanIterator.cond_position = attrs[i].position;
			out = true;
			size = attrs[i].length + 4;
		}
		for (unsigned j = 0; j < attributeNames.size(); j++) {
			if (strcmp(attrs[i].name.c_str(), attributeNames[j].c_str()) == RC_SUCCESS) {
				rm_ScanIterator.projected_position.push_back(attrs[i].position);

			}

		}
	}
	if (value != NULL) {
		rm_ScanIterator.value = malloc(size);
		memcpy(rm_ScanIterator.value, (char*) value, size);
	}

	rm_ScanIterator.operation = compOp;

	return RC_SUCCESS;

}

// Extra credit

RC RM::dropAttribute(const string tableName, const string attributeName) {
	vector<Attribute> oldAttributes;
	unsigned latest_schema;
	getAttributesAndSchema(tableName, oldAttributes, latest_schema);

	void * data = malloc(DATA_SIZE);
	void *content_buffer = malloc(PF_PAGE_SIZE);
	void *header_buffer = malloc(PF_PAGE_SIZE);

	short length;
	latest_schema++;

	columnHandle.ReadPage(0, header_buffer);
	unsigned totalRuns = oldAttributes.size();

	for (unsigned i = 0; i < totalRuns; i++) {
		if (strcmp(attributeName.c_str(), oldAttributes[i].name.c_str()) != RC_SUCCESS) {

			prepareColumnTuple(latest_schema, tableName, oldAttributes[i], oldAttributes[i].position, data, &length);

			PageNum currentPage = 1;
			bool pass = false;
			short freeSpace;

			do {

				if (currentPage == columnHandle.GetNumberOfPages()) {
					createNewPage(content_buffer);
					freeSpace = PF_PAGE_SIZE - 4;
					columnHandle.WritePage(currentPage, content_buffer);
					writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
				}
				getFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
				if (freeSpace < length + 4) {
					currentPage++;
				} else {
					pass = true;
				}
			} while (!pass);

			freeSpace = freeSpace - length - 4;
			writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
			columnHandle.ReadPage(currentPage, content_buffer);
			writeTo(content_buffer, data, length);

			columnHandle.WritePage(0, header_buffer);
			columnHandle.WritePage(currentPage, content_buffer);

		}
	}
	free(data);
	free(header_buffer);
	free(content_buffer);
	return RC_SUCCESS;
}

RC RM::addAttribute(const string tableName, const Attribute attr) {
	vector<Attribute> oldAttributes;
	unsigned latest_schema;
	getAttributesAndSchema(tableName, oldAttributes, latest_schema);

	void * data = malloc(DATA_SIZE);
	void *content_buffer = malloc(PF_PAGE_SIZE);
	void *header_buffer = malloc(PF_PAGE_SIZE);

	short length;
	latest_schema++;
	unsigned max_position = 0;
	unsigned temp;
	columnHandle.ReadPage(0, header_buffer);
	unsigned totalRuns = oldAttributes.size();
	totalRuns++;
	for (unsigned i = 0; i < totalRuns; i++) {
		if (i == totalRuns - 1) {
			max_position++;
			prepareColumnTuple(latest_schema, tableName, attr, max_position, data, &length);
		} else {
			temp = oldAttributes[i].position;
			if (temp > max_position)
				max_position = temp;

			prepareColumnTuple(latest_schema, tableName, oldAttributes[i], temp, data, &length);
		}

		PageNum currentPage = 1;
		bool pass = false;
		short freeSpace;

		do {
			//cout << " Number of pages=" ;
			//cout << tableFileHandle.GetNumberOfPages() << endl;
			if (currentPage == columnHandle.GetNumberOfPages()) {
				createNewPage(content_buffer);
				freeSpace = PF_PAGE_SIZE - 4;
				columnHandle.WritePage(currentPage, content_buffer);
				writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
			}
			getFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
			if (freeSpace < length + 4) {
				currentPage++;
			} else {
				pass = true;
			}
		} while (!pass);

		freeSpace = freeSpace - length - 4;
		writeFreeSpaceInHeaderPage(header_buffer, currentPage, freeSpace);
		columnHandle.ReadPage(currentPage, content_buffer);
		writeTo(content_buffer, data, length);

		columnHandle.WritePage(0, header_buffer);
		columnHandle.WritePage(currentPage, content_buffer);

	}
	free(data);
	free(header_buffer);
	free(content_buffer);
	return RC_SUCCESS;
}

RC RM::reorganizeTable(const string tableName) {

	PF_FileHandle tableFileHandle;
	getTableHandle(tableName, tableFileHandle);


	unsigned totalRuns = tableFileHandle.GetNumberOfPages();

	for (unsigned i = 1; i < totalRuns; i++) {
		reorganizePage(tableName, i);

	}
	return RC_SUCCESS;
}

RM_ScanIterator::RM_ScanIterator() {
	currentPage = 1;
	currentSlot = 1;
	switch_page = true;
	gotAttributes = false;
	latest_schema = 1;
	buffer = malloc(PF_PAGE_SIZE);

}

RM_ScanIterator::~RM_ScanIterator() {
	free(buffer);
}

bool isIn(unsigned int num, vector<unsigned int> &vector_int) {
	for (unsigned i = 0; i < vector_int.size(); i++) {
		if (num == vector_int[i])
			return true;
	}
	return false;
}

RC RM_ScanIterator::grabValidTuple(void * source, short slotNumber, void * data_returned, bool inSearch) {

	short offset;
	short length;
	RC rc;
	getSpecificOffset(source, slotNumber, offset);
	if (offset == -1) {
		memset(data_returned, 0, 100);
		return RC_FAIL;
	} else {
		getSpecificLength(source, slotNumber, length);
		if (length == TOMBSTONE) { //find the new location
			short tempNum;
			memcpy(&tempNum, (char*) source + offset + unit,unit);
			void * temp_buffer = malloc(PF_PAGE_SIZE);
			dataFileHandle.ReadPage((unsigned) tempNum, temp_buffer);
			memcpy(&tempNum, (char*) buffer + offset + unit + unit,unit);
			rc = grabValidTuple(temp_buffer, tempNum, data_returned, true);
			free(temp_buffer);
			return rc;
		} else if (length > 0) {
			memcpy(data_returned, (char*) source + offset, length);
			return RC_SUCCESS;
		} else {
			if (inSearch == true) {
				memcpy(data_returned, (char*) source + offset, (length * -1));
				return RC_SUCCESS;
			} else {
				memset(data_returned, 0, 100);
				return RC_FAIL;
			}
		}
	}
}

// "data" follows the same format as RM::insertTuple()

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	RM* recordManager = RM::Instance();
	if (currentPage == (short) dataFileHandle.GetNumberOfPages())
		return EOF;

	bool ok = false;

	short total;

	bool pass = false;

	do {
		ok = false;
		do {
			if (switch_page) {
				if (currentPage == (short) dataFileHandle.GetNumberOfPages())
					return EOF;

				dataFileHandle.ReadPage(currentPage, buffer);
				switch_page = false;
			}
			getTotalEntries(buffer, total);
			if (currentSlot <= total)
				ok = true;
			else {
				currentPage++;
				switch_page = true;
				currentSlot = 1;
			}
		} while (!ok);

		RC rc;
		void * tuple = malloc(PF_PAGE_SIZE);

		rc = grabValidTuple(buffer, currentSlot, tuple, false);
		if (rc != RC_SUCCESS) {
			currentSlot++;
		} else {

			short schema;
			short data_offset;
			unsigned temp_latest_schema;
			memcpy(&schema, (char*) tuple, unit);
			data_offset = 2;

			int value_size;
			AttrType value_type;
			void * tuple_value;

			if (schema == latest_schema) { // no changing in schema
				if (!gotAttributes) {
					recordManager->getAttributesAndSchema(tableName, attributes, temp_latest_schema);
					latest_schema = (short) temp_latest_schema;
					gotAttributes = true;
				}
			} else {
				gotAttributes = false;

				latest_schema = schema;
				recordManager->getAttributesOfSchema(tableName, attributes, (unsigned) schema);

			}
			RC rc = RC_SUCCESS;
			if (cond_position != 0) { // no condition

				bool condition_found = false;
				for (unsigned i = 0; i < attributes.size(); i++) {
					if (!condition_found && attributes[i].position == cond_position) {
						value_type = attributes[i].type;
						condition_found = true;
						switch (attributes[i].type) {
						case TypeInt:
						case TypeReal:
							value_size = 4;

							break;
						case TypeVarChar:
							memcpy(&value_size, (char*) tuple + data_offset, 4);
							value_size += 4;
							break;
						}
						tuple_value = malloc(value_size);
						memcpy(tuple_value, (char*) tuple + data_offset, value_size);

					} else {
						switch (attributes[i].type) {
						case TypeInt:
						case TypeReal:
							data_offset += 4;
							break;
						case TypeVarChar:
							memcpy(&value_size, (char*) tuple + data_offset, 4);
							data_offset = data_offset + 4 + (short) value_size;
							break;
						}
					}
				}
				if (condition_found) {
					// do compare
					rc = compare(tuple_value, value, value_size, value_type);
				} else {
					rc = RC_FAIL;
				}
			}
			if (rc == RC_SUCCESS) {
				void * result = malloc(PF_PAGE_SIZE);

				int result_offset = 0;
				data_offset = 2;
				for (unsigned i = 0; i < attributes.size(); i++) {
					if (isIn(attributes[i].position, projected_position) == true) {
						switch (attributes[i].type) {
						case TypeInt:
						case TypeReal:
							value_size = 4;
							break;
						case TypeVarChar:
							memcpy(&value_size, (char*) tuple + data_offset, 4);
							value_size += 4;
							break;
						}
						memcpy((char*) result + result_offset, (char*) tuple + data_offset, value_size);
						data_offset += value_size;
						result_offset += value_size;

					} else {
						switch (attributes[i].type) {
						case TypeInt:
						case TypeReal:
							data_offset += 4;
							break;
						case TypeVarChar:
							memcpy(&value_size, (char*) tuple + data_offset, 4);
							data_offset = data_offset + 4 + (short) value_size;
							break;
						}
					}
				}
				rid.pageNum = currentPage;
				rid.slotNum = currentSlot;
				currentSlot++;
				memcpy(data, result, result_offset);
				pass = true;
			} else
				currentSlot++;

		}

	} while (!pass);
	return RC_SUCCESS;
}

RC RM_ScanIterator::compare(void *tuple, void* value, int tuple_size, AttrType type) {
	switch (type) {
	case TypeInt:
		int int1;
		memcpy(&int1, (char*) tuple, tuple_size);
		int int2;
		memcpy(&int2, (char*) value, tuple_size);
		switch (operation) {
		case EQ_OP:
			if (int1 == int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LT_OP:
			if (int1 < int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GT_OP:
			if (int1 > int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LE_OP:
			if (int1 <= int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GE_OP:
			if (int1 >= int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NE_OP:
			if (int1 != int2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NO_OP:
			return RC_SUCCESS;
			break;

		}
		break;
	case TypeReal:
		float real1;
		memcpy(&real1, (char*) tuple, tuple_size);
		float real2;
		memcpy(&real2, (char*) value, tuple_size);
		switch (operation) {
		case EQ_OP:
			if (real1 == real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LT_OP:
			if (real1 < real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GT_OP:
			if (real1 > real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LE_OP:
			if (real1 <= real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GE_OP:
			if (real1 >= real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NE_OP:
			if (real1 != real2)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NO_OP:
			return RC_SUCCESS;
			break;

		}
		break;
	case TypeVarChar:
		int len1;
		memcpy(&len1, (char*) tuple, 4);
		int len2;
		memcpy(&len2, (char*) value, 4);
		char* str1 = (char*) malloc(len1);
		memcpy(str1, (char*) tuple, len1);
		str1[len1] = '\0';
		char* str2 = (char*) malloc(len2);
		memcpy(str2, (char*) value, len2);
		str2[len2] = '\0';
		int cmp_ret = strcmp(str1, str2);
		free(str1);
		free(str2);
		switch (operation) {
		case EQ_OP:
			if (cmp_ret == RC_SUCCESS
			)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LT_OP:
			if (cmp_ret < RC_SUCCESS
			)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GT_OP:
			if (cmp_ret > RC_SUCCESS
			)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case LE_OP:
			if (cmp_ret <= RC_SUCCESS
			)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case GE_OP:
			if (cmp_ret >= RC_SUCCESS
			)
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NE_OP:
			if ((cmp_ret != RC_SUCCESS))
				return RC_SUCCESS;
			else
				return RC_FAIL;
			break;
		case NO_OP:
			return RC_SUCCESS;
			break;

		}
		break;
	}
	return RC_FAIL;
}
RC RM_ScanIterator::close() {
	projected_position.clear();
	attributes.clear();
	switch_page = true;
	gotAttributes = false;
	currentPage = 1;
	currentSlot = 1;

	free(value);
	return RC_SUCCESS;
}

