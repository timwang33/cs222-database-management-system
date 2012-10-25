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

RC getColumnTupleOfSchema(short schema, const string tableName, Attribute *attr,
		unsigned *position, void *buffer, short lengthTuple) {
	short offset = 0;
	string columnName;
	AttrType columnType;
	AttrLength columnLength;
	string table_Name;
	short version;
	memcpy(&version, (char*) buffer, sizeof(short));
	if (version != schema) return RC_FAIL;
	offset += sizeof(short);


	int string_length;
	memcpy(&string_length, (char*) buffer + offset, sizeof(int));
	offset += sizeof(int);

	memcpy((char*) table_Name.c_str(), (char*) buffer + offset, string_length);
	offset += string_length;

	//check if this column belong to tableName table

	if (!strcmp(table_Name.c_str(), tableName.c_str()))
		return RC_FAIL;
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
/*
RC RM::getAttributeFromColumnFile(void * data, void * result, int index) {

	int length_var_char;
	Attribute attr = columnAttrs[index];
	int offset = 0;
	for (unsigned i = 0; i < index; i++) {
		switch (columnAttrs[i].type) {
		case TypeInt:
		case TypeReal:
			offset += 4;
			break;
		case TypeVarChar:
			memcpy(&length_var_char, (char*) data, 4);
			offset += 4 + length_var_char;
			break;
		}
	}

}*/



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

RC RM::getAttributesAndSchema(const string tableName, vector<Attribute> &attrs, int &schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	int offset = PF_PAGE_SIZE;
	int length;
	int N;
	int freeSpace;
	RC rc = -1;

	Attribute attr;
	unsigned schema_Column = 0, old_Schema_Column, position_Column = 0;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	int index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i >= 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, &N);

		//freeSpace of page i
		getFreeSpace(buffer, &freeSpace);

		index = N;
		while (index > 0 || !finished) {
			//get last tuple of table Name from page i to have initial schema, it may not be N tuple
			getSpecificOffset(buffer, index, &offset);
			getSpecificLength(buffer, index, &length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			rc = getColumnTuple(&schema_Column, tableName, &attr,&position_Column, tuple, length);

			if (rc == RC_SUCCESS)	attrs.push_back(attr);
			if (attrs.size() == 1)			{	old_Schema_Column = schema_Column; schema = schema_Column;}
			if (attrs.size() >= 2 && old_Schema_Column != schema_Column) 				finished = true;
			index--;
		}

	}
	reverse(attrs.begin(), attrs.end());
	return 0;

}

RC RM::getAttributesOfSchema(const string tableName, vector<Attribute> &attrs, short schema) {

	//buffer contain page i starts from page 1
	//offset of directory
	int offset = PF_PAGE_SIZE;
	int length, offsetColumnTuple;
	int N;
	int freeSpace;
	RC rc = -1;
	int lastTuple;
	Attribute attr;
	unsigned schema_Column = 0, old_Schema_Column, position_Column = 0;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	int index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i >= 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, &N);

		//freeSpace of page i
		getFreeSpace(buffer, &freeSpace);

		index = N;
		while (index > 0 || !finished) {
			//get last tuple of table Name from page i to have initial schema, it may not be N tuple
			getSpecificOffset(buffer, index, &offset);
			getSpecificLength(buffer, index, &length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			rc = getColumnTupleOfSchema(schema, tableName, &attr,&position_Column, tuple, length);

			if (rc == RC_SUCCESS)	attrs.push_back(attr);
			if (attrs.size() == 1)			{	old_Schema_Column = schema_Column; schema = schema_Column;}
			if (attrs.size() >= 2 && old_Schema_Column != schema_Column) 				finished = true;
			//if (position_Column ==1 && rc == RC_SUCCESS) finished ==true
			index--;
		}

	}
	reverse(attrs.begin(), attrs.end());
	return 0;

}


RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {

	//buffer contain page i starts from page 1
	//offset of directory
	int offset = PF_PAGE_SIZE;
	int length;
	int N;
	int freeSpace;
	RC rc = -1;

	Attribute attr;
	unsigned schema_Column = 0, old_Schema_Column, position_Column = 0;

	void *buffer = malloc(PF_PAGE_SIZE);

	PageNum pageNum = columnHandle.GetNumberOfPages();

	void * tuple = malloc(PF_PAGE_SIZE);
	int index;
	//traverse bottom-up
	bool finished = false;
	for (unsigned i = pageNum - 1; i >= 0; i--) {
		if (finished)
			break;
		//read page i
		columnHandle.ReadPage(i, buffer);
		//read number of column tuple on page i

		getTotalEntries(buffer, &N);

		//freeSpace of page i
		getFreeSpace(buffer, &freeSpace);

		index = N;
		while (index > 0 || !finished) {
			//get last tuple of table Name from page i to have initial schema, it may not be N tuple
			getSpecificOffset(buffer, index, &offset);
			getSpecificLength(buffer, index, &length);

			//get tuple

			memcpy((char*) tuple, (char*) buffer + offset, length);

			rc = getColumnTuple(&schema_Column, tableName, &attr,&position_Column, tuple, length);

			if (rc == RC_SUCCESS)	attrs.push_back(attr);
			if (attrs.size() == 1)				old_Schema_Column = schema_Column;
			if (attrs.size() >= 2 && old_Schema_Column != schema_Column) 				finished = true;
			index--;
		}

	}
	reverse(attrs.begin(), attrs.end());
	return 0;

}
RC RM::getLatestSchema(const string tableName, int* schema) {
//buffer contain page i starts from page 1
//offset of directory
	int offset = PF_PAGE_SIZE;
	short N, length, offsetColumnTuple, freeSpace;
	RC rc = -1;
	int lastTuple;
	Attribute attr;
	unsigned schema_Column = 0, position_Column = 0;


	void *buffer;
	buffer = malloc(PF_PAGE_SIZE);


	PageNum pageNum = columnHandle.GetNumberOfPages();

//traverse bottom-up
	for (unsigned i = pageNum - 1; i >= 0; i--) {
		if (rc == RC_SUCCESS) break;
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
			// update Schema
		}
		if (schema_Column != 0) {
			*schema = schema_Column;
		}


	}

	free(buffer);
	return rc;
}

void insertSchema(void * tuple, int schema, int tuple_size) {
	void *temp;
	temp = malloc(100);

//write schema to the begin of temp
	memcpy((char*) temp, (short*)&schema, unit);
//write temp_data to temp, from byte 5th to (byte (5 + tuple_size)th
	memcpy((char*) temp + unit, (char*) tuple, tuple_size);

	memcpy((char*) tuple, (char*) temp, tuple_size + unit);

	free(temp);
}

void getDataSize(void *data, vector<Attribute> attrs, int *tuple_size,
		bool hasSchema) {
	int length_var_char;

	int offset;

	if (hasSchema) {
		offset = 2;

	} else {
		offset = 0;

	}

	for (unsigned i=0; i < attrs.size(); i++) {
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
	return RC_SUCCESS;
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

	PF_FileHandle tableFileHandle;

	getTableHandle(tableName, tableFileHandle);

	void *content_buffer = malloc(PF_PAGE_SIZE);
	void *header_buffer = malloc(PF_PAGE_SIZE);
	void *temp_data = malloc(PF_PAGE_SIZE);
	tableFileHandle.ReadPage(0, header_buffer);
	PageNum N = tableFileHandle.GetNumberOfPages();
	PageNum currentPage;
	int schema=0;
	vector<Attribute> tableAttributes;

	getAttributesAndSchema(tableName, tableAttributes, schema);


	int tuple_size=0;

    getDataSize((void*)data,tableAttributes,&tuple_size,false);

    //write schema to the begin of temp_data, which takes 2 bytes
    	memcpy((char*) temp_data, &schema, unit);
    //write data to temp_data, from byte 3th to (byte (3 + tuple_size)th
    	memcpy((char*) temp_data + unit, (char*) data, tuple_size);

	tuple_size += 2;
	currentPage = 1;
	bool dirty = false;
	bool pass = false;
	int freeSpace;

	do {
		if (currentPage == N) {
			if (dirty) {
				tableFileHandle.WritePage(currentPage - 1, content_buffer);
			}
			createNewPage(content_buffer);
			freeSpace = PF_PAGE_SIZE - 4;
			writeFreeSpaceInHeaderPage(header_buffer, currentPage, &freeSpace);
		}
		getFreeSpaceInHeaderPage(header_buffer, currentPage, &freeSpace);
		if (freeSpace < tuple_size + 4) {
			if (dirty) {
				tableFileHandle.WritePage(currentPage, content_buffer);
			}
			currentPage++;

		} else {
			pass = true;
		}
	} while (!pass);

	freeSpace = freeSpace - tuple_size - 4;
	writeFreeSpaceInHeaderPage(header_buffer, currentPage, &freeSpace);
	tableFileHandle.ReadPage(currentPage, content_buffer);
	writeTo(content_buffer, temp_data, tuple_size);
	dirty = true;

	tableFileHandle.WritePage(0, header_buffer);
	tableFileHandle.WritePage(currentPage, content_buffer);

	free(temp_data);
	free(header_buffer);
	free(content_buffer);

	return RC_SUCCESS;
}

RC RM::deleteTuples(const string tableName) {
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
	writeSpecificOffset(buffer, (int) rid.slotNum, (int*) &offset);

//Nhu vay la da dat offset cua slotNum bang -1

	fileHandle.WritePage(rid.pageNum, buffer);

	free(buffer);
	return 0;
}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	//get TableHAndle (tableName)
	// read tuple rid
	//memcpy(data,tuple)
	return 0;
}

RC RM::readAttribute(const string tableName, const RID &rid,
		const string attributeName, void *data) {

	//get TableHAndle (tableName)
	// read tuple rid
	//get Vector<Attribute>
	// compare with attributeName, find the offset & size of attribute
	// memcpy(data, tuple+offset,size

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
	RecordManager = RM::Instance();
}

RM_ScanIterator::~RM_ScanIterator() {

}

// "data" follows the same format as RM::insertTuple()

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	if (next == (result.size() -1)) return RM_EOF;

	rid = result[next];
	PF_FileHandle fileHandle;
	RecordManager->getTableHandle(tableName,fileHandle);

	void * buffer = malloc (PF_PAGE_SIZE);
	fileHandle.ReadPage(rid.pageNum, buffer);
	int offset, length;
	getSpecificOffset(buffer,rid.slotNum,&offset);
	getSpecificLength(buffer,rid.slotNum,&length);

	void * tuple = malloc (PF_PAGE_SIZE);


	vector<Attribute> attributes;
	RecordManager->getAttributesOfSchema(tableName,attributes,);
	return RM_EOF;
}

RC RM_ScanIterator::close() {
	return -1;
}

