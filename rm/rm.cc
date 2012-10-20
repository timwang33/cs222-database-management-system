#include "rm.h"

using namespace std;
RM* RM::_rm = 0;

RM* RM::Instance() {
	if (!_rm)
		_rm = new RM();

	return _rm;
}

void prepareCatalogTuple(const string name, const string filename, void *buffer,
		int *tuple_size) {
	int offset = 0;

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

RM::RM() {

	fileManager = PF_Manager::Instance();
	remove(catalog_file_name);
	//open catalog files
	struct stat stFileInfo;

	if (stat(catalog_file_name, &stFileInfo) != 0) { //catalog not exist
		fileManager->CreateFile(catalog_file_name);
		fileManager->OpenFile(catalog_file_name, catalogHandle);
		int tuple_size = 0;
		void *tuple = malloc(100);
		prepareCatalogTuple("Catalog", catalog_file_name, tuple, &tuple_size);

		void *buffer = malloc(PF_PAGE_SIZE);
		int free_space = PF_PAGE_SIZE - tuple_size-8;
		memcpy((char *) buffer, tuple, tuple_size);

		unsigned short total_entry = 1;

		cout << total_entry << endl;
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
		catalogHandle.WritePage(0, buffer);
		fileManager->CloseFile(catalogHandle);
	}
	fileManager->OpenFile(catalog_file_name, catalogHandle);

	if (stat(column_file_name, &stFileInfo) != 0) { //column not exist
		fileManager->CreateFile(column_file_name);
		fileManager->OpenFile(column_file_name, columnHandle);


		Attribute attr;
		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		catalogAttrs.push_back(attr);

		attr.name = "FileName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 50;
		catalogAttrs.push_back(attr);

		attr.name = "Schema";
		attr.type = TypeInt;
		attr.length = 4;
		columnAttrs.push_back(attr);

		attr.name = "ColName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 20;
		columnAttrs.push_back(attr);

		attr.name = "TableName";
		attr.type = TypeVarChar;
		attr.length = (AttrLength) 30;
		columnAttrs.push_back(attr);

		attr.name = "Type";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);

		attr.name = "Length";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);

		attr.name = "Position";
		attr.type = TypeInt;
		attr.length = (AttrLength) 4;
		columnAttrs.push_back(attr);
	}
	fileManager->OpenFile(column_file_name, columnHandle);
}

RM::~RM() {
	fileManager->CloseFile(columnHandle);
	fileManager->CloseFile(catalogHandle);
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {
	if (tableName.compare(catalog_file_name) == 0) {
		//making catalog file
		return RC_SUCCESS;
	}

}

RC RM::deleteTable(const string tableName) {

}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {

}

//  Format of the data passed into the function is the following:
//  1) data is a concatenation of values of the attributes
//  2) For int and real: use 4 bytes to store the value;
//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
RC RM::insertTuple(const string tableName, const void *data, RID &rid) {

}

RC RM::deleteTuples(const string tableName) {

}

RC RM::deleteTuple(const string tableName, const RID &rid) {

}

// Assume the rid does not change after update
RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {

}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {

}

RC RM::readAttribute(const string tableName, const RID &rid,
		const string attributeName, void *data) {

}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {

}

// scan returns an iterator to allow the caller to go through the results one by one.
RC RM::scan(const string tableName, const string conditionAttribute,
		const CompOp compOp, // comparision type such as "<" and "="
		const void *value, // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RM_ScanIterator &rm_ScanIterator) {

}

// Extra credit

RC RM::dropAttribute(const string tableName, const string attributeName) {

}

RC RM::addAttribute(const string tableName, const Attribute attr) {

}

RC RM::reorganizeTable(const string tableName) {

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

