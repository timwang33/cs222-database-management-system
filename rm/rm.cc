
#include "rm.h"


RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	_pf = PF_Manager::Instance();

}

RM::~RM()
{

}
RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {

 }

  RC RM:: deleteTable(const string tableName) {

  }

  RC RM::getAttributes(const string tableName, vector<Attribute> &attrs){

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
  RC RM::updateTuple(const string tableName, const void *data, const RID &rid){

  }

  RC RM::readTuple(const string tableName, const RID &rid, void *data) {

  }

  RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data){

  }

  RC RM::reorganizePage(const string tableName, const unsigned pageNumber){

  }

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC RM::scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
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



	 RM_ScanIterator::RM_ScanIterator() {};
	 RM_ScanIterator::~RM_ScanIterator() {};

    // "data" follows the same format as RM::insertTuple()
    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; };
    RC RM_ScanIterator::close() { return -1; };
