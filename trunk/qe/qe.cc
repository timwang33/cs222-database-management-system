# include "qe.h"
#include<algorithm>
#include <sstream>
#include <math.h>
#include <stdlib.h>

const int dataSize = 200;
//compare 2 memory
int compare(void *data1, void *data2, AttrType type) {
	short rc;
	int int1, int2;
	float real1, real2;
	int length1, length2;
	string str1, str2;

	//Compare data1, data2 with length = length
	switch (type) {
	case TypeInt:
		memcpy(&int1, data1, 4);
		memcpy(&int2, data2, 4);
		if (int1 < int2)
			rc = -1;
		else if (int1 == int2)
			rc = 0;
		else
			rc = 1;
		break;

	case TypeReal:
		memcpy(&real1, data1, 4);
		memcpy(&real2, data2, 4);
		if (real1 < real2)
			rc = -1;
		else if (real1 == real2)
			rc = 0;
		else
			rc = 1;
		break;
	case TypeVarChar:
		memcpy(&length1, data1, 4);
		memcpy(&length2, data2, 4);

		memcpy((char*) str1.c_str(), (char*) data1 + 4, length1);
		memcpy((char*) str2.c_str(), (char*) data2 + 4, length2);
		rc = strcmp(str1.c_str(), str2.c_str());
		break;

	default:
		cerr << "Type is not correct!";
		break;
	}
	return rc;
}

//get attribute data
void getAttributeData(const vector<Attribute> attrs, const string attr, const void *data, void *attrData) {
	short offset = 0;
	int length_var_char;
	for (int i = 0; i < (int) attrs.size(); i++) {
		if (strcmp(attrs[i].name.c_str(), attr.c_str()) == RC_SUCCESS)
		{
			switch (attrs[i].type) {
			case TypeInt:
			case TypeReal:
				memcpy(attrData, (char*) data + offset, 4);
				return;
				break;
			case TypeVarChar:
				memcpy(&length_var_char, (char*) data +offset, 4);
				memcpy((char*)attrData, (char*) data +offset, length_var_char+4 );
				//copy string & length of string
				return;
				break;
			default:
				cerr << "Error decoding attrs[i]" << endl;
				break;
			}
		} else {

			switch (attrs[i].type) {
			case TypeInt:
			case TypeReal:
				offset += 4;
				break;
			case TypeVarChar:
				memcpy(&length_var_char, (char*) data + offset, 4);
				offset = offset + 4 + length_var_char;
				break;
			default:
				cerr << "Error decoding attrs[i]" << endl;
				break;
			}

		}
	}

}

//get size of tuple
void getSizeOfTuple(void *data, vector<Attribute> attrs, short &size) {
	int count = 0;
	int length_var_char;
	for (unsigned i = 0; i < attrs.size(); i++) {
		switch (attrs[i].type) {
		case TypeInt:
		case TypeReal:
			count += 4;
			break;
		case TypeVarChar:
			memcpy(&length_var_char, (char*) data + count, 4);
			count = count + 4 + length_var_char;
			break;
		default:
			cerr << "Error decoding attrs[i]" << endl;
			break;
		}
	}
	size = count;
	return;
}
//get attribute from name
Attribute getAttributeFromName(const vector<Attribute> attrs, const string attrName) {
	Attribute attr;
	for (int i = 0; i < (int) attrs.size(); i++)
		if (strcmp(attrs[i].name.c_str(), attrName.c_str()) == RC_SUCCESS)
		{
			attr = attrs[i];
			break;
		}
	return attr;

}
//size of attribute
RC sizeOfAttribute(void *data, Attribute attr, short &size) {
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
		cerr << "Error decoding attrs" << endl;
		break;
	}
	size = count;
	return 0;
}

Filter::~Filter() {

}

RC Filter::getNextTuple(void *data) {

	void *temp = malloc(200);
	void *attrData = malloc(200); //keep data of Attribute

	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	CompOp operation = condition.op;
	int rc;

	while (true) {
		memset(temp, 0, 200);
		memset(attrData, 0, 200);
		rc = input->getNextTuple(temp);
		if (rc == RC_FAIL)
		{
			goto fail;
		}
		if (operation == NO_OP) {
			goto end;
		}

		//get data of leftAttribute
		getAttributeData(attrs, leftAttrName, temp, attrData);
		//get leftAttribute all information
		LeftAttr = getAttributeFromName(attrs, leftAttrName);

		//filter data
		if (condition.bRhsIsAttr == false) {
			Value rightValue;
			int compareValue;
			//compare with a value
			rightValue = condition.rhsValue;
			if (LeftAttr.type == rightValue.type) {
				compareValue = compare(attrData, rightValue.data, LeftAttr.type);
				//Type is compatible
				switch (operation) {
				case EQ_OP:
					if (compareValue == 0) {
						goto end;
					}
					break;
				case LT_OP:
					if (compareValue == -1) {
						goto end;
					}
					break;
				case GT_OP:
					if (compareValue == 1) {
						goto end;
					}
					break;
				case LE_OP:
					if (compareValue != 1) {
						goto end;
					}
					break;
				case GE_OP:
					if (compareValue != -1) {
						goto end;
					}
					break;
				case NE_OP:
					if (compareValue != 0) {
						goto end;
					}
					break;
				default:
					goto fail;

				} // end of switch

			} else {
				cout << "Type of 2 sides are not compatible!!";
				goto fail;
			}

		} else {
			cout << "Right side cannot be an Attribute";
			goto fail;

		}
	} // end of while

	end: memcpy(data, temp, 200);
	free(temp);
	free(attrData);
	return RC_SUCCESS;
	fail: free(temp);
	free(attrData);
	return RC_FAIL;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;

}

//Projection
Project::~Project() {

}

void Project::getAttributes(vector<Attribute> &projectAttrs) const {
	projectAttrs.clear();
	Attribute attr;
	vector<Attribute> attrs;
	attrs = this->attrs;

	for (unsigned i = 0; i < this->projectAttrNames.size(); i++) {
		attr = getAttributeFromName(attrs, projectAttrNames[i]);
		attr.position = i+1;
		projectAttrs.push_back(attr);
	}

}

RC Project::getNextTuple(void *data) {

	void *temp = malloc(500);
	int rc = input->getNextTuple(temp);
	if (rc != RC_SUCCESS) {
		free(temp);
		return rc;
	}
	void *projectData = malloc(100); //keep data of project Attribute
	Attribute projectAttr;
	short offset = 0;
	short size = 0;

	for (unsigned i = 0; i < this->projectAttrNames.size(); i++) {
		//get data of each attribute
		getAttributeData(attrs, projectAttrNames[i], temp, projectData);
		projectAttr = getAttributeFromName(this->attrs, projectAttrNames[i]);
		sizeOfAttribute(projectData, projectAttr, size);
		memcpy((char*) data + offset, projectData, size);
		offset += size;
	}
	free(temp);
	free(projectData);
	return RC_SUCCESS;
}

//NLJoin
NLJoin::~NLJoin() {
	if (leftTuple != NULL
	)
		free(leftTuple);
}

void copyData(void *leftData, void *rightData, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs, void *data) {
	short leftSize = 0;
	short rightSize = 0;

	getSizeOfTuple(leftData, leftAttrs, leftSize);
	getSizeOfTuple(rightData, rightAttrs, rightSize);
	memcpy(data, leftData, leftSize);
	memcpy((char*) data + leftSize, rightData, rightSize);
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	//contain all attributes
	attrs.clear();
	Attribute attr;
	unsigned count = 0;
	for (unsigned i = 0; i < this->leftAttrs.size(); i++) {
		attr = leftAttrs[i];
		count++;
		attr.position = count;
		attrs.push_back(attr);
	}
	for (unsigned i = 0; i < this->rightAttrs.size(); i++) {
		attr = rightAttrs[i];
		count++;
		attr.position = count;
		attrs.push_back(attr);
	}
}

RC NLJoin::getNextTuple(void *data) {
	//leftTuple da duoc luu trong NLJoin
	void *rightTuple = malloc(200);
	void *leftAttrData = malloc(200);
	void *rightAttrData = malloc(200);
	int rc1 = RC_SUCCESS;

	//Get Left Attribute
	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	LeftAttr = getAttributeFromName(this->leftAttrs, leftAttrName);
	//Get Right Attribute
	Attribute RightAttr;

	CompOp operation = condition.op;

	//Doc moi va endofFile = false, start right table

	while (true) {
		memset(rightTuple, 0, 200);
		memset(leftAttrData, 0, 200);
		memset(rightAttrData, 0, 200);

		if (endOftable == true) { // end of right table
			memset(this->leftTuple, 0, 200);
			rc1 = this->leftInput->getNextTuple(this->leftTuple);

			if (rc1 == RC_FAIL)
			{
				goto fail;
			}

			endOftable = false;
		}

		//get data of leftAttribute
		getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
		// leftInput van con => success va rightTuple van con endOffile => false

		int rc2 = this->rightInput->getNextTuple(rightTuple);
		if (rc2 == RC_FAIL)
		{
			endOftable = true;
			RestartIterator();
		} else {
			//join, not eliminate duplicate attributes
			if (condition.bRhsIsAttr == false) {
				cout << "Doesnt make sense to Join left table with Value" << endl;
				goto fail;
			} else {
				string rightAttrName = condition.rhsAttr;
				RightAttr = getAttributeFromName(this->rightAttrs, rightAttrName);
				//get data of rightAttribute
				getAttributeData(this->rightAttrs, rightAttrName, rightTuple, rightAttrData);

				//compare 2 attribute type and value
				int compareValue;

				if (LeftAttr.type == RightAttr.type) {
					compareValue = compare(leftAttrData, rightAttrData, LeftAttr.type);
					//Type is compatible
					switch (operation) {
					case NO_OP:
						goto success;
						break;
					case EQ_OP:
						if (compareValue == 0) {
							goto success;
						}
						break;
					case LT_OP:
						if (compareValue == -1) {
							goto success;
						}
						break;

					case GT_OP:
						if (compareValue == 1) {
							goto success;
						}
						break;
					case LE_OP:
						if (compareValue != 1) {
							goto success;
						}
						break;
					case GE_OP:
						if (compareValue != -1) {
							goto success;
						}
						break;

					case NE_OP:
						if (compareValue != 0) {
							goto success;
						}
						break;
					default:
						break;
					} //end of switch case

				} else // type of attributes are not compatible
				{
					cout << "Type of 2 sides are not compatible!!";
					goto fail;
				}

			} // end of compare

		} //end rc2 = RC_SUCCESS

	} // end of while

	success: copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
	free(rightTuple);
	free(leftAttrData);
	free(rightAttrData);
	return RC_SUCCESS;

	fail: free(rightTuple);
	free(leftAttrData);
	free(rightAttrData);
	return RC_FAIL;
}

INLJoin::~INLJoin() {
	if (leftTuple != NULL
	)
		free(leftTuple);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	//contain all attributes
	attrs.clear();
	Attribute attr;
	unsigned count = 0;
	for (unsigned i = 0; i < this->leftAttrs.size(); i++) {
		attr = leftAttrs[i];
		count++;
		attr.position = count;
		attrs.push_back(attr);
	}
	for (unsigned i = 0; i < this->rightAttrs.size(); i++) {
		attr = rightAttrs[i];
		count++;
		attr.position = count;
		attrs.push_back(attr);
	}

}
CompOp oppositeOperation(CompOp anOp) {
	switch (anOp) {
	case NO_OP:
		return NO_OP;
	case LT_OP:
		return GE_OP;
	case LE_OP:
		return GT_OP;
	case GT_OP:
		return LE_OP;
	case GE_OP:
		return LT_OP;
	case EQ_OP:
		return EQ_OP;
	case NE_OP:
		return NE_OP;
	default:
		return NO_OP; // should never reach here
	}
}
RC INLJoin::getNextTuple(void *data) {
	//leftTuple da duoc luu trong NLJoin

	void *rightTuple = malloc(dataSize);
	void *leftAttrData = malloc(dataSize);
	void *rightAttrData = malloc(dataSize);
	int rc1 = RC_SUCCESS;

	//Get Left Attribute
	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	LeftAttr = getAttributeFromName(this->leftAttrs, leftAttrName);

	//Get Right Attribute
	Attribute RightAttr;

	CompOp operation = condition.op;

	//Doc moi va endofFile = false, start right table

	while (true) {
		memset(rightTuple, 0, dataSize);
		memset(leftAttrData, 0, dataSize);
		memset(rightAttrData, 0, dataSize);

		if (endOftable == true) {
			memset(this->leftTuple, 0, dataSize);
			rc1 = this->leftInput->getNextTuple(this->leftTuple);

			if (rc1 != RC_SUCCESS) {
				goto fail;
			}
			endOftable = false;
			//get data of leftAttribute
			getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
			this->rightInput->setIterator(oppositeOperation(operation), leftAttrData);
		}

		int rc2 = this->rightInput->getNextTuple(rightTuple);
		//cout<<" Gia tri cua ve phai la " << *(float*)((char*)rightTuple + 4)<<endl;

		if (rc2 != RC_SUCCESS)
		{
			endOftable = true;
			//RestartIterator();
		} else {
			//join, not eliminate duplicate attributes
			if (condition.bRhsIsAttr == false) {
				cout << "Doesnt make sense to join with VALUE" << endl;
				goto fail;
			} else {
				string rightAttrName = condition.rhsAttr;
				RightAttr = getAttributeFromName(this->rightAttrs, rightAttrName);
				//get data of rightAttribute
				getAttributeData(this->rightAttrs, rightAttrName, rightTuple, rightAttrData);
				//cout<<" Gia tri cua ve phai la " << *(float*)((char*)rightAttrData)<<endl;

				//compare 2 attribute type and value
				int compareValue;

				if (LeftAttr.type == RightAttr.type) {
					compareValue = compare(leftAttrData, rightAttrData, LeftAttr.type);
					//Type is compatible
					switch (operation) {
					case NO_OP:
						goto success;
						break;

					case EQ_OP:
						if (compareValue == 0) {
							goto success;
						}
						break;
					case LT_OP:
						if (compareValue == -1) {
							goto success;
						}
						break;

					case GT_OP:
						if (compareValue == 1) {
							goto success;
						}
						break;
					case LE_OP:
						if (compareValue != 1) {
							goto success;
						}
						break;
					case GE_OP:
						if (compareValue != -1) {
							goto success;
						}
						break;
					case NE_OP:
						if (compareValue != 0) {
							goto success;
						}
						break;
					default:
						goto fail;
						break;
					} //end of switch case

				} else { // type of attributes are not compatible

					cout << "Type of 2 sides are not compatible!!";
					goto fail;
				}

			} // end of compare

		} //end rc2 = RC_SUCCESS

	} // end of while

	success:

	copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
	free(rightTuple);
	free(leftAttrData);
	free(rightAttrData);
	return RC_SUCCESS;

	fail:

	free(rightTuple);
	free(leftAttrData);
	free(rightAttrData);
	return RC_FAIL;
}

HashJoin::~HashJoin() {

	for (unsigned i = 0; i < numBuckets; i++) {
		fclose(leftPartitions[i].fHandle.GetHandle());
		fclose(rightPartitions[i].fHandle.GetHandle());
		remove(leftPartitions[i].fHandle.nameOfFile);
		remove(rightPartitions[i].fHandle.nameOfFile);
		free(leftPartitions[i].buffer);
		free(rightPartitions[i].buffer);
		leftPartitions[i].fHandle.ClearHandle();
		rightPartitions[i].fHandle.ClearHandle();
	}

	leftPartitions.clear();
	rightPartitions.clear();

	for (multimap<unsigned, Record>::iterator it = m.begin(); it != m.end(); ++it) {
		free((*it).second.data);
	}
	m.clear();

}
unsigned hash_function(void* value, unsigned modNumber, AttrType type) {

	int intVal;
	float floatVal;

	switch (type) {
	case TypeInt:
		memcpy(&intVal, value, 4);

		break;

	case TypeReal:
		memcpy(&floatVal, value, 4);
		intVal = (floatVal >= 0) ? (int) (floatVal + 0.5) : (int) (floatVal - 0.5);

		break;
	case TypeVarChar:
		memcpy(&intVal, value, 4);
		break;
	default:
		cerr << "Type is not correct!";
		break;
	}

	intVal = abs(intVal);

	return (unsigned) intVal % modNumber;
}

void CreateNewPage(void * buffer) {

	memset(buffer, 0, PF_PAGE_SIZE);
	short free_space = PF_PAGE_SIZE - 4;
	memcpy((char*) buffer + END_OF_PAGE - 2 * unit, &free_space,unit);
}

void WriteTo(void * dest, void * source, short source_len) {

	short lastOffset = 0;
	short lastLength = 0;

	short total = 0;

	memcpy(&total, (char*) dest + END_OF_PAGE - unit,unit);

	short offset = 0;
	if (total > 0) {
		offset = (total << 2) + unit;
		memcpy(&lastOffset, (char*) dest + END_OF_PAGE - offset, unit);
		offset += unit;
		memcpy(&lastLength, (char*) dest + END_OF_PAGE - offset, unit);

		offset = lastOffset + lastLength;
	}
	memcpy((char*) dest + offset, (char*) source, source_len);

//update directory:

	total++;
	short temp_offset = (total << 2) + unit;
	memcpy((char *) dest + END_OF_PAGE - temp_offset, &offset, unit);
	temp_offset += unit;
	memcpy((char *) dest + END_OF_PAGE - temp_offset, &source_len, unit);

	memcpy((char*) dest + END_OF_PAGE - unit, &total,unit);
	short free_space;
	memcpy(&free_space, (char*) dest + END_OF_PAGE - 2 * unit,unit);
	free_space = free_space - source_len - 4;
	memcpy((char*) dest + END_OF_PAGE - 2 * unit, &free_space,unit);

}
RC HashJoin::partitionTable(Iterator *input, vector<Partition> &allPartitions, string attr) {

	unsigned slot;
	PF_Manager *manager = PF_Manager::Instance();

	string name = input->getTableName();

	for (unsigned i = 0; i < numBuckets; i++) {
		Partition part;
		string String = static_cast<ostringstream*>(&(ostringstream() << i))->str();
		string fileName = name + "Part" + String + ".datatemp";
		remove(fileName.c_str());
		manager->CreateFile(fileName.c_str());
		manager->OpenFile(fileName.c_str(), part.fHandle);
		part.totalPages = 0;
		part.buffer = malloc(PF_PAGE_SIZE);
		CreateNewPage(part.buffer);
		allPartitions.push_back(part);
	}

	void *data = malloc(200);
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	void *attrData = malloc(200);
	Attribute theAttribute;
	theAttribute = getAttributeFromName(attrs, attr);
	int count = 0;
	RC rc;
	while (true) {
		rc = input->getNextTuple(data);
		//cerr << count << endl;
		count++;
		//if (count == 1500)
		//	cerr << "wait" << endl;
		if (rc != RC_SUCCESS) {
			//cerr << "exiting" << endl;
			free(data);
			free(attrData);
			// flush all bucket to disk
			short total;

			for (unsigned i = 0; i < this->numBuckets; i++) {

				memcpy(&total, (char*) allPartitions[i].buffer + END_OF_PAGE - unit,unit);
				if (total > 0) {
					allPartitions[i].fHandle.WritePage(allPartitions[i].totalPages, allPartitions[i].buffer);

					//cerr << i << " " << "page " << allPartitions[i].totalPages << " " << total << "records" << endl;
					allPartitions[i].totalPages++;
					CreateNewPage(allPartitions[i].buffer);
				}
			}
			//cerr << "returning" << endl;
			return RC_SUCCESS;
		}

		//get data
		getAttributeData(attrs, attr, data, attrData);

		slot = hash_function(attrData, this->numBuckets, theAttribute.type);
		short size;
		getSizeOfTuple(data, attrs, size);
		//Partition writer = allPartitions[slot];
		short freeSpace;
		short total;
		memcpy(&freeSpace, (char*) allPartitions[slot].buffer + END_OF_PAGE - 2 * unit,unit);
		if (freeSpace < size) {
			allPartitions[slot].fHandle.WritePage(allPartitions[slot].totalPages, allPartitions[slot].buffer);
			memcpy(&total, (char*) allPartitions[slot].buffer + END_OF_PAGE - unit,unit);
			//cerr << "page " << allPartitions[slot].totalPages << " : " << total << "records" << endl;
			allPartitions[slot].totalPages++;
			CreateNewPage(allPartitions[slot].buffer);

		}
		WriteTo(allPartitions[slot].buffer, data, size);

	}
	return RC_SUCCESS;
}

RC HashJoin::getNextTuple(void *data) {

	if (currentPartition == numBuckets) {
		//cerr << "returning EOF " << endl;
		return QE_EOF;
	}

	//PF_Manager *manager = PF_Manager::Instance();
	short offset, start, length = 0;

	Partition left = leftPartitions[currentPartition];

	left.fHandle.ReadPage(leftCurrentPage, left.buffer);
	short leftTotals;
	memcpy(&leftTotals, (char*) left.buffer + END_OF_PAGE - unit,unit);

	//grab data from left

	Attribute leftAttribute = getAttributeFromName(leftAttrs, cond.lhsAttr);

	void *leftData = malloc(200);
	void *leftAttrData = malloc(100);
	while (true) {
		if (leftCurrentSlot == leftTotals) {
			leftCurrentPage++;

			if (leftCurrentPage == (short) left.totalPages) {
				leftCurrentPage = 0;
				currentPartition++;
				if (currentPartition == numBuckets) {
					free(leftData);
					free(leftAttrData);
					//cerr << " End of partitions I'm returning" << endl;

					return QE_EOF;
				}
				left = leftPartitions[currentPartition];
				hasHashTable = false;

			}

			leftCurrentSlot = 0;

			left.fHandle.ReadPage(leftCurrentPage, left.buffer);
			memcpy(&leftTotals, (char*) left.buffer + END_OF_PAGE - unit,unit);
		}

		if (!hasHashTable) {
			//cerr << "entering  building hash" << endl;
			for (multimap<unsigned, Record>::iterator it = m.begin(); it != m.end(); ++it) {
				free((*it).second.data);
			}
			m.clear();
			hasHashTable = true;
			Partition part = rightPartitions[currentPartition];
			unsigned total = part.totalPages;

			Attribute theAttribute = getAttributeFromName(rightAttrs, cond.rhsAttr);
			void * attrData = malloc(100);

			for (unsigned page = 0; page < total; page++) {
				part.fHandle.ReadPage(page, part.buffer);
				short total_records;
				memcpy(&total_records, (char*) part.buffer + END_OF_PAGE - unit,unit);

				for (short i = 0; i < total_records; i++) {
					void *rightdata = malloc(200);
					offset = ((i + 1) << 2) + unit;
					memcpy(&start, (char*) part.buffer + END_OF_PAGE - offset, unit);
					offset += unit;
					memcpy(&length, (char*) part.buffer + END_OF_PAGE - offset, unit);
					memcpy((char*) rightdata, (char*) part.buffer + start, length);

					getAttributeData(rightAttrs, cond.rhsAttr, rightdata, attrData);

					Record aRecord;

					aRecord.data = rightdata;
					aRecord.size = length;

					unsigned key = hash_function(attrData, 2 * numBuckets, theAttribute.type);

					m.insert(pair<unsigned, Record>(key, aRecord));

				}

			} // end of looping thru pages
			free(attrData);
		} // end of !hasHashTable
		if (m.empty())
			leftCurrentSlot = leftTotals;
		else {
			offset = ((leftCurrentSlot + 1) << 2) + unit;
			memcpy(&start, (char*) left.buffer + END_OF_PAGE - offset, unit);
			offset += unit;
			memcpy(&length, (char*) left.buffer + END_OF_PAGE - offset, unit);
			memcpy((char*) leftData, (char*) left.buffer + start, length);

			getAttributeData(leftAttrs, cond.lhsAttr, leftData, leftAttrData);

			unsigned key = hash_function(leftAttrData, 2 * numBuckets, leftAttribute.type);

			pair<multimap<unsigned, Record>::iterator , multimap<unsigned, Record>::iterator> ppp;

			ppp = m.equal_range(key);

			Record matched;
			short i = 0;
			void *rhs = malloc(100);
			for (multimap<unsigned, Record>::iterator it2 = ppp.first; it2 != ppp.second; ++it2) {
				if (i == rightIndex) {
					matched = (*it2).second;

					getAttributeData(rightAttrs, cond.rhsAttr, matched.data, rhs);
					int compareValue = compare(leftAttrData, rhs, leftAttribute.type);

					if (compareValue == 0) {
						memcpy(data, leftData, length);
						memcpy((char*) data + length, matched.data, matched.size);
						rightIndex++;
						free(rhs);
						free(leftData);
						free(leftAttrData);
						//cerr << "I'm returning SUCCESS" << endl;
						return RC_SUCCESS;
					} else {
						rightIndex++;
						i++;
					}
				} else
					i++;
			}
			leftCurrentSlot++;
			rightIndex = 0;
		}
	}
	//cerr << "returning SUCCESS" << endl;
	return RC_SUCCESS;
}

void HashJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
Attribute attr;
unsigned count =0;

	for (unsigned i = 0; i < this->leftAttrs.size(); i++) {
		attr = leftAttrs[i];
		count++;
		attr.position = count;
		attrs.push_back(attr);
	}
	for (unsigned i = 0; i < this->rightAttrs.size(); i++) {
		attr = rightAttrs[i];
				count++;
				attr.position = count;
				attrs.push_back(attr);
	}
}

Aggregate::~Aggregate() {

	for (map<string, char*>::iterator it = mymap.begin(); it != mymap.end(); ++it) {
		free((*it).second);
	}
	mymap.clear();
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {

	attrs.clear();
	Attribute att;
	string operation;
	switch (op) {
	case MIN:
		operation = "MIN";
		break;
	case MAX:
		operation = "MAX";
		break;
	case AVG:
		operation = "AVG";
		break;
	case COUNT:
		operation = "COUNT";
		break;
	case SUM:
		operation = "SUM";
		break;
	}

	att.name = operation + "(" + aggAttr.name + ")";
	att.type = aggAttr.type;
	att.length = aggAttr.length;
	att.position = 1;
	if (numberOfParameter == 3) {
		attrs.push_back(att);

	} else {
		att.position = 2;
		attrs.push_back(att);
		Attribute att1;
		att1.name = gAttr.name;
		att1.type = gAttr.type;
		att1.length = gAttr.length;
		att1.position = 1;
		std::vector<Attribute>::iterator ite;
		ite = attrs.begin();
		attrs.insert(ite, att1);

	}

	/*for (unsigned i = 0; i < attrs.size(); i++) {
	 cout << "NAME = " << attrs[i].name << " ";
	 cout << "Type = " << attrs[i].type << " ";
	 cout << "Length = " << attrs[i].length << endl;
	 cout << "position = " << attrs[i].position << endl;

	 }*/
}

RC Aggregate::Init(Iterator *input, vector<Attribute> attrs, Attribute aggAtt, float *min, float *max) {
	void *temp = malloc(200);
	void *aggData = malloc(200); //keep data of Attribute
	float tempValue;
	int rc = input->getNextTuple(temp);
	if (rc == RC_SUCCESS)
	{
		getAttributeData(attrs, aggAtt.name, temp, aggData);
		if (aggAtt.type == TypeInt) {
			tempValue = (float) *(int*) aggData;
		} else
			tempValue = *(float*) aggData;
		*max = tempValue;
		*min = tempValue;

	}
	input->setIterator();
	free(aggData);
	free(temp);
	return rc;
}

RC Aggregate::getNextTuple(void *data) {
	if (isDone)
		return QE_EOF;

	int rc;
	float value = 0.0;
	float returnValue = 0.0;

	if (this->numberOfParameter == 3) {
		void *temp = malloc(200);
		void *aggData = malloc(200); //keep data of Attribute
		while (true) {
			rc = this->input->getNextTuple(temp);
			if (rc == RC_SUCCESS) {
				getAttributeData(attrs, aggAttr.name, temp, aggData);
				if (aggAttr.type == TypeInt)
					value = (float) *(int*) aggData;
				else if (aggAttr.type == TypeReal)
					value = *(float*) aggData;
				else
					goto fail;

				this->count += 1;
				this->sum += value;
				switch (op) {
				case MIN:
					if (this->min > value) {
						this->min = value;
					}
					returnValue = min;
					break;
				case MAX:
					if (this->max < value) {
						this->max = value;
					}
					returnValue = max;
					break;
				case COUNT:
					returnValue = (float) count;
					break;
				case SUM:
					returnValue = sum;
					break;
				case AVG:
					returnValue = (float) sum / count;
					break;
				default:
					cerr << "Error Aggredate Operation";
					goto fail;

				} // end of switch

			} else {
				isDone = true;
				goto success;
			}
		}
		success: memcpy((char*) data, &returnValue, 4);
		free(temp);
		free(aggData);
		return RC_SUCCESS;

		fail: free(temp);
		free(aggData);
		return RC_FAIL;

	} //Chuyen sang phan co 4 tham so - groupBy
	else {
		if (it == mymap.end())
			return QE_EOF;
		string first = (*it).first;
		char* second = (*it).second;
		unsigned length = 4;
		//cout << (*it).first << " => " << *(float*)((*it).second)<< endl;

		if (gAttr.type == TypeVarChar) {
			length = first.length();
			memcpy((char*) data, &length, 4);
			memcpy((char*) data + 4, first.c_str(), length);
			length += 4;
		} else if (gAttr.type == TypeInt) {
			int intFirst = ::strtod(first.c_str(), 0);
			memcpy((char*) data, &intFirst, 4);
		} else if (gAttr.type == TypeReal) {
			float floatFirst = ::strtod(first.c_str(), 0);
			memcpy((char*) data, &floatFirst, 4);
		}
		memcpy((char*) data + length, second, 4);
		//cout<<*(float*)data<<endl;
		//*(float *)((char *)data + offset)
		//cout<<*(float*)((char*)data+ 4)<<endl;
		it++;
		return RC_SUCCESS;
	}

}

RC Aggregate::createMap() {
	void * gData = malloc(200);
	void * temp = malloc(200);
	void *aggData = malloc(200);

	float value;

	string aValue;
	unsigned count = 0;

	while (true) {
		int rc = this->input->getNextTuple(temp);
		if (rc == RC_FAIL
		)
			goto fail;
		getAttributeData(attrs, aggAttr.name, temp, aggData);
		if (aggAttr.type == TypeInt)
			value = (float) *(int*) aggData;
		else if (aggAttr.type == TypeReal)
			value = *(float*) aggData;
		else
			goto fail;

		getAttributeData(this->attrs, gAttr.name, temp, gData);

		float val;
		string gValue;
		if (gAttr.type != TypeVarChar) {
			if (gAttr.type == TypeInt)
				val = (float) *(int*) gData;
			else if (gAttr.type == TypeReal)
				val = *(float*) gData;

			gValue = static_cast<ostringstream*>(&(ostringstream() << val))->str();
		} else {
			// not tested yet!

			unsigned length;
			memcpy(&length, (char*) gData, 4);
			char* tempChar = (char*)malloc (length+4);

			memcpy(tempChar, (char*) gData+4, length);
			//tempChar[length] = '\0';
			//memcpy((char*)gValue.c_str(),(char*)gData +4, length);
			gValue = string(tempChar);
			gValue[length]='\0';

		}

		it = mymap.find(gValue);

		if (it == mymap.end()) {
			// not existed yet
			char *test = NULL;

			if (op == AVG) {
				test = (char*) malloc(8);
			} else {
				test = (char*) malloc(4);
			}

			if (aggAttr.type == TypeInt || aggAttr.type == TypeReal) {
				if (op == COUNT) {
					float oneFloat = 1.0;
					memcpy(test, &oneFloat, 4);
				} else {
					memcpy(test, &value, 4);

					if (op == AVG) {
						unsigned one = 1;
						memcpy((char*) test + 4, (char*) &one, 4);
					}
				}
			} else
				goto fail;

			mymap.insert(pair<string, char*>(gValue, test));

		} else {
			//co roi cap nhat lai so luong

			char* secondValue = (*it).second;
			float second;
			memcpy(&second, secondValue, 4);
			switch (op) {
			case MIN:
				if (second > value)
					second = value;
				break;
			case MAX:
				if (second < value)
					second = value;
				break;
			case COUNT:
				second++;
				break;
			case SUM:
				second += value;
				break;
			case AVG:
				memcpy(&count, (char*) secondValue + 4, 4);
				count++;
				second += (value - second) / count;
				break;
			default:
				cerr << "Error Aggregate Operation";
				goto fail;

			} //het switch
			memcpy((char*) secondValue, &second, 4);
			if (op == AVG) {
				memcpy((char*) secondValue + 4, &count, 4);

			}

			//(*it).second = secondValue;
		}
	} //end of while

	//}//het 2 kieu int
	goto success;

	success: free(temp);
	free(aggData);
	free(gData);
	return RC_SUCCESS;

	fail: free(temp);
	free(aggData);
	free(gData);
	return RC_FAIL;
}

