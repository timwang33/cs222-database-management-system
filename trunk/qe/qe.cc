# include "qe.h"
#include<algorithm>

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
				break;
			case TypeVarChar:
				memcpy(&length_var_char, (char*) data + offset, 4);
				memcpy(attrData, (char*) data + offset, length_var_char + 4);
				//copy string & length of string
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
		projectAttrs.push_back(attr);
	}
	// Reverse ?
	//reverse(projectAttrs.begin(), projectAttrs.end());
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

	for (unsigned i = 0; i < this->leftAttrs.size(); i++) {
		attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	for (unsigned i = 0; i < this->rightAttrs.size(); i++) {
		attr = rightAttrs[i];
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

	for (unsigned i = 0; i < this->leftAttrs.size(); i++) {
		attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	for (unsigned i = 0; i < this->rightAttrs.size(); i++) {
		attr = rightAttrs[i];
		attrs.push_back(attr);
	}

}

RC INLJoin::getNextTuple(void *data) {
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

		if (endOftable == true) {
			memset(this->leftTuple, 0, 200);
			rc1 = this->leftInput->getNextTuple(this->leftTuple);

			//cout<<" Gia tri cua ve trai la " << *(float*)((char*)leftTuple + 8)<<endl;

			if (rc1 != RC_SUCCESS) {
				goto fail;
			}
			endOftable = false;
		}

		//get data of leftAttribute
		getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
		// leftInput van con => success va rightTuple van con endOffile => false
		int rc2 = this->rightInput->getNextTuple(rightTuple);
		//cout<<" Gia tri cua ve phai la " << *(float*)((char*)rightTuple + 4)<<endl;

		if (rc2 != RC_SUCCESS)
		{
			endOftable = true;
			RestartIterator();
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

	fail: free(rightTuple);
	free(leftAttrData);
	free(rightAttrData);
	return RC_FAIL;
}

HashJoin::~HashJoin() {

}



RC HashJoin::getNextTuple(void *data) {
	return QE_EOF;
}

void HashJoin::getAttributes(vector<Attribute> &attrs) const {

}

