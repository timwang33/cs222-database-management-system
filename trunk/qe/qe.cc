# include "qe.h"
#include<algorithm>

//compare 2 memory
int compare(void *data1, void *data2, AttrType type)
{
	short rc;
	int int1, int2;
	float real1, real2;
	int length1, length2;
	string str1, str2;

	//Compare data1, data2 with length = length
	switch(type)
	{
		case TypeInt:
			memcpy(&int1, data1, 4);
			memcpy(&int2, data2, 4);
			if(int1 < int2) rc = -1;
			else if (int1 == int2) rc = 0;
			else rc = 1;
			break;

		case TypeReal:
			memcpy(&real1, data1, 4);
			memcpy(&real2, data2, 4);
			if(real1 < real2) rc = -1;
			else if (real1 == real2) rc = 0;
			else rc = 1;
			break;
		case TypeVarChar:
			memcpy(&length1, data1, 4);
			memcpy(&length2, data2, 4);

			memcpy((char*)str1.c_str(), (char*)data1 + 4, length1);
			memcpy((char*)str2.c_str(), (char*)data2 + 4, length2);
			rc = strcmp(str1.c_str(), str2.c_str());
			break;

		default:
			cerr<<"Type is not correct!";
			break;
	}
	return rc;
}

//get attribute data
void getAttributeData(const vector<Attribute> attrs, const string attr, const void *data, void *attrData)
{
	short offset = 0;
	int length_var_char;
	for (int i = 0; i < (int) attrs.size(); i++) {
		if (strcmp(attrs[i].name.c_str(), attr.c_str()) == RC_SUCCESS)
		{
			switch (attrs[i].type)
			{
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
		}
		else
		{

			switch (attrs[i].type)
			{
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
	for (unsigned i = 0; i< attrs.size(); i++)
	{
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
Attribute getAttributeFromName(const vector<Attribute> attrs, const string attrName)
{
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
		cerr << "Error decoding attrs[i]" << endl;
		break;
	}
	size = count;
	return 0;
}

Filter::~Filter()
{

}
RC Filter:: getNextTuple(void *data)
{
	bool hasReturn = false;
	void *temp = malloc(200);
	void *attrData = malloc(200); //keep data of Attribute

	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	CompOp  operation = condition.op;
	int rc;


	while(hasReturn == false)
	{
	rc = input->getNextTuple(temp);
	if (rc == RC_FAIL)
		{
			free(temp);
			free(attrData);
			return RC_FAIL;
		}
	if(operation == NO_OP)
		{
			memcpy(data, temp, 200);
			free(temp);
			free(attrData);
			return RC_SUCCESS;
		}
	//get data of leftAttribute
	getAttributeData(attrs, leftAttrName, temp, attrData);
	//get leftAttribute all information
	LeftAttr = getAttributeFromName(attrs, leftAttrName);

	//filter data
	if(condition.bRhsIsAttr==false)
	{
		Value  rightValue;
		int compareValue;
		//compare with a value
		rightValue = condition.rhsValue;
		if(LeftAttr.type == rightValue.type)
		{
			compareValue = compare(attrData, rightValue.data, LeftAttr.type);
			//Type is compatible
			switch(operation)
			{
					case EQ_OP:
						if(compareValue == 0)
							{
								//This tuple is satisfied condition
								memcpy(data, temp, 200);
								free(temp);
								free(attrData);
								return RC_SUCCESS;
							}
						else
						{
							//free(temp);
							//free(attrData);
							memset(temp, 0, 200);
							memset(attrData, 0, 200);
							//return RC_FAIL;
						}
						break;

					case LT_OP:
						if(compareValue == -1)
								{
									//This tuple is satisfied condition
									memcpy(data, temp, 200);
									free(temp);
									free(attrData);
									return RC_SUCCESS;
								}
							else
							{
								//free(temp);
								//free(attrData);
								memset(temp, 0, 200);
								memset(attrData, 0, 200);
								//return RC_FAIL;
							}

							break;
						case GT_OP:
							if(compareValue == 1)
							{
								memcpy(data, temp, 200);
								free(temp);
								free(attrData);
								return RC_SUCCESS;
							}
							else
							{
								//free(temp);
								//free(attrData);
								memset(temp, 0, 200);
								memset(attrData, 0, 200);
								//return RC_FAIL;
							}
							break;
						case LE_OP:
							if(compareValue!=1)
							{
								memcpy(data, temp, 200);
								free(temp);
								free(attrData);
								return RC_SUCCESS;
							}
							else
							{
								//free(temp);
								//free(attrData);
								memset(temp, 0, 200);
								memset(attrData, 0, 200);
								//return RC_FAIL;
							}
							break;
						case GE_OP:
							if(compareValue!=-1)
							{
								memcpy(data, temp, 200);
								free(attrData);
								free(temp);
								return RC_SUCCESS;
							}
							else
							{
								//free(attrData);
								//free(temp);
								memset(temp, 0, 200);
								memset(attrData, 0, 200);
								//return RC_FAIL;
							}

							break;
						case NE_OP:
							if(compareValue!=0)
							{
								memcpy(data, temp, 200);
								free(temp);
								free(attrData);
								return RC_SUCCESS;
							}
							else
							{
								//free(attrData);
								//free(temp);
								//return RC_FAIL;
								memset(temp, 0, 200);
								memset(attrData, 0, 200);
							}
							break;
						default:
						{
							free(temp);
							free(attrData);
							return RC_FAIL;
						}
							//break;
				}


		}
		else
		{
			cout<<"Type of 2 sides are not compatible!!";
			free(temp);
			free(attrData);
			return RC_FAIL;
		}

	}
}

return RC_SUCCESS;

};

void Filter::getAttributes(vector<Attribute> &attrs) const
 {
        	 attrs.clear();
        	 attrs = this->attrs;

 }


//Project
Project::~Project()
{

}


void Project::getAttributes(vector<Attribute> &projectAttrs) const
{
	projectAttrs.clear();
	Attribute attr;
	vector<Attribute> attrs;
	attrs = this -> attrs;

	for (unsigned i = 0; i< this->projectAttrNames.size();i++)
	{
		attr = getAttributeFromName(attrs, projectAttrNames[i]);
		projectAttrs.push_back(attr);
	}
	// Reverse
	reverse(attrs.begin(), attrs.end());
}

RC Project::getNextTuple(void *data)
{

	void *temp = malloc(200);
	int rc = input->getNextTuple(temp);
	void *projectData = malloc(200); //keep data of project Attribute
	Attribute projectAttr;
	short offset = 0;
	short size = 0;


	for (unsigned i = 0; i< this->projectAttrNames.size(); i++)
	{
		//get data of each attribute
		getAttributeData(attrs, projectAttrNames[i], temp, projectData);
		projectAttr = getAttributeFromName(this->attrs, projectAttrNames[i]);
		sizeOfAttribute(projectData, projectAttr,size);
		memcpy((char*)data + offset, projectData, size);
		offset += size;
	}
	free(temp);
	free(projectData);
	return rc;
}




//NLJoin
NLJoin::~NLJoin()
{

}

void copyData(void *leftData, void *rightData, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs, void *data)
{
	short leftSize = 0;
	short rightSize = 0;

	getSizeOfTuple(leftData, leftAttrs, leftSize);
	getSizeOfTuple(rightData, rightAttrs, rightSize);
	memcpy(data, leftData, leftSize);
	memcpy((char*)data + leftSize, rightData, rightSize);
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	//contain all attributes
	attrs.clear();
	Attribute attr;

	for(unsigned i = 0; i< this->leftAttrs.size(); i++)
	{
		attr = rightAttrs[i];
		attrs.push_back(attr);
	}
	for(unsigned i = 0; i< this->rightAttrs.size(); i++)
	{
		attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	reverse(attrs.begin(), attrs.end());

}
//void getPartData(vector<Attribute> attrs, vector<Attribute>)


RC NLJoin:: getNextTuple(void *data)
{
	//leftTuple da duoc luu trong NLJoin
	void *rightTuple = malloc(200);
	void *leftAttrData = malloc(200);
	void *rightAttrData = malloc(200);
	int rc1 = RC_SUCCESS;
	bool hasReturn = false;

	//Get Left Attribute
	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	LeftAttr = getAttributeFromName(this->leftAttrs, leftAttrName);
	//Get Right Attribute
	Attribute RightAttr;

	CompOp  operation = condition.op;

	//Doc moi va endofFile = false, start right table

	while(hasReturn == false)
	{
		if(endOftable == true)
		{
			memset(this->leftTuple, 0, 200);
			rc1 = this->leftInput->getNextTuple(this->leftTuple);

			if(rc1 == RC_FAIL)
			{
				free(this->leftTuple);
				free(leftAttrData);
				free(rightTuple);
				free(rightAttrData);
				return RC_FAIL;
			}

			endOftable = false;
		}

	//get data of leftAttribute
	getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
	// leftInput van con => success va rightTuple van con endOffile => false
	if (rc1 == RC_SUCCESS && endOftable == false)
	{
		int rc2 = this->rightInput->getNextTuple(rightTuple);
		if(rc2 == RC_FAIL)
		{
			endOftable = true;
			this->rightInput->setIterator();
		}
		if(rc2 == RC_SUCCESS)
		{
			//join, not eliminate duplicate attributes
			if(condition.bRhsIsAttr==true)
			{
				string rightAttrName = condition.rhsAttr;
				RightAttr = getAttributeFromName(this->rightAttrs, rightAttrName);
				//get data of rightAttribute
				getAttributeData(this->rightAttrs, rightAttrName, rightTuple, rightAttrData);

				//compare 2 attribute type and value
				int compareValue;

				if(LeftAttr.type == RightAttr.type)
				{
					compareValue = compare(leftAttrData, rightAttrData, LeftAttr.type);
					//Type is compatible
					switch(operation)
					{
						case NO_OP:
							{
								copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
								free(rightTuple);
								free(leftAttrData);
								free(rightAttrData);
								hasReturn = true;
								return RC_SUCCESS;
							}
						case EQ_OP:
							if(compareValue == 0)
							{
									//This tuple is satisfied condition
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
							}
							else
							{
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								hasReturn = false;
								//return RC_SUCCESS;
								break;

							}
							//break;


						case LT_OP:
							if(compareValue == -1)
							{
									//This tuple is satisfied condition
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
							}
							else
							{
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								hasReturn = false;
								//return RC_FAIL;
								break;
							}

								//break;
							case GT_OP:
								if(compareValue == 1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(leftAttrData);
									free(rightTuple);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn = false;
									//return RC_FAIL;
									break;
								}
								//break;
							case LE_OP:
								if(compareValue!=1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn= false;
									//return RC_FAIL;
									break;
								}
								//break;
							case GE_OP:
								if(compareValue!=-1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn= false;
									//return RC_FAIL;
									break;
								}

								//break;
							case NE_OP:
								if(compareValue!=0)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn = false;
									//return RC_FAIL;
									break;
								}
								//break;
							default:
							{
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								//hasReturn = false;
								//return RC_FAIL;
								break;
							}
								//break;
						} //end of switch case

				}
				else // type of attributes are not compatible
				{
					cout<<"Type of 2 sides are not compatible!!";
					free(this->leftTuple);
					free(rightTuple);
					free(leftAttrData);
					free(rightAttrData);
					return RC_FAIL;
				}

			} // end of compare
			//Neu right table het thi Restart right table
		} //end rc2 = RC_SUCCESS

	}
	}
}


INLJoin::~INLJoin()
{

}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	//contain all attributes
	attrs.clear();
	Attribute attr;

	for(unsigned i = 0; i< this->leftAttrs.size(); i++)
	{
		attr = rightAttrs[i];
		attrs.push_back(attr);
	}
	for(unsigned i = 0; i< this->rightAttrs.size(); i++)
	{
		attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	reverse(attrs.begin(), attrs.end());
}
/*RC INLJoin::getNextTuple(void *data)
{
	//leftTuple da duoc luu trong NLJoin
	void *rightTuple = malloc(200);
	void *leftAttrData = malloc(200);
	void *rightAttrData = malloc(200);
	int rc1 = RC_SUCCESS;
	bool hasReturn = false;

	//Get Left Attribute
	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	LeftAttr = getAttributeFromName(this->leftAttrs, leftAttrName);

	//Get Right Attribute
	Attribute RightAttr;

	CompOp  operation = condition.op;

	//Doc moi va endofFile = false, start right table

	while(hasReturn == false)
	{
		//Doc moi
		if(endOftable == true)
			{
				memset(this->leftTuple, 0, 200);
				rc1 = this->leftInput->getNextTuple(this->leftTuple);
				if(rc1 == RC_FAIL)
					{
						free(this->leftTuple);
						free(leftAttrData);
						free(rightTuple);
						free(rightAttrData);
						return RC_FAIL;
					}

						endOftable = false;
			}

		//get data of leftAttribute
		getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
		// leftInput van con => success va rightTuple van con endOffile => false
		if (endOftable == false)
			{
				int rc2 = this->rightInput->getNextTuple(rightTuple);
				if(rc2 == RC_SUCCESS)
				{
					//join, not eliminate duplicate attributes
					if(condition.bRhsIsAttr==true)
					{
						string rightAttrName = this->condition.rhsAttr;
						RightAttr = getAttributeFromName(this->rightAttrs, rightAttrName);
						//get data of rightAttribute
						getAttributeData(this->rightAttrs, rightAttrName, rightTuple, rightAttrData);
						//
						//
						void *data1 = malloc(4);
						void *data2 = malloc(4);
						memcpy(data2, (char*)rightAttrData, 4);
						memcpy(data1, (char*)leftAttrData, 4);
						cout<<" Gia tri cua ve trai la " << *(int*)((char*)data1)<<endl;
						cout<<" Gia tri cua ve phai la " << *(int*)((char*)data2)<<endl;
						free(data1);
						free(data2);

						//compare 2 attribute type and value
						int compareValue;

						if(LeftAttr.type == RightAttr.type)
						{
							compareValue = compare(leftAttrData, rightAttrData, LeftAttr.type);
							//Type is compatible
							switch(operation)
							{
							case NO_OP:
								{
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									//hasReturn = true;
									return RC_SUCCESS;
								}
							case EQ_OP:
								if(compareValue == 0)
								{
									//This tuple is satisfied condition
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									//hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//memset(rightTuple, 0, 200);
									//memset(leftAttrData, 0, 200);
									//memset(rightAttrData, 0, 200);
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									//hasReturn = false;
									//return RC_SUCCESS;
									break;
								}
							case GE_OP:
								if(compareValue!=-1)
								{
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									return RC_SUCCESS;
								}
								else
								{
									break;
								}

							default:
								return RC_FAIL;
							}
						}
					}
				}
			}
		}
	}

*/
RC INLJoin::getNextTuple(void *data)
{
	//leftTuple da duoc luu trong NLJoin
	void *rightTuple = malloc(200);
	void *leftAttrData = malloc(200);
	void *rightAttrData = malloc(200);
	int rc1 = RC_SUCCESS;
	bool hasReturn = false;

	//Get Left Attribute
	Attribute LeftAttr;
	string leftAttrName = condition.lhsAttr;
	LeftAttr = getAttributeFromName(this->leftAttrs, leftAttrName);
	//Get Right Attribute
	Attribute RightAttr;

	CompOp  operation = condition.op;

	//Doc moi va endofFile = false, start right table

	while(hasReturn == false)
	{
		if(endOftable == true)
		{
			memset(this->leftTuple, 0, 200);
			rc1 = this->leftInput->getNextTuple(this->leftTuple);


			//cout<<" Gia tri cua ve trai la " << *(float*)((char*)leftTuple + 8)<<endl;


			if(rc1 == RC_FAIL)
			{
				free(this->leftTuple);
				free(leftAttrData);
				free(rightTuple);
				free(rightAttrData);
				return RC_FAIL;
			}

			endOftable = false;
		}

	//get data of leftAttribute
	getAttributeData(this->leftAttrs, leftAttrName, this->leftTuple, leftAttrData);
	// leftInput van con => success va rightTuple van con endOffile => false
	if (rc1 == RC_SUCCESS && endOftable == false)
	{
		int rc2 = this->rightInput->getNextTuple(rightTuple);
		//cout<<" Gia tri cua ve phai la " << *(float*)((char*)rightTuple + 4)<<endl;
		if(rc2 == RC_FAIL)
			{
				endOftable = true;
				RestartIterator();
			}
		if(rc2 == RC_SUCCESS)
		{
			//join, not eliminate duplicate attributes
			if(condition.bRhsIsAttr==true)
			{
				string rightAttrName = condition.rhsAttr;
				RightAttr = getAttributeFromName(this->rightAttrs, rightAttrName);
				//get data of rightAttribute
				getAttributeData(this->rightAttrs, rightAttrName, rightTuple, rightAttrData);
				//cout<<" Gia tri cua ve phai la " << *(float*)((char*)rightAttrData)<<endl;

				//compare 2 attribute type and value
				int compareValue;

				if(LeftAttr.type == RightAttr.type)
				{
					compareValue = compare(leftAttrData, rightAttrData, LeftAttr.type);
					//Type is compatible
					switch(operation)
					{
						case NO_OP:
							{
								copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
								free(rightTuple);
								free(leftAttrData);
								free(rightAttrData);
								hasReturn = true;
								return RC_SUCCESS;
							}
						case EQ_OP:
							if(compareValue == 0)
							{
									//This tuple is satisfied condition
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									//hasReturn = true;
									return RC_SUCCESS;
							}
							else
							{
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								//hasReturn = false;
								//return RC_SUCCESS;
								break;

							}
							//break;


						case LT_OP:
							if(compareValue == -1)
							{
									//This tuple is satisfied condition
									copyData(this->leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
							}
							else
							{
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								hasReturn = false;
								//return RC_FAIL;
								break;
							}

								//break;
							case GT_OP:
								if(compareValue == 1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(leftAttrData);
									free(rightTuple);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn = false;
									//return RC_FAIL;
									break;
								}
								//break;
							case LE_OP:
								if(compareValue!=1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn= false;
									//return RC_FAIL;
									break;
								}
								//break;
							case GE_OP:
								if(compareValue!=-1)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									//hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									//hasReturn= false;
									//return RC_FAIL;
									break;
								}

								//break;
							case NE_OP:
								if(compareValue!=0)
								{
									copyData(leftTuple, rightTuple, this->leftAttrs, this->rightAttrs, data);
									free(rightTuple);
									free(leftAttrData);
									free(rightAttrData);
									//hasReturn = true;
									return RC_SUCCESS;
								}
								else
								{
									//free(rightTuple);
									//free(leftAttrData);
									//free(rightAttrData);
									memset(rightTuple, 0, 200);
									memset(leftAttrData, 0, 200);
									memset(rightAttrData, 0, 200);
									hasReturn = false;
									//return RC_FAIL;
									break;
								}
								//break;
							default:
							{
								//free(rightTuple);
								//free(leftAttrData);
								//free(rightAttrData);
								memset(rightTuple, 0, 200);
								memset(leftAttrData, 0, 200);
								memset(rightAttrData, 0, 200);
								//hasReturn = false;
								//return RC_FAIL;
								break;
							}
								//break;
						} //end of switch case

				}
				else // type of attributes are not compatible
				{
					cout<<"Type of 2 sides are not compatible!!";
					free(this->leftTuple);
					free(rightTuple);
					free(leftAttrData);
					free(rightAttrData);
					return RC_FAIL;
				}

			} // end of compare
			//Neu right table het thi Restart right table
		} //end rc2 = RC_SUCCESS
		/*else //end of rightTable
		{
			rightInput->setIterator();
			endOftable = true; //leftTable-> getNextuple
			hasReturn = false;
		}*/
	}
	}
}


