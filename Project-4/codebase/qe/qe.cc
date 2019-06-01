

#include "qe.h"
#include <cmath>
#include <cstring>

bool comp (int left, int right, CompOp op) {
	switch (op) {
		case EQ_OP: return (left == right);
		case LT_OP: return (left < right);
		case LE_OP: return (left <= right);
		case GT_OP: return (left > right);
		case GE_OP: return (left >= right);
		case NE_OP: return (left != right);
		case NO_OP: return true;
		default: return false;
	}
}

bool comp (float left, float right, CompOp op) {
	switch (op) {
		case EQ_OP: return (left == right);
		case LT_OP: return (left < right);
		case LE_OP: return (left <= right);
		case GT_OP: return (left > right);
		case GE_OP: return (left >= right);
		case NE_OP: return (left != right);
		case NO_OP: return true;
		default: return false;
	}
}

bool comp (const void *left, const void *right, CompOp op) {

	int leftSize;
	memcpy (&leftSize, (char*)left, sizeof(int));
	char leftStr[leftSize + 1];
	leftStr[leftSize] = '\0';
	memcpy (leftStr, (char*)left + sizeof(int), leftSize);

	int rightSize;
	memcpy (&rightSize, (char*)right, sizeof(int));
	char rightStr[rightSize + 1];
	rightStr[rightSize] = '\0';
	memcpy (rightStr, (char*)right + sizeof(int), rightSize);

	int cmp = strcmp (leftStr, rightStr);

	switch (op) {
		case EQ_OP: return (cmp == 0);
		case LT_OP: return (cmp < 0);
		case LE_OP: return (cmp <= 0);
		case GT_OP: return (cmp > 0);
		case GE_OP: return (cmp >= 0);
		case NE_OP: return (cmp != 0);
		case NO_OP: return true;
		default: return false;
	}
}

RC checkComp (CompOp op, Attribute attr, const void *left, const void *right) {
	if (attr.type == TypeVarChar) {
		return comp (left, right, op);
	}
	else if (attr.type == TypeInt) {
		int leftInt, rightInt;
		memcpy (&leftInt, left, sizeof(int));
		memcpy (&rightInt, right, sizeof(int));
		return comp (leftInt, rightInt, op);
	}
	else if (attr.type == TypeReal) {
		float leftFloat, rightFloat;
		memcpy (&leftFloat, left, sizeof(float));
		memcpy (&rightFloat, right, sizeof(float));
		return comp (leftFloat, rightFloat, op);
	}
	return -1;
}

int getNullIndicatorSize (int fieldCount) {
	return int(ceil((double) fieldCount / 8.0));
}

bool fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

int getLHSValue (vector<Attribute> attrs, int index, void *data, void *leftValue) {
	// check for a null
	int nullIndicatorSize = getNullIndicatorSize (attrs.size());
	char nullIndicator[nullIndicatorSize];
	memset (nullIndicator, 0, nullIndicatorSize);
	memcpy (nullIndicator, (char*)data, nullIndicatorSize);

	int offset = nullIndicatorSize;
	for (int i = 0; i < attrs.size(); ++i) {
		if (fieldIsNull(nullIndicator, i))
			continue;

		if (attrs[i].type == TypeVarChar) {
			int length = 0;
			memcpy (&length, (char*)data + offset, sizeof(int));
			memcpy (leftValue, (char*)data + offset, sizeof(int) + length);
			offset += sizeof(int) + length;
		}
		else {
			memcpy (leftValue, (char*)data + offset, 4); // works for int or float
			offset += sizeof(int);
		}

		if (i == index)
			return SUCCESS;
	}
	return -1;
}

Filter::Filter(Iterator* input, const Condition &condition) {
	m_input = input;
	cond = condition;
	m_input->getAttributes (attrs);

}

RC Filter::getNextTuple (void *data) {

	for (;;) {
		if (m_input->getNextTuple (data) == QE_EOF)
			return QE_EOF;
		
		if (cond.bRhsIsAttr)
			return -1;

		// check if attr.type matches
		auto pred = [&](Attribute a) {return a.name == cond.lhsAttr;};
		auto iterPos = find_if (attrs.begin(), attrs.end(), pred);
		unsigned index = distance (attrs.begin(), iterPos);

		if (index == attrs.size() or cond.rhsValue.type != attrs[index].type)
			return FILTER_NO_SUCH_ATTR;

		int nullIndicatorSize = getNullIndicatorSize (attrs.size());
		char nullIndicator[nullIndicatorSize];
		memset (nullIndicator, 0, nullIndicatorSize);
		memcpy (nullIndicator, (char*)data, nullIndicatorSize);

		if (fieldIsNull (nullIndicator, index))
			continue;

		// grab lhs data
		void *leftValue = malloc (PAGE_SIZE);

		getLHSValue (attrs, index, data, leftValue);

		if (checkComp(cond.op, attrs[index], leftValue, cond.rhsValue.data)) {
			free (leftValue);
			return SUCCESS;
		}
		free (leftValue);
	}
	return -1;
}

void Filter::getAttributes (vector<Attribute> &attrs) const {
	m_input->getAttributes (attrs);
}

// ... the rest of your implementations go here
Project::Project(Iterator *input, const vector<string> &attrNames) {
	this->attrs.clear();
	this->iter = input;

	vector<Attribute> iterAttrs;
	iter->getAttributes(iterAttrs);
	for(int i = 0; i < (int)attrNames.size() ; i++){
		for(int j = 0; j < (int)iterAttrs.size() ; j++){
			if(attrNames[i] == iterAttrs[j].name){
				this->attrs.push_back(iterAttrs[j]);
				break;
			}
		}
	}
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.assign(this->attrs.begin(), this->attrs.end());
}

RC Project::getNextTuple(void *data){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	//get the tuple
	void * tempData = malloc(PAGE_SIZE);
	iter->getNextTuple(tempData);
	//get the recordDiscriptor of the record.
	vector<Attribute> iterAttrs;
	iter->getAttributes(iterAttrs);

	//get the attributes that the function wants.

	//get nulls
	int nullIndicatorSize = rbfm->getNullIndicatorSize(iterAttrs.size());
	char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    memcpy (nullIndicator, tempData, nullIndicatorSize);
    int tempOffset = nullIndicatorSize;

    //stuff for returned data
    nullIndicatorSize = rbfm->getNullIndicatorSize(this->attrs.size());
    char dataNulls[nullIndicatorSize];
    memset(dataNulls, 0, nullIndicatorSize);
    memcpy(data, dataNulls, nullIndicatorSize);
    int dataOffset = nullIndicatorSize;

    int length = 0;

    for (int i = 0; i < (int)this->attrs.size() ; i++)
    {
    	for(int j = 0; j < (int)iterAttrs.size() ; j++){
	        if (rbfm->fieldIsNull(nullIndicator, j)){
	        	if(iterAttrs[j].name == this->attrs[i].name){
	        		//flip the bit
	        		int mask = 1 << (CHAR_BIT - 1 - (j % CHAR_BIT));
	        		dataNulls[j/8] ^= mask;
	        		memcpy(data, dataNulls, nullIndicatorSize);
	        	}
	            continue;
	        }
	        if (iterAttrs[j].name == this->attrs[i].name){
	        	if(iterAttrs[j].type == TypeVarChar){
	        		memcpy(&length, (char *)tempData + tempOffset, sizeof(int));
	        		memcpy((char*)data + dataOffset, (char *)tempData + tempOffset, sizeof(int)+length);
	        		tempOffset += sizeof(int)+length;
	        		dataOffset += sizeof(int)+length;
	        	}
	        	else{
	        		memcpy((char*)data + dataOffset, (char *)tempData + tempOffset, sizeof(int));
	        		tempOffset += sizeof(int);
	        		dataOffset += sizeof(int);
	        	}
	        }
	        else{
	        	if(iterAttrs[j].type == TypeVarChar){
	        		memcpy(&length, (char *)tempData + tempOffset, sizeof(int));
	        		tempOffset += sizeof(int) + length;
	        	}
	        	else{
	        		tempOffset += sizeof(int);
	        	}
	        }
	    }
    }
   	return SUCCESS;
}





















