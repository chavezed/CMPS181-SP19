
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
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





















