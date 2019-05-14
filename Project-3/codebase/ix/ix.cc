
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{   
    void* rootPage = malloc(PAGE_SIZE);
    void* leafPage = malloc(PAGE_SIZE);
    char rootChar = ROOT_CHAR;
    char leafChar = LEAF_CHAR;
    int offset = 4092;
    int freeSpaceRoot = 5;
    int freeSpaceLeaf = 13;

    if(_pf_manager->createFile(fileName) != 0){
        free(rootPage);
        free(leafPage);
        return -1;
    }

    memset(rootPage,0,PAGE_SIZE);
    memset(leafPage,0,PAGE_SIZE);
    memcpy((char*)rootPage, &rootChar, sizeof(char));
    memcpy((char*)leafPage, &leafChar, sizeof(char));
    memcpy((char*)rootPage+offset, &freeSpaceRoot, sizeof(int));
    memcpy((char*)leafPage+offset, &freeSpaceLeaf, sizeof(int));

    FileHandle fh;
    _pf_manager->openFile(fileName, fh);
    fh.appendPage(rootPage);
    fh.appendPage(leafPage);
    _pf_manager->closeFile(fh);

    free(rootPage);
    free(leafPage);
    return 0;
    // return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
    // return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    FileHandle pfmfh;
    _pf_manager->openFile(fileName,pfmfh);
    int pagesInFile = pfmfh.getNumberOfPages();
    char indicator[1];
    void* page = malloc(PAGE_SIZE);
    for(int i = 0; i < pagesInFile; i++){
        pfmfh.readPage(i, page);
        memcpy(indicator, page, sizeof(char));
        if(indicator[0] == ROOT_CHAR){
            ixfileHandle.rootPageNum = i;
            break;
        }
    }
    free(page);
    ixfileHandle.fh = pfmfh;
    if(ixfileHandle.rootPageNum == -1)
        return -1;
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    ixfileHandle.rootPageNum = -1;
    return _pf_manager->closeFile(ixfileHandle.fh);
}

bool IndexManager::checkCondition(int checkInt, const void *value){
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);
    return checkInt >= intValue;
}

bool IndexManager::checkCondition(float checkReal, const void *value){
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);
    return checkReal >= realValue;
}

//for string
bool IndexManager::checkCondition(void *checkString, const void *value){
    //for value
    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    //for check string
    int32_t checkSize;
    memcpy(&checkSize, checkString, VARCHAR_LENGTH_SIZE);
    char checkStr[checkSize + 1];
    checkStr[checkSize] = '\0';
    memcpy(checkStr, (char*) checkString + VARCHAR_LENGTH_SIZE, checkSize);

    int cmp = strcmp(checkStr, valueStr);

    return cmp >= 0; // checkString >= value
    
}

RC IndexManager::findLeaf(IXFileHandle &ixfileHandle, const Attribute att, int &pageNum, const void* val){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
    char indicator = ROOT_CHAR;
    int numSlots = 0;
    int nextPage = 0;
    void* key = malloc(PAGE_SIZE);
    memcpy(&numSlots, (char*)page + 4088, sizeof(int));
    while(indicator != LEAF_CHAR){
        if(numSlots == 0){
            memcpy(&nextPage, (char*)page+5, sizeof(int));
            pageNum = nextPage;
            ixfileHandle.readPage(nextPage, page);
            memcpy(&indicator, (char*)page, sizeof(char));
            memcpy(&numSlots, (char*)page + 4088, sizeof(int));
            continue;
        }
        int slotStart = 0;
        int length = 0;
        int offset = 0;
        bool condition = true;
        for(int i = 0; i < numSlots; i++){
            slotStart = 4080-(i*8);
            memcpy(&length, (char*)page+slotStart,sizeof(int));
            memcpy(&offset, (char*)page+slotStart+sizeof(int), sizeof(int));
            memcpy((char*)key, (char*)page+offset, length-sizeof(int));//subtract int because page pointer
            if(att.type == TypeVarChar){
                condition = checkCondition(key,val);
            }
            else if(att.type == TypeReal){
                float floatKey= 0;
                memcpy(&floatKey, key, sizeof(float));
                condition = checkCondition(floatKey,val);
            }
            else{
                int intKey= 0;
                memcpy(&intKey, key, sizeof(int));
                condition = checkCondition(intKey,val);
            }
            if(condition == true && i == numSlots-1){//last slot is still smaller
                //open page on right
                offset = offset + length-sizeof(int);//end of for loop, offset is reused.
                memcpy(&pageNum, (char*)page+offset, sizeof(int));
                memcpy(&nextPage, &pageNum, sizeof(int));
                ixfileHandle.readPage(nextPage, page);
                memcpy(&indicator, (char*)page, sizeof(char));
                memcpy(&numSlots, (char*)page + 4088, sizeof(int));
                break;
            }
            if(condition == false){
                offset = offset - sizeof(int);//end of for loop, offset is reused.
                memcpy(&pageNum, (char*)page+offset, sizeof(int));
                memcpy(&nextPage, &pageNum, sizeof(int));
                ixfileHandle.readPage(nextPage, page);
                memcpy(&indicator, (char*)page, sizeof(char));
                memcpy(&numSlots, (char*)page + 4088, sizeof(int));
                break;
            }

        }
    }
    free(page);
    free(key);
    return 0;
}

bool IndexManager::isSpaceLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    int offset = 4088;//start of number of records
    int freeOffset = 0;
    int numberOfRecord = 0;
    int recordSize = 0;

    memcpy(&numberOfRecord, (char *)page + offset, sizeof(int));
    memcpy(&freeOffset, (char *)page + offset + sizeof(int), sizeof(int));

    if(att.type != TypeVarChar){
        recordSize = sizeof(int) * 3; // key + rid.slotnumber + rid.pageNum
    }
    else {
        int length = 0;
        memcpy(&length, (char *)val, sizeof(int));
        recordSize = sizeof(int) * 3 + length; // rid.slotnumber + rid.pageNum + key.length + key
    }

    free(page);

    int totalSpaceUsed = freeOffset + recordSize + (numberOfRecord + 1) * (sizeof(int) * 2);

    return ( totalSpaceUsed < PAGE_SIZE ); 
}

bool IndexManager::isSpaceNonLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    int offset = 4088;//start of number of records
    int freeOffset = 0;
    int numberOfRecord = 0;
    int recordSize = 0;

    memcpy(&numberOfRecord, (char *)page + offset, sizeof(int));
    memcpy(&freeOffset, (char *)page + offset + sizeof(int), sizeof(int));

    if(att.type != TypeVarChar){
        recordSize = sizeof(int) * 2; // key + pointer to page
    }
    else {
        int length = 0;
        memcpy(&length, (char *)val, sizeof(int));
        recordSize = sizeof(int) * 2 + length; // pointer to page + key.length + key
    }

    free(page);

    int totalSpaceUsed = freeOffset + recordSize + (numberOfRecord + 1) * (sizeof(int) * 2);

    return ( totalSpaceUsed < PAGE_SIZE ); 
}

void IndexManager::insertToLeafSorted(IXFileHandle &ixfileHandle, const Attribute &att, const void *key, const RID &rid, PageNum pageID){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageID, page);
    int offset = 4088;//start of number of records
    int freeOffset = 0;
    int numberOfRecord = 0;
    int recordSize = 0;

    memcpy(&numberOfRecord, (char *)page + offset, sizeof(int));
    memcpy(&freeOffset, (char *)page + offset + sizeof(int), sizeof(int));

    if(att.type != TypeVarChar){
        recordSize = sizeof(int) * 3; // key + rid.slotnumber + rid.pageNum
        void * recordForInsert = malloc(recordSize);
        memset(recordForInsert, 0, recordSize);
        memcpy(recordForInsert, key, sizeof(int));
        memcpy((char*)recordForInsert + sizeof(int), &rid.pageNum, sizeof(int));
        memcpy((char*)recordForInsert + 2*sizeof(int), &rid.slotNum, sizeof(int));

        if(att.type == TypeInt){
            int compOffSet = 0;
            int slot;
            int compKey = 0;

            for(slot = 0; slot < numberOfRecord; slot++){
                offset -= sizeof(int) * 2; 
                memcpy(&compOffSet, (char*)page + offset + sizeof(int), sizeof(int));
                memcpy(&compKey, (char*)page + compOffSet, sizeof(int));

                if(checkCondition(compKey, key)){
                    break;
                }
            }
        }
        else{
            int compOffSet = 0;
            int slot;
            float compKey = 0;

            for(slot = 0; slot < numberOfRecord; slot++){
                offset -= sizeof(int) * 2; 
                memcpy(&compOffSet, (char*)page + offset + sizeof(int), sizeof(int));
                memcpy(&compKey, (char*)page + compOffSet, sizeof(int));

                if(checkCondition(compKey, key)){
                    break;
                }
            }
        }

    }
    else {

        int length = 0;
        memcpy(&length, (char *)key, sizeof(int));
        recordSize = sizeof(int) * 3 + length; // rid.slotnumber + rid.pageNum + key.length + key

        //creating void pointer for insertion, goes <key, rid.pageNum, rid.slotNum>
        void * recordForInsert = malloc(recordSize);
        memset(recordForInsert, 0, recordSize);
        memcpy(recordForInsert, key, sizeof(int)+length);
        memcpy((char*)recordForInsert + sizeof(int)+length, &rid.pageNum, sizeof(int));
        memcpy((char*)recordForInsert + 2*sizeof(int) + length, &rid.slotNum, sizeof(int));

        void * compKey = malloc(PAGE_SIZE);
        memset(compKey, 0, PAGE_SIZE);

        int compOffSet = 0;
        length = 0;
        int slot;

        for(slot = 0; slot < numberOfRecord; slot++){
            offset -= sizeof(int) * 2; 
            memcpy(&length, (char*)page + offset, sizeof(int));
            memcpy(&compOffSet, (char*)page + offset + sizeof(int), sizeof(int));

            memcpy(compKey, (char *) page + compOffSet, length);

            if(checkCondition(compKey, key)){
                break;
            }
        }
        free(compKey);
    }

    void * shift = malloc(freeOffset - compOffSet);
    memset(shift, 0, (freeOffset - compOffSet));

    //shift gets from key to free space offset.
    memcpy(shift, (char*)page + compOffSet, (freeOffset - compOffSet));
    //inserting key at correct spot.
    memcpy((char*)page + compOffSet, recordForInsert, recordSize);
    //inserting shift after key
    memcpy((char*)page + compOffSet + recordSize, shift, (freeOffset - compOffSet));

    //record upkeep: slot records
    //shift first
    int endOfSlots = 4088 - numberOfRecord * 2 * sizeof(int);
    memcpy(shift, (char*)page + endOfSlots, (endOfSlots-offset));
    memcpy((char*)page + endOfSlots - 2 * sizeof(int), shift, (endOfSlots-offset));
    memcpy((char*)page + endOfSlots, &recordSize, sizeof(int));
    memcpy((char*)page + endOfSlots + sizeof(int), &compOffSet, sizeof(int));

    //update number of records
    numberOfRecord += 1;
    memcpy((char*)page + 4088, numberOfRecord, sizeof(int));
    //update free space offset
    freeOffset += recordSize;
    memcpy((char*)page + 4092, freeOffset, sizeof(int));

    int oldOffset = 0;
    for(int i = slot + 1; i < numberOfRecord; i++){
        offset -= sizeof(int) * 2; 
        memcpy(&oldOffset, (char*)page + offset + sizeof(int), sizeof(int));
        oldOffset += recordSize;
        memcpy((char*)page + offset + sizeof(int), &oldOffset, sizeof(int));
    }
}

void IndexManager::splitLeaf(IXFileHandle &ixfileHandle, PageNum pageID, const void * key, const Attribute &att, const RID &rid){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageID, page);
    int offset = 4088;//start of number of records
    int freeOffset = 0;
    int numberOfRecord = 0;

    memcpy(&numberOfRecord, (char *)page + offset, sizeof(int));
    memcpy(&freeOffset, (char *)page + offset + sizeof(int), sizeof(int));

    int halfWay = freeOffset/2; 
    int halfOffset = 0;
    int slot;

    for(slot = 0; slot < numberOfRecord; slot++){
        offset -= 2* sizeof(int);
        memcpy(&halfOffset, (char*)page + offset + sizeof(int), sizeof(int));
        if (halfOffset > halfWay){
            offset += 2* sizeof(int);
            memcpy(&halfOffset, (char*)page + offset + sizeof(int), sizeof(int));
            break;
        }
    }
    //for pushing up the traffic cop.
    int lengthOfRecord = 0; 
    void * recordAtSplit = malloc(PAGE_SIZE);
    memcpy(&lengthOfRecord, (char*)page + offset, sizeof(int));
    memcpy(recordAtSplit, (char*)page + halfOffset, lengthOfRecord);
    /////////////////////////////////////////////////////////////////////////////////////

    //append page
    void* newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    int newPageOffset = 0;
    //set up page for appending
    //setting indicator
    char indicator = LEAF_CHAR;
    memcpy((char*)newPage, indicator, sizeof(char));
    newPageOffset += sizeof(char);

    //find parent of old page and set newPage parent to that.
    int parent = 0;
    memcpy(&parent, (char*)page + sizeof(char), sizeof(int));
    memcpy((char*)newPage + newPageOffset, &parent, sizeof(int));
    newPageOffset += sizeof(int);

    //set back to old page
    memcpy((char*)newPage + newPageOffset, &pageID, sizeof(int));
    newPageOffset += sizeof(int);

    //get old page forward, set new page forward page to old page forward page, and set old page forward to new page.
    int forwardPage = 0;
    memcpy(&forwardPage, (char*)page + newPageOffset, sizeof(int));
    memcpy((char*)newPage + newPageOffset, &forwardPage, sizeof(int));

    forwardPage = ixfileHandle.getNumberOfPages() + 1;
    memcpy((char*)page + newPageOffset, forwardPage, sizeof(int));

    newPageOffset += sizeof(int);

    //setting number of records and beginning of freespace for new page.
    slot += 1; //started at zero
    newNumberOfRecord = numberOfRecord - slot;
    memcpy((char*)newPage + 4088, &newNumberOfRecord, sizeof(int));

    int newPageFreeSpace = newPageOffset + (freeOffset - halfOffset);
    memcpy((char*)newPage + 4092, &newPageFreeSpace, sizeof(int));

    //setting number of records and beginning of freespace for old page.
    memcpy((char*)page + 4088, &slot, sizeof(int));
    memcpy((char*)newPage + 4092, &halfOffset, sizeof(int));

    //getting half of entries from old page and putting it in new page.
    memcpy((char*)newPage + newPageOffset, (char*)page + halfOffset, (freeOffset - halfOffset));

    int endOfSlots = 4088 - numberOfRecord * 2 * sizeof(int);
    newPageOffset = 4088 - (numberOfRecord - slot) * 2 * sizeof(int);

    memcpy((char*)newPage + newPageOffset, (char*)page + endOfSlots, (endOfSlots-offset));

    offset = 4088;
    int numberToSub = halfOffset - (3 * sizeof(int) + sizeof(char));
    int changeOffset = 0;
    for(int i = 0; i < slot; i++){
        offset -= 2* sizeof(int);
        memcpy(&changeOffset, (char*)newPage + offset + sizeof(int), sizeof(int));
        changeOffset -= numberToSub;
        memcpy((char*)newPage + offset + sizeof(int), &changeOffset, sizeof(int));
    }

    ixfileHandle.appendPage(newPage); 

    /////////////////////////////////////////////////////////////////////////////////////
    if(att.type == TypeVarChar){
        if(checkCondition(key, recordAtSplit)){
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
    }
    else if(att.type == TypeInt){
        int comp = 0;
        memcpy(&comp, key, sizeof(int));
        if(checkCondition(comp, recordAtSplit)){
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
    }
    else {
        float comp = 0;
        memcpy(&comp, key, sizeof(float));
        if(checkCondition(comp, recordAtSplit)){
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
    } 

    //Put traffic cop up here:

    free(page);
    free(keyAtSplit);
    free(newPage);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool            lowKeyInclusive,
        bool            highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){
    return fh.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
    return fh.writePage(pageNum, data);
} 

RC IXFileHandle::appendPage(const void *data){
    return fh.appendPage(data);
}

unsigned IXFileHandle::getNumberOfPages(){
    return fh.getNumberOfPages();
}  

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    ixReadPageCounter = fh.readPageCounter;
    ixWritePageCounter = fh.writePageCounter;
    ixAppendPageCounter = fh.appendPageCounter;
    readPageCount   = ixReadPageCounter;
    writePageCount  = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}