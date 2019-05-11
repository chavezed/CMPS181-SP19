
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
    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int32_t checkSize;
    memcpy(&checkSize, checkString, VARCHAR_LENGTH_SIZE);
    char checkStr[checkSize + 1];
    checkStr[checkSize] = '\0';
    memcpy(checkStr, (char*) checkString + VARCHAR_LENGTH_SIZE, checkSize);

    int cmp = strcmp(checkStr, valueStr);

    return cmp >= 0;
    
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
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
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

