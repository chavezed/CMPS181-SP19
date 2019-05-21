
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
    int offsetRoot = 5;// char indicator + empty parent pointer
    int offsetLeaf = 13;// char indicator + parent pointer + back pointer + forward pointer
    int freeSpaceRoot = 9;//char indicator + empty parent pointer + freespace pointer
    int freeSpaceLeaf = 17;// char indicator + parent pointer + back pointer + forward pointer + freespace pointer

    if(_pf_manager->createFile(fileName) != 0){
        free(rootPage);
        free(leafPage);
        return -1;
    }

    memset(rootPage,0,PAGE_SIZE);
    memset(leafPage,0,PAGE_SIZE);
    memcpy((char*)rootPage, &rootChar, sizeof(char));
    memcpy((char*)leafPage, &leafChar, sizeof(char));
    memcpy((char*)rootPage+offsetRoot, &freeSpaceRoot, sizeof(int)); //placing free space pointer
    memcpy((char*)leafPage+offsetLeaf, &freeSpaceLeaf, sizeof(int)); //placing free space pointer
    //parent of leaf is root which is page zero at the moment.
    //root has no parent, but keeping the space open for when it gets split and turned into a interior node.
    // setting first pointer in root page to the first leaf page.
    int leafPageID = 1;
    memcpy((char*)rootPage+freeSpaceRoot, &leafPageID, sizeof(int));

    FileHandle fh;
    _pf_manager->openFile(fileName, fh);
    fh.appendPage(rootPage);
    fh.appendPage(leafPage);
    _pf_manager->closeFile(fh);

    free(rootPage);
    free(leafPage);
    return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    FileHandle pfmfh = ixfileHandle.fh;
    if(_pf_manager->openFile(fileName, pfmfh) != 0){
        return -1;
    }
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
    if(ixfileHandle.rootPageNum == -1){
        return -1;
    }
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    ixfileHandle.rootPageNum = -1;
    return _pf_manager->closeFile(ixfileHandle.fh);
}

RC IndexManager::checkCondition(int checkInt, const void *value){
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);
    if(checkInt == intValue){
        return 1;
    }
    else if (checkInt >= intValue){
        return 2;
    }
    else {
        return 3;
    }
}

RC IndexManager::checkCondition(float checkReal, const void *value){
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);
    if(checkReal == realValue){
        return 1;
    }
    else if (checkReal >= realValue){
        return 2;
    }
    else {
        return 3;
    }
}

//for string
RC IndexManager::checkCondition(void *checkString, const void *value){
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

    if(cmp == 0){
        return 1;
    }
    else if(cmp >= 0){
        return 2; // checkString >= value
    }
    else {
        return 3; // checkString < value
    }
}

RC IndexManager::findLeaf(IXFileHandle &ixfileHandle, const Attribute att, int &pageNum, const void* val){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
    int offset = 9; //start of free space
    int freeSpace = 0;
    char indicator = ROOT_CHAR;
    RC condition = 0;
    int nextPage = 0;
    void* key = malloc(PAGE_SIZE);
    memset(key, 0, PAGE_SIZE);
    int length = 0; // used to find length of char.

    memcpy(&freeSpace, (char*)page+5, sizeof(int));//5 because char indicator then parent pointer then freespace poiner

    while(indicator != LEAF_CHAR){ //loops through pages

        if(freeSpace == 9){//means that there are no entries in this page
            memcpy(&nextPage, (char*)page+9, sizeof(int));
            pageNum = nextPage;
            ixfileHandle.readPage(nextPage, page);
            memcpy(&indicator, (char*)page, sizeof(char));
            memcpy(&freeSpace, (char*)page+5, sizeof(int));//finds free space offset of page.
            continue;
        }

        while(freeSpace > offset && condition){ // loops through keys in page
            memcpy(&nextPage, (char*)page+offset, sizeof(int));
            offset += sizeof(int);
            if(att.type == TypeVarChar){
                memcpy(&length, (char*)page+offset, sizeof(int));
                memcpy(key, (char*)page+offset, sizeof(int) + length);
                
                condition = checkCondition(key,val); // returns bool

                if(condition == LESSTHAN){//if key < val then go to next page.
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    pageNum = nextPage;
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else if (freeSpace == (int)(offset + sizeof(int) + length)){// case where val is greater than last key on the page. 
                    memcpy((char*)page+offset, val, sizeof(int) + length);//dethrone last greatest key and try to place val in the leaf page
                    pageNum = nextPage;
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else {//go to the next key in the interior page.
                    offset += (sizeof(int) + length);
                }
            }
            else if(att.type == TypeReal){
                float floatKey= 0;
                memcpy(&floatKey, key, sizeof(float));
                condition = checkCondition(floatKey,val);
                if(condition == LESSTHAN){//if false then go to next page.
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    pageNum = nextPage;
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else if (freeSpace == (int)(offset + sizeof(int) + length)){// case where val is greater than last key on the page. 
                    memcpy((char*)page+offset, val, sizeof(int));//dethrone last greatest key and try to place val in the leaf page
                    pageNum = nextPage;
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else {//go to the next key in the interior page.
                    offset += sizeof(int);
                }
            }
            else{
                int intKey= 0;
                memcpy(&intKey, key, sizeof(int));
                condition = checkCondition(intKey,val);

                if(condition == LESSTHAN){//if false then go to next page.
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    pageNum = nextPage;
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else if (freeSpace == (int)(offset + sizeof(int) + length)){// case where val is greater than last key on the page. 
                    memcpy((char*)page+offset, val, sizeof(int));//dethrone last greatest key and try to place val in the leaf page
                    pageNum = nextPage;
                    ixfileHandle.readPage(ixfileHandle.rootPageNum, page);
                    memset(key, 0, PAGE_SIZE);
                    offset = 9;
                    memcpy(&freeSpace, (char*)page+5, sizeof(int));
                    memcpy(&indicator, (char*)page, sizeof(char));
                    break;
                }
                else {//go to the next key in the interior page.
                    offset += sizeof(int);
                }
            }
        }  
    }
    free(page);
    free(key);
    return 0;
}

bool IndexManager::isSpaceLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val){
    void* page = malloc(PAGE_SIZE);
    int freeOffset = 0;
    int recordSize = 0;

    memcpy(&freeOffset, (char *)page + 13, sizeof(int));

    if(att.type != TypeVarChar){
        recordSize = sizeof(int) * 3; // key + rid.slotnumber + rid.pageNum
    }
    else {
        int length = 0;
        memcpy(&length, (char *)val, sizeof(int));
        recordSize = sizeof(int) * 3 + length; // rid.slotnumber + rid.pageNum + key.length + key
    }

    free(page);

    int totalSpaceUsed = freeOffset + recordSize;

    return ( totalSpaceUsed < PAGE_SIZE ); 
}

bool IndexManager::isSpaceNonLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    int freeOffset = 0;
    int recordSize = 0;

    memcpy(&freeOffset, (char *)page + 5, sizeof(int));
    free(page);

    if(att.type != TypeVarChar){
        recordSize = sizeof(int) * 2; // key + pointer to page
    }
    else {
        int length = 0;
        memcpy(&length, (char *)val, sizeof(int));
        recordSize = sizeof(int) * 2 + length; // pointer to page + key.length + key
    }

    

    int totalSpaceUsed = freeOffset + recordSize;

    return ( totalSpaceUsed < PAGE_SIZE ); 
}

void IndexManager::insertToLeafSorted(IXFileHandle &ixfileHandle, const Attribute &att, const void *key, const RID &rid, PageNum pageID){
    void* page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageID, page);
    int offset = 17;//start of free space
    int freeOffset = 0;
    int recordSize = 0;
    int numberOfRIDs = 0;
    RC condition = 0;

    memcpy(&freeOffset, (char *)page + 13, sizeof(int));


    //This if statement finds the spot where the insert needs to go.
    if(att.type == TypeInt){
        int compKey = 0;

        while(offset < freeOffset){ // will break if it finds the spot
            memcpy(&numberOfRIDs, (char*)page + offset, sizeof(int));

            //find size of entire record.
            recordSize = 2 * sizeof(int) + numberOfRIDs * (2 * sizeof(int)); // numberOfRIDs + key + numberOfRIDs * (rid.pageNum + rid.slotNum)

            //get the key of the record.
            memcpy(&compKey, (char*)page + offset + sizeof(int), sizeof(int));

            condition = checkCondition(compKey, key);
            if(condition != LESSTHAN){
                break;
            }
            //move offset past record in prep to get next record.
            offset += recordSize; 
        }
    }
    else if (att.type == TypeReal){
        float compKey = 0;

        while(offset < freeOffset){ // will break if it finds the spot
            memcpy(&numberOfRIDs, (char*)page + offset, sizeof(int));

            //find size of entire record.
            recordSize = 2 * sizeof(int) + numberOfRIDs * (2 * sizeof(int)); // numberOfRIDs + key + numberOfRIDs * (rid.pageNum + rid.slotNum)

            memcpy(&compKey, (char*)page + offset + sizeof(float), sizeof(float)); 

            condition = checkCondition(compKey, key);
            if(condition != LESSTHAN){
                break;
            }
            //move offset past record in prep to get next record.
            offset += recordSize;
        }
    }
    else {
        void * compKey = malloc(PAGE_SIZE);
        memset(compKey, 0, PAGE_SIZE);
        int length = 0;

        while(offset < freeOffset){ // will break if it finds the spot
            memcpy(&numberOfRIDs, (char*)page + offset, sizeof(int));
            memcpy(&length, (char*)page + offset + sizeof(int), sizeof(int));

            recordSize = 2 * sizeof(int) + length + numberOfRIDs * (2 * sizeof(int)); // numberOfRIDs + length + key + numberOfRIDs * (rid.pageNum + rid.slotNum)

            memcpy(compKey, (char *) page + offset + sizeof(int), length + sizeof(int)); // get lenght of key and key

            condition = checkCondition(compKey, key);
            if(condition == LESSTHAN){
                break;
            }
            offset += recordSize;
        }
        free(compKey);
    }

    int sizeofShift = freeOffset - offset + recordSize;
    void * shift = malloc(sizeofShift);
    memset(shift, 0, sizeofShift);

    //shift gets from end of compKey to free space offset.
    memcpy(shift, (char*)page + offset + recordSize, sizeofShift);

    //inserting key at correct spot.
    if(condition == EQUAL){
        //update the number of rids associated with the key.
        numberOfRIDs += 1;
        memcpy((char*)page + offset, &numberOfRIDs, sizeof(int));
        //place rid at end of record.
        offset += recordSize;
        memcpy((char*)page + offset, &rid.pageNum, sizeof(int));
        memcpy((char*)page + offset + sizeof(int), &rid.slotNum, sizeof(int));
        offset += 2*sizeof(int);

    }
    else{
        offset += recordSize;
        numberOfRIDs = 1; //reusing this for new record.
        memcpy((char*)page + offset, &numberOfRIDs, sizeof(int));
        offset += sizeof(int);

        if (att.type == TypeVarChar){
            int length = 0;
            memcpy(&length, key, sizeof(int));
            memcpy((char*)page + offset, key, sizeof(int) + length);
            offset += sizeof(int) + length;
        }
        else {
            memcpy((char*)page + offset, key, sizeof(int));
            offset += sizeof(int);
        }

        memcpy((char*)page + offset, &rid.pageNum, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)page + offset, &rid.slotNum, sizeof(int));
        offset += sizeof(int);
    }
    //inserting shift after key
    memcpy((char*)page + offset, shift, sizeofShift);
    ixfileHandle.writePage(pageID, page);
}

void IndexManager::splitLeaf(IXFileHandle &ixfileHandle, PageNum pageID, const void * key, const Attribute &att, const RID &rid){
    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageID, page);
    void * keyAtSplit = malloc(PAGE_SIZE); //for pushing up the traffic cop.
    int offset = 17;
    int freeOffset = 0;
    int numberOfRIDs = 0;
    int length = 0;

    memcpy(&freeOffset, (char *)page + 13, sizeof(int));

    int halfWay = freeOffset/2; 

    while(offset < halfWay){
        memcpy(&numberOfRIDs, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        if(att.type == TypeVarChar){
            memcpy(&length, (char*)page + offset, sizeof(int));
            memcpy(keyAtSplit, (char*)page + offset, sizeof(int) + length); //info for traffic cop
            offset += sizeof(int) + length;
        }
        else {
            memcpy(keyAtSplit, (char*)page + offset, sizeof(int)); //info for traffic cop
            offset += sizeof(int);
        }

    }
    
    /////////////////////////////////////////////////////////////////////////////////////

    //append page
    void* newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    int newPageOffset = 0;
    //set up page for appending
    //setting indicator
    char indicator = LEAF_CHAR;
    memcpy((char*)newPage, &indicator, sizeof(char));
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
    memcpy((char*)page + newPageOffset, &forwardPage, sizeof(int));
    newPageOffset += sizeof(int);

    //setting beginning of freespace for new page.
    int newPageFreeSpace = newPageOffset + sizeof(int) + (freeOffset - offset); //header + the old page's freeOffset - the old page's offset
    memcpy((char*)newPage + newPageOffset, &newPageFreeSpace, sizeof(int));
    newPageOffset += sizeof(int);

    //getting half of entries from old page and putting it in new page.
    memcpy((char*)newPage + newPageOffset, (char*)page + offset, (freeOffset - offset));

    ixfileHandle.appendPage(newPage); 

    /////////////////////////////////////////////////////////////////////////////////////
    //insert key into the correct page
    if(att.type == TypeVarChar){
        if(checkCondition(keyAtSplit, key)){
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
    }
    else if(att.type == TypeInt){
        int comp = 0;
        memcpy(&comp, keyAtSplit, sizeof(int));
        if(checkCondition(comp, key)){
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
    }
    else {
        float comp = 0;
        memcpy(&comp, keyAtSplit, sizeof(float));
        if(checkCondition(comp, key)){
            insertToLeafSorted(ixfileHandle, att, key, rid, pageID);
        }
        else{
            insertToLeafSorted(ixfileHandle, att, key, rid, ixfileHandle.getNumberOfPages());
        }
    } 

    //Put traffic cop up here:

    free(page);
    free(keyAtSplit);
    free(newPage);
}

// * * * * * * * * * * * * * * * * * * * * * * * * * * 
// start of push traffic cop up

void IndexManager::pushTrafficCopUp (IXFileHandle &ixfileHandle, void *key, Attribute attr, int pageNum, int childPage) {
    int freeSpace = getPageFreeSpace (pageNum, ixfileHandle);
    int keySize = getKeySize (key, attr);

    void *page = malloc (PAGE_SIZE);
    ixfileHandle.readPage (pageNum, page);

    int freeSpaceOffset = 0;
    memcpy (&freeSpaceOffset, (char*)page + 5, sizeof(int));

    // check if key + pagepointer fits in page
    // if so enter into page and exit, no need to split page
    if (keySize + sizeof(int) <= freeSpace) {
        int offset = 13;
        
        insertIntoInternal (page, key, attr, offset, freeSpaceOffset, childPage);
        ixfileHandle.writePage(pageNum, page);
        free (page);
        return;
    }

    // else key doesn't fit in page, handle split
    else {
        int offset = 13;

        // find how many key entries in page
        int count = 0;
        while (offset < freeSpaceOffset) {
            count += 1;
            if (attr.type == TypeVarChar) {
                int length = 0;
                memcpy (&length, (char*)page + offset, sizeof(int));
                offset += sizeof(int) + length + sizeof(int);
            }
            else {
                offset += sizeof(int) + sizeof(int); // key size (int or float) + pagePointer
            }
        }

        int halfEntriesCount = count / 2;
        offset = 13;
        int i;

        // advance offset to middle key
        for (i = 0; i < halfEntriesCount; ++i) {
            if (attr.type == TypeVarChar) {
                int length = 0;
                memcpy (&length, (char*)page + offset, sizeof(int));
                offset += sizeof(int) + length + sizeof(int);
            }
            else {
                offset += sizeof(int) + sizeof(int); // key size (int or float) + pagePointer
            }
        }

        // grab length of middle key plus page pointer
        int middleKeyLen = 0;
        if (attr.type != TypeVarChar) {
            middleKeyLen = sizeof(int) + sizeof(int); // key(int or float) + pagePointer
        }
        else {
            memcpy (&middleKeyLen, (char*)page + offset, sizeof(int));
            middleKeyLen += sizeof(int) + sizeof(int); // key (length + string) + pagePointer
        }

        // grab middle key plus page pointer; update offset past middle key
        void *middleKey = malloc (middleKeyLen);
        memcpy (middleKey, (char*)page + offset, middleKeyLen);
        offset += middleKeyLen;

        // check if key less than middle key to determine where key belongs
        // left page or the split page
        int condition = 0;
        void *midKey = malloc (middleKeyLen - 4); // exclude page pointer
        memcpy (midKey, middleKey, middleKeyLen - 4);

        if (attr.type == TypeVarChar) {
            condition = checkCondition (key, midKey);   
        }
        else if (attr.type == TypeInt) {
            int intKey = 0;
            memcpy (&intKey, key, sizeof(int));
            condition = checkCondition (intKey, midKey);
        }
        else { // TypeReal
            float floatKey = 0;
            memcpy (&floatKey, key, sizeof(float));
            condition = checkCondition (floatKey, midKey);
        }

        free (midKey);

        // set up the split page; copy over second half over to split page
        int splitPageNum = ixfileHandle.getNumberOfPages();

        void *splitPage = malloc (PAGE_SIZE);
        // insert page type
        char indicator = INTERNAL_CHAR;
        memcpy(splitPage, &indicator, sizeof(char));
        // set up parent page
        int parent = 0;
        memcpy (&parent, (char*)page + 1, sizeof(int));
        memcpy ((char*)splitPage + 1, &parent, sizeof(int));
        // set up leftmost pagePointer (first child pageNum)
        int firstChildPageNum = 0;
        memcpy (&firstChildPageNum, (char*)middleKey + middleKeyLen - 4, sizeof(int));
        memcpy ((char*)splitPage + 9, &firstChildPageNum, sizeof(int));

        // copy right half of index entries to split page (except middle key)
        // offset points past middle key
        int splitEntriesLength = freeSpaceOffset - offset;
        memcpy((char*)splitPage + 13, (char*)page + offset, splitEntriesLength);
        int splitFreeSpaceOffset = 13 + splitEntriesLength;
        // update freespace offset on split page
        memcpy ((char*)splitPage + 5, &splitFreeSpaceOffset, sizeof(int));

        // update page freespace offset
        // remove split half plus middle key
        freeSpaceOffset -= (splitEntriesLength + middleKeyLen);
        memcpy((char*)page + 5, &freeSpaceOffset, sizeof(int));

        // clear moved index entries from page
        // offset points one entry past middle key so offset to start of middle key
        // to clear entries from middle key onwards
        memset ((char*)page + offset - middleKeyLen, 0, middleKeyLen + splitEntriesLength);

        offset = 13;
        if (condition == LESSTHAN) { // insert key in left half
            memcpy (&freeSpaceOffset, (char*)page + 5, sizeof(int));

            insertIntoInternal (page, key, attr, offset, freeSpaceOffset, childPage); 
        }
        else { // insert key in right half of split
            memcpy (&freeSpaceOffset, (char*)splitPage + 5, sizeof(int));

            insertIntoInternal (splitPage, key, attr, offset, freeSpaceOffset, childPage);
        }

        // commit changes so far to disk
        ixfileHandle.writePage(pageNum, page);
        ixfileHandle.appendPage(splitPage);

        // check if page that was split was the root
        char pageIndicator;
        memcpy (&pageIndicator, page, sizeof(char));

        // key size passed in = PAGE_SIZE
        // so copy over middle key to key
        // and free allocated mem for middle key
        // key now middle key that needs to be pushed up
        memcpy (key, middleKey, middleKeyLen - 4);
        free (middleKey);

        if (pageIndicator == ROOT_CHAR) {
            // set up root page
            // since we appended above, getNumberOfPages gives us the correct page number
            // since page numbers start at 0
            int rootPageNum = ixfileHandle.getNumberOfPages();
            void *newRootPage = malloc (PAGE_SIZE);
            memset (newRootPage, 0, PAGE_SIZE);
            // set indicator to root page
            memcpy (newRootPage, &pageIndicator, sizeof(char));
            // set freespaceoffset of new root
            int rootFreeSpaceOffset = 13;
            memcpy ((char*)newRootPage + 5, &rootFreeSpaceOffset, sizeof(int));
            // set the leftmost child of the new child to point to page
            memcpy ((char*)newRootPage + 9, &pageNum, sizeof(int));

            // update parents of page and splitPage to the rootPageNum
            memcpy ((char*)page + 1, &rootPageNum, sizeof(int));
            memcpy ((char*)splitPage + 1, &rootPageNum, sizeof(int));

            // update page to be interior page. remove root indicator
            pageIndicator = INTERNAL_CHAR;
            memcpy(page, &pageIndicator, sizeof(char));

            // update rootPageNum of file
            ixfileHandle.rootPageNum = rootPageNum;

            ixfileHandle.appendPage (newRootPage);
            ixfileHandle.writePage (pageNum, page);
            ixfileHandle.writePage (splitPageNum, splitPage);

            free (page);
            free (splitPage);
            free (newRootPage);

            pushTrafficCopUp (ixfileHandle, key, attr, rootPageNum, splitPageNum);
        }
        else {
            free (page);
            free (splitPage);

            pushTrafficCopUp (ixfileHandle, key, attr, parent, splitPageNum);
        }
    }
}

// * * * * * * * * * * * * * * *
// helpers for pushTrafficCopUp *
// * * * * * * * * * * * * * * *
int IndexManager::getKeySize (void *key, Attribute attr) {
    int keySize = sizeof(int); // works for both int and float since size is 4
    if (attr.type == TypeVarChar) {
        int length = 0;
        memcpy (&length, key, sizeof(int));
        keySize += length;
    }
    return keySize;
}

int IndexManager::getPageFreeSpace (int pageNum, IXFileHandle &ixfileHandle) {
    void *page = malloc (PAGE_SIZE);
    ixfileHandle.readPage (pageNum, page);
    int freeSpaceOffset = 0;
    memcpy ((char*)page + 5, &freeSpaceOffset, sizeof(int));
    free (page);
    return PAGE_SIZE - freeSpaceOffset;
}

// pass offset past meta data
void IndexManager::insertIntoInternal (void *page, void *key, Attribute attr, int offset, int freeSpaceOffset, int childPage) {
    int condition;
    bool found = false;
    int keySize = getKeySize (key, attr);

    // find offset to insert key in page
    while (offset < freeSpaceOffset) {
        if (attr.type == TypeVarChar) {
            int length = 0;
            memcpy (&length, (char*)page + offset, sizeof(int));
            void *innerKey = malloc (sizeof(int) + length);
            memcpy (innerKey, (char*)page + offset, sizeof(int) + length);

            condition = checkCondition (key, innerKey);
            if (condition == LESSTHAN) {
                free (innerKey);
                found = true;
                break;
            }
            offset += sizeof(int) + length + sizeof(int); // length + string + pagePointer
            free (innerKey);
        }
        else if (attr.type == TypeInt) {
            void *innerKey = malloc (sizeof(int));
            memcpy (innerKey, (char*)page + offset, sizeof(int));

            int intKey = 0;
            memcpy (&intKey, key, sizeof(int));

            condition = checkCondition (intKey, innerKey);
            if (condition == LESSTHAN) {
                free (innerKey);
                found = true;
                break;
            }
            offset += sizeof(int) + sizeof(int); // key + pagePointer
            free (innerKey);
        }
        else {
            void *innerKey = malloc (sizeof(float));
            memcpy (innerKey, (char*)page + offset, sizeof(float));

            float floatKey = 0;
            memcpy (&floatKey, key, sizeof(float));

            condition = checkCondition (floatKey, innerKey);
            if (condition == LESSTHAN) {
                free (innerKey);
                found = true;
                break;
            }
            offset += sizeof(float) + sizeof(int); // key + pagePointer
            free (innerKey);
        }
    }

    if (not found) { // append new entry
        memcpy ((char*)page + offset, key, keySize);
        offset += keySize;
        memcpy((char*)page + offset, &childPage, sizeof(int));
        offset += sizeof(int);
        // update freespaceoffset
        memcpy ((char*)page + 5, &offset, sizeof(int));
    }
    else { // move stuff back to make space for new entry
        int shiftSize = freeSpaceOffset - offset;
        void *shift = malloc (shiftSize);
        // copy shift entries in shift
        memcpy(shift, (char*)page + offset, shiftSize);
        // place key in correct location
        memcpy ((char*)page + offset, key, keySize);
        offset += keySize;
        // place childPage pointer after key
        memcpy ((char*)page + offset, &childPage, sizeof(int));
        offset += sizeof(int);

        // place shifted entries back in page after key + pagePointer
        memcpy((char*)page + offset, shift, shiftSize);
        offset += shiftSize;

        // update freeSpaceOffset
        memcpy ((char*)page + 5, &offset, sizeof(int));
        free (shift);
    }
}

// * * * * * * * * * * * * * * * * * * * * * * * * * * 

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int pageNum = 0;
    findLeaf(ixfileHandle, attribute, pageNum, key);
    bool placeable = isSpaceLeaf(ixfileHandle, pageNum, attribute, key);
    if(placeable){
        insertToLeafSorted(ixfileHandle, attribute, key, rid, pageNum);
    }
    else{
        splitLeaf(ixfileHandle, pageNum, key, attribute, rid);
    }
    return SUCCESS;
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

void IndexManager::printRecursive(IXFileHandle &ixfileHandle, const Attribute &att, int pageNum, int tabs) const{
    // set tabs length
    char tempSpaces[tabs*4 + 1];
    memset(tempSpaces, ' ', tabs*4);
    tempSpaces[tabs*4+ 1] = '\0';
    string spaces=string(tempSpaces);

    void* page = malloc(PAGE_SIZE);
    memset((char*)page, 0, PAGE_SIZE);
    ixfileHandle.readPage(pageNum,page);
    char indicatorChar;
    memcpy(&indicatorChar, (char*)page, sizeof(char));
    int parent=0;
    int offset = sizeof(char);
    if(indicatorChar == LEAF_CHAR){
        int back = 0;
        int forward = 0;
        int freeSpaceOffset = 0;
        memcpy(&parent, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&back, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&forward, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&freeSpaceOffset, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        int numRIDs = 0;

        while(offset < freeSpaceOffset){
            memcpy(&numRIDs, (char*)page + offset , sizeof(int));
            offset += sizeof(int);
            if(att.type == TypeVarChar){
                cout << spaces << "{\"keys\":[";
                while(offset < freeSpaceOffset){
                    int keySize=0;
                    int numKeys = 0;
                    memcpy(&numKeys, (char*)page + offset, sizeof(int));
                    offset += sizeof(int);
                    memcpy(&keySize, (char*)page + offset, sizeof(int));
                    offset += sizeof(int);
                    string stringkeyVal[keySize];
                    memcpy(&stringkeyVal, (char*)page + offset, keySize);
                    offset += keySize;
                    cout<<"\""<<stringkeyVal<<"\":[";
                    
                    for(int i=0; i<numKeys; i++){
                        int ridPageNum=0;
                        int ridSlotNum=0;
                        memcpy(&ridPageNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        memcpy(&ridSlotNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        cout<<"("<<ridPageNum<<","<<ridSlotNum<<")";
                        if((i+1)<numKeys)
                            cout<<",";
                    }
                    cout<<"]}";
                    if(offset < freeSpaceOffset)
                        cout<<",\n";
                    else cout<<"\n";
                }  
            }
            else if(att.type == TypeReal){
                cout << spaces << "{\"keys\":[";
                while(offset < freeSpaceOffset){
                    int numKeys = 0;
                    memcpy(&numKeys, (char*)page + offset, sizeof(int));
                    offset += sizeof(int);
                    float keyVal=0;
                    memcpy(&keyVal, (char*)page + offset, sizeof(float));
                    offset += sizeof(float);
                    cout<<"\""<<keyVal<<"\":[";
                    
                    for(int i=0; i<numKeys; i++){
                        int ridPageNum=0;
                        int ridSlotNum=0;
                        memcpy(&ridPageNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        memcpy(&ridSlotNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        cout<<"("<<ridPageNum<<","<<ridSlotNum<<")";
                        if((i+1)<numKeys)
                            cout<<",";
                    }
                    cout<<"]}";
                    if(offset < freeSpaceOffset)
                        cout<<",\n";
                    else cout<<"\n";
                }
            }
            else{
                cout << spaces << "{\"keys\":[";
                while(offset < freeSpaceOffset){
                    int numKeys = 0;
                    memcpy(&numKeys, (char*)page + offset, sizeof(int));
                    offset += sizeof(int);
                    float floatkeyVal=0;
                    memcpy(&floatkeyVal, (char*)page + offset, sizeof(float));
                    offset += sizeof(float);
                    cout<<"\""<<floatkeyVal<<"\":[";
                    
                    for(int i=0; i<numKeys; i++){
                        int ridPageNum=0;
                        int ridSlotNum=0;
                        memcpy(&ridPageNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        memcpy(&ridSlotNum,(char*)page + offset, sizeof(int));
                        offset += sizeof(int);
                        cout<<"("<<ridPageNum<<","<<ridSlotNum<<")";
                        if((i+1)<numKeys)
                            cout<<",";
                    }
                    cout<<"]}";
                    if(offset < freeSpaceOffset)
                        cout<<",\n";
                    else cout<<"\n";
                }

            }
        }
    }
    else{//non-leaf page
        int freeSpaceOffset = 0;
        memcpy(&parent, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&freeSpaceOffset, (char*)page + offset, sizeof(int));
        offset += sizeof(int);
        int tempOffset = offset;
        tempOffset += sizeof(int);//temp offset used for keys
        
        if(att.type == TypeVarChar){
            cout << spaces << "{\"keys\":[";
            while(tempOffset < freeSpaceOffset){
                int keySize = 0;
                memcpy(&keySize, (char*)page+offset, sizeof(int));
                tempOffset += sizeof(int);
                string keyVal[keySize+1];
                memcpy(&keyVal, (char*)page+offset, keySize);
                tempOffset += keySize;
                tempOffset += sizeof(int);
                cout<<"\""<<keyVal<<"\"";
                if(tempOffset<freeSpaceOffset)
                cout<<",";
            }
            cout<<"],\n";
            cout<<spaces<<" \"children\": [\n";
            while(offset < freeSpaceOffset){
                int newPageNum = 0;
                memcpy(&newPageNum, (char*)page+offset, sizeof(int));
                offset += sizeof(int);
                printRecursive(ixfileHandle, att, newPageNum, tabs+1);
                int keySize = 0;
                memcpy(&keySize, (char*)page+offset, sizeof(int));
                offset += sizeof(int);
                // string keyVal[keySize+1];
                // memcpy(&keyVal, (char*)page+offset, keySize);
                offset += keySize;
                cout<<spaces<<"]}";
                if(offset < freeSpaceOffset)
                    cout<<",\n";
                else cout<<"\n";    
            }
            
        }
        else if(att.type == TypeReal){
            cout << spaces << "{\"keys\":[";
            while(tempOffset < freeSpaceOffset){
                float key = 0;
                memcpy(&key, (char*)page+offset, sizeof(int));
                tempOffset += sizeof(int);
                tempOffset += sizeof(int);
                cout<<"\""<<key<<"\"";
                if(tempOffset<freeSpaceOffset)
                cout<<",";
            }
            cout<<"],\n";
            cout<<spaces<<" \"children\": [\n";
            while(offset < freeSpaceOffset){
                int newPageNum = 0;
                memcpy(&newPageNum, (char*)page+offset, sizeof(int));
                offset += sizeof(int);
                printRecursive(ixfileHandle, att, newPageNum, tabs+1);
                offset += sizeof(int);
                cout<<spaces<<"]}";
                if(offset < freeSpaceOffset)
                    cout<<",\n";
                else cout<<"\n";
            }
        }
        else{//TypeInt
            cout << spaces << "{\"keys\":[";
            while(tempOffset < freeSpaceOffset){
                int key = 0;
                memcpy(&key, (char*)page+offset, sizeof(int));
                tempOffset += sizeof(int);
                tempOffset += sizeof(int);
                cout<<"\""<<key<<"\"";
                if(tempOffset<freeSpaceOffset)
                    cout<<",";
            }
            cout<<"],\n";
            cout<<spaces<<" \"children\": [\n";
            while(offset < freeSpaceOffset){
                int newPageNum = 0;
                memcpy(&newPageNum, (char*)page+offset, sizeof(int));
                offset += sizeof(int);
                printRecursive(ixfileHandle, att, newPageNum, tabs+1);
                offset += sizeof(int);
                cout<<spaces<<"]}";
                if(offset < freeSpaceOffset)
                    cout<<",\n";
                else cout<<"\n";
            }
        }
    }
    free(page);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    printRecursive(ixfileHandle, attribute, ixfileHandle.rootPageNum, ZERO);
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