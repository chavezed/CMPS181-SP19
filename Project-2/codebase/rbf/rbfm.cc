#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

bool sortByDecreasingOffset (const SlotDirectoryRecordEntry &lhs, const SlotDirectoryRecordEntry &rhs);
void setBit(char *nullIndicator, int field);

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    unsigned targetSlotNum;
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            SlotDirectoryHeader tempSlotHeader = getSlotDirectoryHeader (pageData);
            // find a dead slot; if none, append new slot
            for (targetSlotNum = 0; targetSlotNum < tempSlotHeader.recordEntriesNumber; ++targetSlotNum) {
                SlotDirectoryRecordEntry tempSlotEntry = getSlotDirectoryRecordEntry (pageData, targetSlotNum);
                if (tempSlotEntry.length == 0 and tempSlotEntry.offset == 0)
                    break;
            }
            pageFound = true;
            break;
        }
    }


    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
        targetSlotNum = 0;
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);


    // Setting up the return RID.
    rid.pageNum = i;
    rid.slotNum = targetSlotNum;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;

    // only increment records in page if slot needs to be appended or new page was created
    if (not pageFound or slotHeader.recordEntriesNumber == targetSlotNum)
        slotHeader.recordEntriesNumber += 1;

    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // check if record has been moved or deleted
    if (recordEntry.length == 0 and recordEntry.offset == 0) {
        free (pageData);
        return RBFM_RECORD_DELETED;
    }
    else if (recordEntry.offset <= 0) {
        RID newRID;
        newRID.pageNum = recordEntry.length;
        newRID.slotNum = -1 * recordEntry.offset;
        free(pageData);
        return readRecord (fileHandle, recordDescriptor, newRID, data);
    }
    else {
        // Retrieve the actual entry data
        getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

        free(pageData);
        return SUCCESS;
    }
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void* page = malloc (PAGE_SIZE);
    fileHandle.readPage (rid.pageNum, page);
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry (page, rid.slotNum);
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);

    if (recordEntry.length == 0 and recordEntry.offset == 0) {
        free (page);
        return RBFM_RECORD_DELETED;
    }

    // check if the slot entry is the last one in the directory
    // can be deleted since it will not affect slotNum of RIDs
    if (rid.slotNum == (unsigned)(slotHeader.recordEntriesNumber - 1)) {
        slotHeader.recordEntriesNumber -= 1;
        setSlotDirectoryHeader (page, slotHeader);
    }

    else if (recordEntry.offset <= 0) {
        RID newRID;
        newRID.pageNum = recordEntry.length;
        newRID.slotNum = -1 * recordEntry.offset;

        // mark the slot entry as deleted
        recordEntry.length = 0;
        recordEntry.offset = 0;
        setSlotDirectoryRecordEntry (page, rid.slotNum, recordEntry);

        fileHandle.writePage(rid.pageNum, page);
        free (page);
        return deleteRecord (fileHandle, recordDescriptor, newRID);
    }
    // record is in current page since offset > 0
    else {
        compactRecords (page, slotHeader, recordEntry, recordEntry.offset + recordEntry.length);

        // set slot to DELETED
        recordEntry.length = 0;
        recordEntry.offset = 0;
        setSlotDirectoryRecordEntry (page, rid.slotNum, recordEntry);

        fileHandle.writePage (rid.pageNum, page);
        free(page);
        return SUCCESS;
    }

    // should never reach here
    return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    void* page = malloc (PAGE_SIZE);
    fileHandle.readPage (rid.pageNum, page);

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader (page);
    if (slotHeader.recordEntriesNumber < rid.slotNum) {
        free (page);
        return RBFM_SLOT_DN_EXIST;
    }

    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry (page, rid.slotNum);
    if (recordEntry.length == 0 and recordEntry.offset == 0) {
        free(page);
        return RBFM_RECORD_DELETED;
    }
    // record was moved
    else if (recordEntry.offset <= 0) {
        RID newRID;
        newRID.pageNum = recordEntry.length;
        newRID.slotNum = -1 * recordEntry.offset;
        free(page);
        return updateRecord (fileHandle, recordDescriptor, data, newRID);
    }
    // record in current page
    else if (recordEntry.offset > 0) {
        unsigned pageFreeSpaceSize = getPageFreeSpaceSize (page);
        unsigned recordSize = getRecordSize (recordDescriptor, data);

        // 3 cases:
        // 1) updatedRecordLength == currentRecordLength, update in place
        // 2) updatedRecordLength < currentRecordLength, compact
        // 3) updatedRecordLength > currentRecordLength
        //    a) if enough freeSpace, remove record, compact, append record
        //    b) else, find a page with free space, append record

        if (recordSize == recordEntry.length) {
            setRecordAtOffset(page, recordEntry.offset, recordDescriptor, data);
            fileHandle.writePage (rid.pageNum, page);
            free (page);
            return SUCCESS;
        }
        else if (recordSize < recordEntry.length) {
            recordEntry.offset = recordEntry.offset + recordEntry.length - recordSize;
            setRecordAtOffset (page, recordEntry.offset, recordDescriptor, data);
            recordEntry.length = recordSize;
            compactRecords (page, slotHeader, recordEntry, recordEntry.offset);
            setSlotDirectoryRecordEntry (page, rid.slotNum, recordEntry);

            fileHandle.writePage (rid.pageNum, page);
            free (page);
            return SUCCESS;
        }
        // recordSize > recordEntry.length
        else if (recordSize > recordEntry.length) {
            // record fits in the page
            // delete it from its current position
            compactRecords (page, slotHeader, recordEntry, recordEntry.offset + recordEntry.length);
            if (recordSize <= pageFreeSpaceSize) {
                recordEntry.length = recordSize;
                recordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
                setRecordAtOffset (page, recordEntry.offset, recordDescriptor, data);

                slotHeader.freeSpaceOffset = recordEntry.offset;
                setSlotDirectoryHeader (page, slotHeader);
                setSlotDirectoryRecordEntry (page, rid.slotNum, recordEntry);

                fileHandle.writePage (rid.pageNum, page);
                free (page);
                return SUCCESS;
            }
            // record doesn't fit
            else {
                RID newRID;
                insertRecord (fileHandle, recordDescriptor, data, newRID);
                recordEntry.length = newRID.pageNum;
                recordEntry.offset = -1 * newRID.slotNum;
                setSlotDirectoryRecordEntry (page, rid.slotNum, recordEntry);
                fileHandle.writePage (rid.pageNum, page);
                free (page);
                return SUCCESS;
            }
        }
    }
    // should never reach here
    return -1;
}

RC RecordBasedFileManager::readAttribute (FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    void *record = malloc (PAGE_SIZE);

    if (fileHandle.readPage(rid.pageNum, record) != SUCCESS) {
        free (record);
        return RBFM_READ_FAILED;
    }

    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(record, rid.slotNum);
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader (record);
    if (rid.slotNum > slotHeader.recordEntriesNumber) {
        free (record);
        return RBFM_SLOT_DN_EXIST;
    }

    if (recordEntry.length == 0 and recordEntry.offset == 0) {
        free (record);
        return RBFM_RECORD_DELETED;
    }
    else if (recordEntry.offset <= 0) {
        RID newRID;
        newRID.pageNum = recordEntry.length;
        newRID.slotNum = -1 * recordEntry.offset;
        free (record);
        return readAttribute (fileHandle, recordDescriptor, newRID, attributeName, data);
    }
    else {
        int i;
        bool found = false;
        for (i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == attributeName) {
              found = true;
              break;
             }
        }

    char* start = (char*)record + recordEntry.offset;

  // solution uses 2 byte unsigned int to store number of records at start of record
  // recordCount | nullIndicator | fieldOffset Directory | set of fields
  RecordLength len = 0;
  memcpy (&len, start, sizeof(RecordLength));

  unsigned nullIndicatorSize = getNullIndicatorSize(len);
  char nullIndicator[nullIndicatorSize];
  memset (nullIndicator, 0, nullIndicatorSize);
  memcpy (nullIndicator, start + sizeof(RecordLength), nullIndicatorSize);


  // sets the null byte to be returned in data
  char nullByte = 0;
  if (fieldIsNull(nullIndicator, i))
    nullByte |= (1 << 7);
  memcpy(data, &nullByte, 1);

  if (fieldIsNull(nullIndicator, i)) {
    free (record);
    return SUCCESS;
  }

  // offset past nullByte in data
  unsigned dataOffset = 1;

  unsigned recordHeaderSize = sizeof(RecordLength) + nullIndicatorSize;

  ColumnOffset startAttributeOffset = 0;
  ColumnOffset endAttributeOffset = 0;

  // If the first field is the target, must point offset past
  // Attribute Count, NullIndicatorLength and AttributeOffsetDirectory
  if (i == 0) {
      startAttributeOffset = recordHeaderSize + (len * sizeof(ColumnOffset));
  }
  else {
      // go into the offset directory before the target to find where the attribute starts
      memcpy(&startAttributeOffset, start + recordHeaderSize + ((i - 1) * sizeof(ColumnOffset)), sizeof(ColumnOffset));
    }

    // go into the offset directory to find the offset where the target attribute ends
    memcpy(&endAttributeOffset, start + recordHeaderSize + (i * sizeof(ColumnOffset)), sizeof(ColumnOffset));

    switch (recordDescriptor[i].type) {
    case TypeInt:
      memcpy ((char*)data + dataOffset, start + startAttributeOffset, INT_SIZE);
      dataOffset += INT_SIZE;
      break;
    case TypeReal:
      memcpy ((char*)data + dataOffset, start + startAttributeOffset, REAL_SIZE);
      dataOffset += REAL_SIZE;
      break;
    case TypeVarChar:
      uint32_t varCharSize = endAttributeOffset - startAttributeOffset;
      memcpy ((char*)data + dataOffset, &varCharSize, VARCHAR_LENGTH_SIZE);
      dataOffset += VARCHAR_LENGTH_SIZE;
      memcpy((char*)data + dataOffset, start + startAttributeOffset, varCharSize);
      dataOffset += varCharSize;
      break;
    }

    free(record);
    return SUCCESS;

    }
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator) 
{

    rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 
    unsigned int max_pages = (int)(fileHandle.getNumberOfPages());
    if (rid.pageNum > max_pages){
        return RBFM_EOF;
    }
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    SlotDirectoryHeader headerOfPage = getSlotDirectoryHeader(page);
    unsigned int max_record = headerOfPage.recordEntriesNumber;

    if(rid.pageNum == max_pages && max_record <= rid.slotNum ){
        free(page);
        return RBFM_EOF;
    }
    else if( max_record <= rid.slotNum ){
        free(page);
        rid.pageNum += 1;
        rid.slotNum = 0;
        return this->getNextRecord(rid, data);
    }

    //this for loop finds the attributes that we are going to use.
    vector< tuple<Attribute, int> > att;
    for(int i = 0; i < (int)recordDescriptor.size(); i++){
        if(find(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name) != attributeNames.end()){
            att.push_back(make_tuple(recordDescriptor[i], i)); //need the field number as well as the attribute description.
        }
    }

    if(this->filter(rid) == SUCCESS){//checks to see if this is a rid that we want to use.
        void *tempData = malloc(PAGE_SIZE);
        int offset = 0;
        //creating nullIndicator.
        int nullIndicatorSize = getNullIndicatorSize(attributeNames.size());
        void *nullIndicator = malloc(nullIndicatorSize);
        memset(nullIndicator, 0, nullIndicatorSize);
        memset((char*) data, 0, PAGE_SIZE);
        offset = nullIndicatorSize;//putting offset in write possition
        //for loop appends data after the null bits
        for(int i = 0; i < (int)att.size(); i++){
            //get the data
            readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, tempData);
            //check for null
            if((char*)tempData == NULL){
                setBit((char *)nullIndicator, get<1>(att[i]));
                continue;
            }
            //check for varChar.
            if(get<0>(att[i]).type == TypeVarChar){
                int sizeofChar = 0;
                memcpy(&sizeofChar, tempData, sizeof(int));
                memcpy((char *)data + offset, tempData, sizeofChar+sizeof(int) );
                offset += sizeofChar+sizeof(int);
            }
            else{
                memcpy((char *)data + offset, tempData, sizeof(int));
                offset += sizeof(int); 
            }
        }
        memcpy((char *)data, (char *)nullIndicator, nullIndicatorSize);//puts the null bits at the front.
        free(nullIndicator);
        free(tempData);
        free(page);
        return SUCCESS;
    }
    else{
        free(page);
        rid.slotNum += 1;
        return this->getNextRecord(rid, data);//find next possible record.
    } 
}
RC RBFM_ScanIterator::close() { 
    free(value); 
    return SUCCESS;
};


RC RBFM_ScanIterator::initialize(FileHandle &fileH, 
    const vector<Attribute> recDes, 
    const string condAtt, 
    const CompOp cOp, 
    const void *v, 
    const vector<string> attNames)
{
    fileHandle = fileH;
    recordDescriptor = recDes;
    conditionAttribute = condAtt;
    compOp = cOp;
    attributeNames = attNames;

    int size = 0;
    memcpy(&size, (char *)v, VARCHAR_LENGTH_SIZE);
    size += VARCHAR_LENGTH_SIZE;
    value = malloc(size);
    memcpy((char*)value, (char*)v, size);

    return SUCCESS;
}

// helper project 2

RC RBFM_ScanIterator::filter(RID &rid){
    if (compOp == NO_OP){
        return SUCCESS;
    }
    void *data = malloc(PAGE_SIZE);
    readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, data);
    bool works = false;
    bool found = false;
    int i;
    for (i = 0; i < (int)recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == conditionAttribute){
            found = true;
            break;
        }
    }

    if (found && recordDescriptor[i].type == TypeVarChar){
        char *compData = (char *) data;
        if (compOp == EQ_OP) {
            works = (compData == (char *) value);
        }
        else if (compOp == LT_OP){
            works = (compData < (char *) value);
        }
        else if (compOp == LE_OP){
            works = (compData <= (char *) value);
        }
        else if (compOp == GT_OP){
            works = (compData > (char *) value);
        }
        else if (compOp == GE_OP){
            works = (compData >=(char *) value);
        }
        else if (compOp == NE_OP){
            works = (compData != (char *) value);
        }
    }

    else if (found && recordDescriptor[i].type == TypeInt) {
        int compData = 0;
        memcpy(&compData, (char *)data, sizeof(int));
        int intValue = 0;
        memcpy(&intValue, (char *)value, sizeof(int));
        if (compOp == EQ_OP) {
            works = (compData == (int) intValue);
        }
        else if (compOp == LT_OP){
            works = (compData < (int) intValue);
        }
        else if (compOp == LE_OP){
            works = (compData <= (int) intValue);
        }
        else if (compOp == GT_OP){
            works = (compData > (int) intValue);
        }
        else if (compOp == GE_OP){
            works = (compData >=(int) intValue);
        }
        else if (compOp == NE_OP){
            works = (compData != (int) intValue);
        }
    }

    else if (found && recordDescriptor[i].type == TypeReal){
        float compData = 0;
        memcpy(&compData, (char *)data, sizeof(float));
        float floatValue = 0;
        memcpy(&floatValue, (char *)value, sizeof(float));
        if (compOp == EQ_OP) {
            works = (compData == (float) floatValue);
        }
        else if (compOp == LT_OP){
            works = (compData < (float) floatValue);
        }
        else if (compOp == LE_OP){
            works = (compData <= (float) floatValue);
        }
        else if (compOp == GT_OP){
            works = (compData > (float) floatValue);
        }
        else if (compOp == GE_OP){
            works = (compData >=(float) floatValue);
        }
        else if (compOp == NE_OP){
            works = (compData != (float) floatValue);
        }
    }

    free(data);
    if (works){
        return SUCCESS;
    }
    return -1;
}

void setBit(char *nullIndicator, int field){
    int indicatorIndex = field / CHAR_BIT;
    nullIndicator[indicatorIndex] |= 1 << (CHAR_BIT - 1 - (field % CHAR_BIT));
}

bool sortByDecreasingOffset (const SlotDirectoryRecordEntry &lhs, const SlotDirectoryRecordEntry &rhs) {
    return lhs.offset > rhs.offset;
}

void RecordBasedFileManager::compactRecords(void* page, SlotDirectoryHeader &slotHeader, SlotDirectoryRecordEntry &recordEntry, unsigned compactStartOffset) {
    vector<SlotDirectoryRecordEntry> slotEntryList;
    unsigned i;
    for (i = 0; i < slotHeader.recordEntriesNumber; ++i) {
        SlotDirectoryRecordEntry slotEntryTemp = getSlotDirectoryRecordEntry (page, i);
        if (slotEntryTemp.length == 0 or slotEntryTemp.offset == recordEntry.offset)
            continue;
        if (slotEntryTemp.offset < recordEntry.offset)
            slotEntryList.push_back (slotEntryTemp);
    }

    sort (slotEntryList.begin(), slotEntryList.end(), sortByDecreasingOffset);

    for (i = 0; i < slotEntryList.size(); ++i) {
        memmove ((char*)page + compactStartOffset - slotEntryList[i].length,
                 (char*)page + slotEntryList[i].offset,
                 slotEntryList[i].length);
        compactStartOffset -= slotEntryList[i].length;
        slotEntryList[i].offset = compactStartOffset;
    }

    slotHeader.freeSpaceOffset = compactStartOffset;
    setSlotDirectoryHeader (page, slotHeader);
}