#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = 0;

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
    // FILE *record;
    // if(fileExists("recordsHeader"))
    //     record = fopen("recordsHeader", "r+b");
    // else(
    //     record = fopen("recordsHeader", "wb")
    // )
    return _pf_manager->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

int size_helper(const vector<Attribute> &recordDescriptor, const void *data, void* formated){
    unsigned numberOfNullBytes = ceil(recordDescriptor.size() / 8.0);
    int numnberOfAttributes = (int)recordDescriptor.size();
    char nullIndicator[numberOfNullBytes];
    memset(nullIndicator, 0, numberOfNullBytes);
    memcpy(nullIndicator, (char*)data, numberOfNullBytes);
    int attribute_size[numnberOfAttributes];
    int offset = numberOfNullBytes;
    void* temp_data = malloc(100);
    memset(temp_data, 0, 100);
    int temp_data_offset = 0;
    for(int field = 0; field < numnberOfAttributes; field++){
        int totalbytes = 0;

        int byteNumber = ceil( (field+1) / 8.0) - 1;
        char mask = 0x01 << (field % 8); // use modulo because only using mask on a byte (8 bits)

        if (nullIndicator[byteNumber] & mask){ //gets single bit.
            //means that entry is null
            attribute_size[field] = totalbytes;
            continue;
        }
        if (recordDescriptor[field].type == TypeInt){
            totalbytes = recordDescriptor[field].length;
            int intAttribute;
            memset(&intAttribute, 0, sizeof(int));
            memcpy(&intAttribute, (char*)data+offset, sizeof(int));
            memcpy((char*) temp_data+temp_data_offset, &intAttribute, sizeof(int));
            temp_data_offset += totalbytes;
        }
        else if(recordDescriptor[field].type == TypeReal){
            totalbytes = recordDescriptor[field].length;
            float realAttribute;
            memset(&realAttribute, 0, sizeof(float));
            memcpy(&realAttribute, (char*)data+offset, sizeof(float));
            memcpy((char*) temp_data+temp_data_offset, &realAttribute, sizeof(float));
            temp_data_offset += totalbytes;
        }
        else if(recordDescriptor[field].type == TypeVarChar){
            memcpy(&totalbytes, (char *)data+offset, sizeof(int));
            offset += sizeof(int); //size of the int we just read
            char varCharData[totalbytes];
            memset(varCharData, 0, totalbytes);
            memcpy(varCharData, (char *)data+offset, totalbytes);
            //  cout << "sizehelper varCharData :" ;
            // for(int j=0; j < totalbytes; j++){
            //    cout << varCharData[j]; 
            // }
            // cout << " \n";
            memcpy((char*) temp_data+temp_data_offset, &varCharData, totalbytes);
            temp_data_offset += totalbytes;
        }
        else { 
            return -1;
        }
        // cout <<"totalbytes :"<< totalbytes << "\n";
        attribute_size[field] = totalbytes;
        offset += totalbytes;
        // cout <<"offset :"<< offset << "\n";
    }
    int size_of_record = offset + (numnberOfAttributes * sizeof(int));
    memset(formated, 0, size_of_record);
    memcpy((char*)formated, &numnberOfAttributes, sizeof(int)); // number of attributes
    memcpy((char*)formated + sizeof(int), nullIndicator, numberOfNullBytes);//null array

    int current_offset = sizeof(int) + numberOfNullBytes;
    int record_offset = numnberOfAttributes * sizeof(int);//assume start after null bytes

    for(int i = 0; i < numnberOfAttributes; i++){
        record_offset += attribute_size[i];
        memcpy((char*)formated + current_offset, &record_offset, sizeof(int));
        current_offset += sizeof(int);
    }
    memcpy((char*) formated + current_offset, temp_data,temp_data_offset);//change x,y
    free(temp_data);
    return size_of_record;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    cout<<"insertRecord\n";
    void* page = malloc(PAGE_SIZE);
    void* formated = malloc(100);
    memset((char*) page, 0, PAGE_SIZE);
    memset((char*) formated, 0, 100);

    int size_of_record = size_helper(recordDescriptor, data, formated);
    int page_num=0;
    int flag=0;
    int offset = PAGE_SIZE - (2 * sizeof(int));
    int num_slots=0;
    int free_space_offset=0;
    cout << "line 135\n";
    while(fileHandle.readPage(page_num, page) == 0){

        memcpy(&num_slots,(char*) page+offset, sizeof(int));
        memcpy(&free_space_offset,(char*) page+(offset+sizeof(int)), sizeof(int));
        if(free_space_offset + ((num_slots+1)*(2*sizeof(int))) + size_of_record + (2*sizeof(int)) < PAGE_SIZE){
            flag=1;
            break;
        }
        page_num+=1;
    }
    if(flag==0){//if while 
        page_num += 1;
        num_slots=0;
        free_space_offset=0;
        memset((char*) page, 0, PAGE_SIZE);
        fileHandle.appendPage(page);
    }
    cout << "pageNum :" << page_num<<"\n";
    int slot_location = (PAGE_SIZE - (sizeof(int) * 2 *(num_slots+2)));
    cout << "slot location start :" << slot_location << "\n";
    memcpy((char*) page + free_space_offset, formated,size_of_record);
    //input new slot
    memcpy((char*) page + slot_location, &free_space_offset, sizeof(int));
    memcpy((char*) page + slot_location+sizeof(int), &size_of_record, sizeof(int));
    //change num-slots and free space offset
    num_slots += 1;
    memcpy((char*) page + offset, &num_slots, sizeof(int));
    free_space_offset += size_of_record;
    memcpy((char*) page + offset+sizeof(int), &free_space_offset, sizeof(int));
    rid.pageNum = page_num;
    rid.slotNum = num_slots;
    int success = fileHandle.writePage(page_num, page);
    free(page);
    free(formated);
    return success;
}
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void* page = malloc(PAGE_SIZE);
    void* record = malloc(100);
    fileHandle.readPage(rid.pageNum, page);
    int offset=0;
    int length=0;
    int data_offset=0;
    int slot = PAGE_SIZE -((rid.slotNum+1) * 2 * sizeof(int));//slot location
    
    //get record using offset and length
    memcpy(&offset, (char*)page + slot, sizeof(int));
    memcpy(&length, (char*)page + slot + sizeof(int), sizeof(int));
    memcpy(record, (char*)page +offset, length);

    //get numberOfNullBytes and nullIndicator
    unsigned numberOfNullBytes = ceil(recordDescriptor.size() / 8.0);
    char nullIndicator[numberOfNullBytes];
    memset(nullIndicator, 0, numberOfNullBytes);
    memcpy(nullIndicator, (char*)record+sizeof(int), numberOfNullBytes);
    memcpy((char*)data, nullIndicator, numberOfNullBytes);

    //new value for variable offset used for record. offset is now offset in record
    offset = numberOfNullBytes + (sizeof(int) * (1+(int)recordDescriptor.size()));//offset to beginning of first field in record
    data_offset = numberOfNullBytes;//offset to end of void* data

    int nextval_offset= 0;//offset to location of end of first value
    int start_nextval_offset= sizeof(int) + numberOfNullBytes;//copy of nextval offset, never changes. pointer
 
    for(int field = 0; field < (int)recordDescriptor.size(); field++){
        int next_val_location = 0;//location of next value
        int field_len = 0;
        memcpy(&next_val_location, (char*) record + start_nextval_offset + nextval_offset, sizeof(int));
        field_len = abs(start_nextval_offset + next_val_location - offset);
        // cout << "field_len :" << field_len << "\n";
        // cout << "field :" << field <<"\n";
        int totalbytes = 0;
        int byteNumber = ceil( (field+1) / 8.0) - 1;
        char mask = 0x01 << (field % 8); // use modulo because only using mask on a byte (8 bits)
        nextval_offset += sizeof(int);
        // cout << "before if\n";
        if (nullIndicator[byteNumber] & mask){ //gets single bit.
            //means that entry is null
            cout << "null\n";
            continue;
        }
        if (recordDescriptor[field].type == TypeInt){
            cout << "typeint\n";
            totalbytes += recordDescriptor[field].length;
            int intAttribute;//temp variable
            memset(&intAttribute, 0, sizeof(int));
            memcpy(&intAttribute, (char*)record+offset, sizeof(int));
            memcpy((char*)data+data_offset, &intAttribute, sizeof(int));
        }
        else if(recordDescriptor[field].type == TypeReal){
            totalbytes += recordDescriptor[field].length;
            float realAttribute;//temp variable
            memset(&realAttribute, 0, sizeof(float));
            memcpy(&realAttribute, (char*)record + offset, sizeof(float));
            memcpy((char*)data+data_offset, &realAttribute, sizeof(float));
        }
        else if(recordDescriptor[field].type == TypeVarChar){
            totalbytes = field_len;
            char varCharData[totalbytes];//temp array
            memset(varCharData, 0, totalbytes);
            memcpy(varCharData, (char*)record + offset, totalbytes);
            memcpy((char*)data+data_offset, &totalbytes, sizeof(int));
            data_offset +=sizeof(int);
            memcpy((char*)data+data_offset, &varCharData, totalbytes);
            // cout << "varCharData :" ;
            // for(int j=0; j < totalbytes; j++){
            //    cout << varCharData[j]; 
            // }
            // cout << " \n";
        }
        else {
            return -1;
        }
        offset += totalbytes;
        data_offset += totalbytes;
        // cout <<"offset :"<< offset << "\n";
        // cout <<"data_offset :"<< data_offset << "\n";
    }
    free(page);
    free(record);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    unsigned numberOfNullBytes = ceil(recordDescriptor.size() / 8.0);
    char nullIndicator[numberOfNullBytes];
    memset(nullIndicator, 0, numberOfNullBytes);
    memcpy(nullIndicator, (char*)data, numberOfNullBytes);

    int offset = numberOfNullBytes;
    for(int field = 0; field < (int)recordDescriptor.size(); field++){
        cout << recordDescriptor[field].name << ": ";
        int totalbytes = 0;

        int byteNumber = ceil( (field+1) / 8.0) - 1;
        char mask = 0x01 << (field % 8); // use modulo because only using mask on a byte (8 bits)

        if (nullIndicator[byteNumber] & mask){ //gets single bit.
            //means that entry is null
            cout << endl;
            continue;
        }
        if (recordDescriptor[field].type == TypeInt){
            totalbytes = recordDescriptor[field].length;
            int intAttribute;
            memset(&intAttribute, 0, sizeof(int));
            memcpy(&intAttribute, (char*)data+offset, sizeof(int));
            cout << intAttribute;
        }
        else if(recordDescriptor[field].type == TypeReal){
            totalbytes = recordDescriptor[field].length;
            float realAttribute;
            memset(&realAttribute, 0, sizeof(float));
            memcpy(&realAttribute, (char*)data+offset, sizeof(float));
            cout << realAttribute;
        }
        else if(recordDescriptor[field].type == TypeVarChar){
            memcpy(&totalbytes, (char *)data+offset, sizeof(int));
            offset += 4; //size of the int we just read
            char varCharData[totalbytes];
            memset(varCharData, 0, totalbytes);
            memcpy(varCharData, (char *)data+offset, totalbytes);
            for(int i = 0; i < totalbytes; i++){
                cout << varCharData[i];
            }
        }
        else {
            return -1;
        }
        cout << endl;
        offset += totalbytes;
    }
    return SUCCESS;
}