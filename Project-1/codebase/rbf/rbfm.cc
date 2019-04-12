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

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
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
