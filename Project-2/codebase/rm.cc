#include "rm.h"

RelationManager* RelationManager::_rm = NULL;
RecordBasedFileManager* RelationManager::_rbf_manager = NULL;
RecordBasedFileManager* RM_ScanIterator::_rbf_manager = NULL;

int RelationManager::maxTableID = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

void RelationManager::table_rd(vector<Attribute> &table_recordDescriptor){
  Attribute attr;
  
  attr.name = "table-id";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  table_recordDescriptor.push_back(attr);

  attr.name = "table-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  table_recordDescriptor.push_back(attr);

  attr.name = "file-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  table_recordDescriptor.push_back(attr);
}

void RelationManager::column_rd(vector<Attribute> &column_recordDescriptor){
  Attribute attr;
  //For column
  attr.name = "table-id";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  column_recordDescriptor.push_back(attr);

  attr.name = "column-name";
  attr.type = TypeVarChar;
  attr.length = (AttrLength)50;
  column_recordDescriptor.push_back(attr);

  attr.name = "column-type";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  column_recordDescriptor.push_back(attr);

  attr.name = "column-length";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  column_recordDescriptor.push_back(attr);

  attr.name = "column-position";
  attr.type = TypeInt;
  attr.length = (AttrLength)4;
  column_recordDescriptor.push_back(attr);

  return;
}

void RelationManager::columnsInsert(int table_id, const string &name, const int type, const int length, int position, 
  FileHandle &tables_file, const vector<Attribute> &column_recordDescriptor)
{
  void* input = malloc(4096);
  char tempName[name.length() +1];
  strcpy(tempName, name.c_str());

  memset(input, 0, 4096);
  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);
 
  int size_of_name = name.length();
  int offset = 1;// number of null

  memcpy((char*) input + offset, &table_id, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, (char*)tempName, size_of_name);
  offset += size_of_name;
  memcpy((char*) input + offset, &type, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &position, sizeof(int));
  offset += sizeof(int);

  RID rid;

  _rbf_manager->insertRecord(tables_file, column_recordDescriptor, input, rid);
  free(input);
}

void RelationManager::tablesInsert(string &name, 
  int id, 
  const vector<Attribute> &table_recordDescriptor, //record descriptor for table file 
  const vector<Attribute> &recordDescriptor, //record descriptor for new table
  FileHandle &tables_file)
{

  _rbf_manager->openFile ("Tables", tables_file);

  void* input = malloc(119);
  memset(input, 0, 119);

  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);

  char tempName[name.length() +1];
  strcpy(tempName, name.c_str());

  int size_of_name = name.length();
  int offset = 1;// number of null
  //insert table ID
  memcpy((char*) input + offset, &id, sizeof(int));
  offset += sizeof(int);
  //insert size of the table name
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of table
  memcpy((char*) input + offset, tempName, size_of_name);
  offset += size_of_name;
  //insert size of file name
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of file
  memcpy((char*) input + offset, tempName, size_of_name);
  offset += size_of_name;

  RID rid;

  _rbf_manager->insertRecord(tables_file, table_recordDescriptor, input, rid);
  free(input);

  _rbf_manager->closeFile (tables_file);

  FileHandle fileHandle;

  _rbf_manager->openFile ("Columns", fileHandle);
  vector<Attribute> column_recordDescriptor;
  column_rd(column_recordDescriptor);

  for (int i = 0; i < (int)(recordDescriptor.size() - 1) ; i++){
    columnsInsert(id, 
      recordDescriptor[i].name, 
      recordDescriptor[i].type, 
      recordDescriptor[i].length, 
      i + 1, 
      fileHandle,
      column_recordDescriptor);
  }
  _rbf_manager->closeFile (fileHandle);
}

RC RelationManager::createCatalog()
{
  /*if(_rbf_manager->createFile("Tables") != 0 &&
    _rbf_manager->createFile("Columns") != 0 ){
    return -1;
  }*/

  RC rc = _rbf_manager->createFile("Tables");
  if (rc != 0) return rc;

  rc = _rbf_manager->createFile("Columns");
  if (rc != 0) return rc;

  FileHandle tables_file;

  vector<Attribute> table_recordDescriptor;
  vector<Attribute> column_recordDescriptor;

  table_rd (table_recordDescriptor);
  column_rd (column_recordDescriptor);
  
  string t = "Tables";
  string c = "Columns";


  tablesInsert(t, 1, table_recordDescriptor, table_recordDescriptor, tables_file);
  tablesInsert(c, 2, table_recordDescriptor, column_recordDescriptor, tables_file);
  RelationManager::maxTableID = 2;


  return 0;
}

RC RelationManager::deleteCatalog()
{
    RC rc = _rbf_manager->destroyFile ("Tables");
    if (rc != 0) return rc;

    rc = _rbf_manager->destroyFile ("Columns");
    if (rc != 0) return rc;

    RelationManager::maxTableID = 0;
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  if(_rbf_manager->createFile(tableName) != 0){
    return -1;
  }

  RelationManager::maxTableID += 1;
  int id = RelationManager::maxTableID;
  string t = tableName;
  vector<Attribute> table_recordDescriptor;
  
  table_rd (table_recordDescriptor);

  FileHandle fileHandle;

  tablesInsert(t, id, table_recordDescriptor, attrs, fileHandle);

  return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
  if(tableName == "Table" || tableName == "Columns"){
    return -1;
  }
  FileHandle file;
  RBFM_ScanIterator deletingIter;
  vector<Attribute> recordDesc;
  void * data = malloc(4096);
  RID rid;
  rid.pageNum = 0;
  rid.slotNum = 0;

  //Setting up to delete from Tables file
  vector<string> tableAttributeNames;
  string att = "table-id";
  tableAttributeNames.push_back(att);
  att = "file-name";
  tableAttributeNames.push_back(att);

  _rbf_manager->openFile("Tables", file);
  table_rd(recordDesc);

  //converting string to void pointer
  string conditionAttribute = "table_id";
  void *name = malloc(100);
  int size_of_name = tableName.size();
  memcpy(name, &size_of_name, sizeof(int));
  memcpy((char *)name + sizeof(int), &tableName, size_of_name);

  //Setting up iterator to find the record in the Tables file
  _rbf_manager->scan(file, recordDesc, conditionAttribute, EQ_OP, name, tableAttributeNames, deletingIter);

  deletingIter.getNextRecord(rid, data);//Got the record.

  //get the table_id
  int table_id = 0;
  int offset = 1; //there is only one null byte for Tables
  memcpy(&table_id, (char*)data+offset, sizeof(int)); //table_id is the first column

  //Delete record from Tables 
  _rbf_manager->deleteRecord(file, recordDesc, rid);

  //get ready to delete from Columns
  rid.pageNum = 0;
  rid.slotNum = 0;
  column_rd(recordDesc);
  _rbf_manager->openFile("Columns", file);

  //setting up iterator to find all the records that match the table_id
  while(deletingIter.getNextRecord(rid, data) != -1){
    _rbf_manager->deleteRecord(file, recordDesc, rid);
  }

  return _rbf_manager->destroyFile(tableName);
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &recordDesc)
{
  // initialization of variables, file handles, table and column record desc.
  string t = "Tables";
  string c = "Columns";

  FileHandle table_file, column_file;

  _rbf_manager->openFile(t, table_file);
  _rbf_manager->openFile(c, column_file);

  vector<Attribute> table_recordDescriptor;
  vector<Attribute> column_recordDescriptor;

  table_rd(table_recordDescriptor);
  column_rd(column_recordDescriptor);

  // create char* table name
  char tempTableName[tableName.size() +1];
  strcpy(tempTableName, tableName.c_str());

  // create return attributes array
  vector<string> tableAttributeNames;
  string att = "table-id";
  tableAttributeNames.push_back(att);
  att = "file-name";
  tableAttributeNames.push_back(att);

  // create scan iterator and initialize it with correct search terms
  RBFM_ScanIterator scanIter;
  _rbf_manager->scan(table_file, table_recordDescriptor,"table-name", EQ_OP, (void*) tempTableName, tableAttributeNames, scanIter);
  
  // parse data from table, 
  RID tempRid;
  tempRid.slotNum = 0;
  tempRid.pageNum = 0;
  void *data = malloc(4080);
  
  // values 
  void * tablenum = malloc(sizeof(int));
  char filename[50];

  scanIter.getNextRecord(tempRid, data);
  int offset = 1; //Tables only has one Null Byte

  //Get the table id number
  memcpy((char *)tablenum, (char*) data+offset, sizeof(int));
  offset+= sizeof(int);
  //Get the filename
  int lenOfChar = 0;
  memcpy(&lenOfChar, (char*) data+offset, sizeof(int));
  offset += sizeof(int);
  memcpy(filename, (char*) data+offset, lenOfChar);


  vector<string> columnAttributeNames;
  //att is string used above for same purpose
  att = "column-name";
  columnAttributeNames.push_back(att);
  att = "column-type";
  columnAttributeNames.push_back(att);
  att = "column-length";
  columnAttributeNames.push_back(att);
  att = "column-position";
  columnAttributeNames.push_back(att);

  // initialize scan function for column table
  _rbf_manager->scan(column_file, column_recordDescriptor, "table-id", EQ_OP, tablenum, columnAttributeNames, scanIter);
  // tempRID and data reused from above
  tempRid.slotNum = 0;
  tempRid.pageNum = 0;
  memset(data,0,4080);//set data to be equal to 0, we have what we want from tables file
  recordDesc.resize(4);
  char columnName[50];
  int columnType, columnLength, columnPosition, columnNameLen;
  Attribute attr;
  
  while(scanIter.getNextRecord(tempRid, data) != -1){
    offset = 1;

    memcpy(&columnNameLen, (char*) data + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(columnName, (char*) data+offset, columnNameLen);
    offset += columnNameLen;

    memcpy(&columnType, (char*) data+offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&columnLength, (char*) data+offset, sizeof(int));
    offset += sizeof(int);

    memcpy(&columnPosition, (char*) data+offset, sizeof(int));

    // create attribute and insert at correct position.
    string str;
    // strcpy(str, columnName);
    str = string(columnName);
    attr.name = str;
    attr.type = (AttrType)columnType;
    attr.length = columnLength;

    recordDesc[columnPosition-1] = attr;
    columnType = 0;
    columnLength = 0;
    columnPosition = 0;
    columnNameLen = 0;
    memset(columnName,0, 50);
  }
  free(data);
  return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  vector<Attribute> recordDesc;
  FileHandle file;

  getAttributes(tableName, recordDesc); //fill recordDec
  _rbf_manager->openFile(tableName, file);

  return _rbf_manager->insertRecord(file, recordDesc, data, rid);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  vector<Attribute> recordDesc;
  FileHandle file;

  getAttributes(tableName, recordDesc); //fill recordDec
  _rbf_manager->openFile(tableName, file);

  return _rbf_manager->deleteRecord(file, recordDesc, rid);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  vector<Attribute> recordDesc;
  FileHandle file;

  getAttributes(tableName, recordDesc); //fill recordDec
  _rbf_manager->openFile(tableName, file);

  return _rbf_manager->updateRecord(file, recordDesc, data, rid);
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{ 
  vector<Attribute> recordDesc;
  FileHandle file;

  getAttributes(tableName, recordDesc); //fill recordDec
  _rbf_manager->openFile(tableName, file); 

  memset(data, 0,4080);

  return _rbf_manager->readRecord(file, recordDesc, rid, data);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  return _rbf_manager->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  vector<Attribute> recordDesc;
  FileHandle file;

  getAttributes(tableName, recordDesc); //fill recordDec
  _rbf_manager->openFile(tableName, file);

  //return _rbf_manager->readRecord(file, recordDesc, rid, data);
  return _rbf_manager->readAttribute (file, recordDesc, rid, attributeName, data);
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
  FileHandle file;
  vector<Attribute> recordDesc;
  getAttributes(tableName, recordDesc);
  _rbf_manager->openFile(tableName, file);
  rm_ScanIterator.initialize(file, recordDesc, conditionAttribute, compOp, value, attributeNames);
  return 0;
}

RC RM_ScanIterator::initialize(FileHandle &fileH, 
  const vector<Attribute> recDes, 
  const string condAtt, 
  const CompOp cOp, 
  const void *v, 
  const vector<string> attNames)
{

  RBFM_ScanIterator temp_rbfmsi;
  _rbf_manager->scan(fileH, recDes, condAtt, cOp, v, attNames, temp_rbfmsi);
  rbfmsi = temp_rbfmsi;
  return 0;
}