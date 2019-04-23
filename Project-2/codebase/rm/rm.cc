
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

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

RC columnsInsert(int table_id, char* name, int type, int length, int position){
  void* input = malloc(100);
  memset( input, 0, 100);
  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);
  int size_of_name = sizeof(name);
  offset = 1;// number of null

  memcpy((char*) input + offset, table_id, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, size_of_name, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, name, sizeof(name));
  offset += sizeof(name);
  memcpy((char*) input + offset, type, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, length, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, position, sizeof(int));
  offset += sizeof(int);

  _rbf_manager->insertRecord(tables_file, recordDescriptor, input, table_rid);
  free(input);
}

RC tablesInsert(char* name, int id, const vector<Attribute> &table_recordDescriptor, const vector<Attribute> &column_recordDescriptor){
  void* input = malloc(119);
  memset(input, 0, 119);

  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);
  int size_of_name = sizeof(name);
  offset = 1;// number of null
  //insert table ID
  memcpy((char*) input + offset, id, sizeof(int));
  offset += sizeof(int);
  //insert size of the table name
  memcpy((char*) input + offset, size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of table
  memcpy((char*) input + offset, name, sizeof(name));
  offset += sizeof(name);
  //insert size of file name
  memcpy((char*) input + offset, size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of file
  memcpy((char*) input + offset, name, sizeof(name));
  offset += sizeof(name);

  _rbf_manager->insertRecord(tables_file, recordDescriptor, input, table_rid);
  free(input);

  for (int i = 0; i < column_recordDescriptor.size(); i++){
    columnsInsert(id, 
      column_recordDescriptor[i].name, 
      column_recordDescriptor[i].type, 
      column_recordDescriptor[i].length, i);
  }
}

RC RelationManager::createCatalog()
{
  if(_pf_manager->createFile("Tables") != 0 &&
    _pf_manager->createFile("Columns") != 0 ){
    return -1;
  }

  FileHandle tables_file, columns_file;
  _rbf_manager->openFile("Tables",tables_file);
  _rbf_manager->openFile("Columns", columns_file);
  RID table_rid, column_rid;
  vector<Attribute> table_recordDescriptor;
  vector<Attribute> column_recordDescriptor;
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

  //for table
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

  tablesInsert("Tables", 1, table_recordDescriptor, column_recordDescriptor);
  tablesInsert("Columns", 2, table_recordDescriptor, column_recordDescriptor);
  return 0;
}

RC RelationManager::deleteCatalog()
{
    if(_pf_manager->deleteFile("Tables") != 0 &&
     _pf_manager->deleteFile("Columns") != 0 ){
        return -1;
    }
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}