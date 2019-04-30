
#include "rm.h"

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = 0;

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

void RelationManager::table_rd(vector<Attribute> table_recordDescriptor){
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

void RelationManager::column_rd(vector<Attribute> column_recordDescriptor){
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

void RelationManager::columnsInsert(int table_id, string &name, int type, int length, int position, 
  FileHandle &tables_file, const vector<Attribute> &column_recordDescriptor)
{
  void* input = malloc(100);
  char tempName[name.size() +1];
  strcpy(tempName, name.c_str());
  memset( input, 0, 100);
  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);
  int size_of_name = sizeof(name);
  int offset = 1;// number of null

  memcpy((char*) input + offset, &table_id, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, (char*)tempName, size_of_name);
  offset += sizeof(tempName);
  memcpy((char*) input + offset, &type, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &length, sizeof(int));
  offset += sizeof(int);
  memcpy((char*) input + offset, &position, sizeof(int));
  offset += sizeof(int);

  RID temp;

  _rbf_manager->insertRecord(tables_file, column_recordDescriptor, input, temp);
  free(input);
}

void RelationManager::tablesInsert(string &name, 
  int id, 
  vector<Attribute> &table_recordDescriptor, 
  vector<Attribute> &column_recordDescriptor, 
  FileHandle &tables_file)
{
  void* input = malloc(119);
  memset(input, 0, 119);

  int nullbytes = 0;
  memcpy((char*) input, &nullbytes, 1);
  char tempName[name.size() +1];
  strcpy(tempName, name.c_str());
  int size_of_name = sizeof(name);
  int offset = 1;// number of null
  //insert table ID
  memcpy((char*) input + offset, &id, sizeof(int));
  offset += sizeof(int);
  //insert size of the table name
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of table
  memcpy((char*) input + offset, tempName, size_of_name);
  offset += sizeof(tempName);
  //insert size of file name
  memcpy((char*) input + offset, &size_of_name, sizeof(int));
  offset += sizeof(int);
  //insert name of file
  memcpy((char*) input + offset, tempName, size_of_name);
  offset += sizeof(tempName);

  RID temp;

  _rbf_manager->insertRecord(tables_file, table_recordDescriptor, input, temp);
  free(input);

  for (int i = 0; i < (int)column_recordDescriptor.size(); i++){
    columnsInsert(id, 
      column_recordDescriptor[i].name, 
      column_recordDescriptor[i].type, 
      column_recordDescriptor[i].length, 
      i, 
      tables_file,
      column_recordDescriptor);
  }
}

RC RelationManager::createCatalog()
{
  if(_pf_manager->createFile("Tables") != 0 &&
    _pf_manager->createFile("Columns") != 0 ){
    return -1;
  }

  FileHandle tables_file, columns_file;
  _rbf_manager->openFile("Tables", tables_file);
  _rbf_manager->openFile("Columns", columns_file);
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
  
  string t = "Tables";
  string c = "Columns";

  tablesInsert(t, 1, table_recordDescriptor, column_recordDescriptor, tables_file);
  tablesInsert(c, 2, table_recordDescriptor, column_recordDescriptor, tables_file);
  return 0;
}

RC RelationManager::deleteCatalog()
{
    if(_pf_manager->destroyFile("Tables") != 0 &&
     _pf_manager->destroyFile("Columns") != 0 ){
        return -1;
    }
    return 0;
}

RC getNewTableID(int &id){
  RM_ScanIterator rmsi;
  return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  if(_pf_manager->createFile(tableName) != 0){
    return -1;
  }
  int id = 0;
  string t = tableName;
  vector<Attribute> table_recordDescriptor;
  Attribute table_attr;
  //for table
  table_attr.name = "table-id";
  table_attr.type = TypeInt;
  table_attr.length = (AttrLength)4;
  table_recordDescriptor.push_back(table_attr);

  table_attr.name = "table-name";
  table_attr.type = TypeVarChar;
  table_attr.length = (AttrLength)50;
  table_recordDescriptor.push_back(table_attr);

  table_attr.name = "file-name";
  table_attr.type = TypeVarChar;
  table_attr.length = (AttrLength)50;
  table_recordDescriptor.push_back(table_attr);

  tablesInsert(t, id, table_recordDescriptor, column_recordDescriptor);
  return 0;
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
  // initialization of variables, file handles, table and column record desc.
  string t = "Tables";
  string c = "Columns";
  FileHandle table_file,column_file, file;
  _rbf_manager->openFile(t, table_file);
  _rbf_manager->openFile(c,column_file);
  vector<Attribute> table_recordDescriptor;
  vector<Attribute> column_recordDescriptor;
  _rm->table_rd(table_recordDescriptor);
  _rm->column_rd(column_recordDescriptor);

  // create char* table name
  char tempTableName[tableName.size() +1];
  strcpy(tempTableName, tableName.c_str());

  // create return attributes array
  vector<string> tableAttributeNames;
  string att = "table-id";
  tableAttributeNames.push_back(att);
  att = "file-name";
  tableAttributeNames.push_back(att);

  // create record desc for reading return value for tables
  vector<Attribute> tablesReturnRD;
  Attribute attr;
  attr.name = "table-ID";
  attr.type = TypeInt;
  attr.length = 4;
  tablesReturnRD.push_back(attr);
  attr.name = "file-name";
  attr.type = TypeVarChar;
  attr.length = 50;
  tablesReturnRD.push_back(attr);
  // create scan iterator and initialize it with correct search terms

  RBFM_ScanIterator scanIter;
  _rbf_manager->scan(table_file,table_recordDescriptor,"table-name", EQ_OP, (void*) tempTableName, tableAttributeNames ,scanIter);
  // parse data from table, 
  RID tempRid;
  tempRid.slotNum = 0;
  tempRid.pageNum = 0;
  data = malloc(4080);
  // values 
  int tablenum = 0;
  char filename[50];

  scanIter.getNextRecord(tempRid, data);
  int offset = _rbf_manager->getNullIndicatorSize(tablesReturnRD.size());
  // parse values of return data
  for(int i = 0; i < tablesReturnRD.size(); i++){
    switch(tablesReturnRD[i].type){
      case TypeVarChar:{
        int len = 0;
        memcpy(&len, (char*) data+offset, sizeof(int));
        offset += sizeof(int);
        memcpy(filename, (char*) data+offset, len);
        offset += len;
        break;}
      case (TypeInt | TypeReal):{
        memcpy(&tablenum, (char*) data+offset, sizeof(int));
        offset+= sizeof(int);
        break;
      }
      default:
        break;
    } 
  }

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

  // create record descriptor for return values
  vector<Attribute> columnsReturnRD;
  attr.name = "column-name";
  attr.type = TypeVarChar;
  attr.length = 50;
  columnsReturnRD.push_back(attr);
  attr.name = "column-type";
  attr.type = TypeInt;
  attr.length = 4;
  columnsReturnRD.push_back(attr);
  attr.name = "column-length";
  attr.type = TypeInt;
  attr.length = 4;
  columnsReturnRD.push_back(attr);
  attr.name = "column-position";
  attr.type = TypeInt;
  attr.length = 4;
  columnsReturnRD.push_back(attr);
  // initialize scan function for column table
  _rbf_manager->scan(column_file,column_recordDescriptor, "table-id", EQ_OP,(void*) &tablenum, columnAttributeNames,scanIter);
  // tempRID and data reused from above
  tempRid.slotNum = 0;
  tempRid.pageNum = 0;
  memset(data,0,4080);//set data to be equal to 0, we have what we want from tables file
  vector<Attribute> recordDesc;
  recordDesc.resize(4);
  char columnName[50];
  int columnType, columnLength, columnPosition, columnNameLen;
  // Attribute attr; //reused from before
  
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
    // offset += sizeof(int);
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
  // convert char[50] to string
  string file_name = string(filename);
  _rbf_manager->openFile(file_name, file);
  memset(data, 0,4080);
  _rbf_manager->readRecord(file, recordDesc, rid, data);
  



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
  // open table and column files, and get record descriptors for those files
  
  string t = "Tables";
  string c = "Columns";
  FileHandle table_file,column_file;
  _rbf_manager->openFile(t, table_file);
  _rbf_manager->openFile(c,column_file);
  vector<Attribute> table_recordDescriptor;
  vector<Attribute> column_recordDescriptor;
  _rm->table_rd(table_recordDescriptor);
  _rm->column_rd(column_recordDescriptor);
  
  // from table file get the file number. cond attr is table name 
  
  char tempTableName[tableName.size() +1];
  strcpy(tempTableName, tableName.c_str());
  
  // create tableAttributeNames vector for return values
  vector<string> tableAttributeNames;
  string att = "table-id";
  tableAttributeNames.push_back(att);
  att = "file-name";
  tableAttributeNames.push_back(att);

  // create scan iterator
  RBFM_ScanIterator scanIter;
  _rbf_manager->scan(table_file,table_recordDescriptor,"table-name", EQ_OP, (void*) tempTableName, tableAttributeNames ,scanIter);
  // parse data from table, 
  RID rid;
  rid.slotNum = 0;
  rid.pageNum = 0;
  void* data = malloc(4080);
  // values 
  int tablenum = 0;
  char* filename[50];

  // create record desc for tablesReturnRD
  vector<Attribute> tablesReturnRD;
  Attribute attr;
  attr.name = "table-ID";
  attr.type = TypeInt;
  attr.length = 4;
  tablesReturnRD.push_back(attr);
  attr.name = "file-name";
  attr.type = TypeVarChar;
  attr.length = 50;
  tablesReturnRD.push_back(attr);

  scanIter.getNextRecord(rid, data);
  int offset = _rbf_manager->getNullIndicatorSize(tablesReturnRD.size());
  // parse values of return data to 
  for(int i = 0; i < tablesReturnRD.size(); i++){
    switch(tablesReturnRD[i].type){
      case TypeVarChar:{
        int len = 0;
        memcpy((char*)len, data+offset, sizeof(int));
        offset += sizeof(int);
        memcpy(filename, data+offset, len);
        offset += len;
        break;}
      case (TypeInt | TypeReal):{
        memcpy((char*)tablenum, data+offset, sizeof(int));
        offset+= sizeof(int);
        break;
      }
      default:
        break;
    } 
  }
  


  // from columns table get record desc.

  

  rm_ScanIterator.initialize(table_file, , conditionAttribute, compOp, value, attributeNames);
  

  return -1;
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




