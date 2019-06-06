
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
  if(!_rm)
      _rm = new RelationManager();

  return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  // Create both tables and columns tables, return error if either fails
  RC rc;
  rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
  if (rc){
    return rc;
  }
  rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
  if (rc){
    return rc;
  }
  rc = rbfm->createFile(getFileName(INDEX_TABLE_NAME));
  if (rc){
    return rc;
  }

  // Add table entries for both Tables and Columns
  rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
  if (rc){
    return rc;
  }
  rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
  if (rc){
    return rc;
  }
  rc = insertTable(INDEX_TABLE_ID, 1, INDEX_TABLE_NAME);
  if (rc){
    return rc;
  }


  // Add entries for tables and columns to Columns table
  rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
  if (rc){
    return rc;
  }
  rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
  if (rc){
    return rc;
  }
  rc = insertColumns(INDEX_TABLE_ID, indexDescriptor);
  if (rc){
    return rc;
  }

  return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  RC rc;

  rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
  if (rc)
      return rc;

  rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
  if (rc)
      return rc;

  rc = rbfm->destroyFile(getFileName(INDEX_TABLE_NAME));
  if (rc)
      return rc;


  return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  RC rc;
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  // Create the rbfm file to store the table
  if ((rc = rbfm->createFile(getFileName(tableName))))
      return rc;

  // Get the table's ID
  int32_t id;
  rc = getNextTableID(id);
  if (rc)
      return rc;

  // Insert the table into the Tables table (0 means this is not a system table)
  rc = insertTable(id, 0, tableName);
  if (rc)
      return rc;

  // Insert the table's columns into the Columns table
  rc = insertColumns(id, attrs);
  if (rc)
      return rc;

  return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  // If this is a system table, we cannot delete it
  bool isSystem;
  rc = isSystemTable(isSystem, tableName);
  if (rc)
      return rc;
  if (isSystem)
      return RM_CANNOT_MOD_SYS_TBL;

  // Delete the rbfm file holding this table's entries
  rc = rbfm->destroyFile(getFileName(tableName));
  if (rc)
      return rc;

  // Grab the table ID
  int32_t id;
  rc = getTableID(tableName, id);
  if (rc)
      return rc;

  // Open tables file
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // Find entry with same table ID
  // Use empty projection because we only care about RID
  RBFM_ScanIterator rbfm_si;
  vector<string> projection; // Empty
  void *value = &id;

  rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

  RID rid;
  rc = rbfm_si.getNextRecord(rid, NULL);
  if (rc)
      return rc;

  // Delete RID from table and close file
  rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
  rbfm->closeFile(fileHandle);
  rbfm_si.close();

  // Delete from Columns table
  rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // Find all of the entries whose table-id equal this table's ID
  rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

  while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
  {
      // Delete each result with the returned RID
      rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
      if (rc)
          return rc;
  }
  if (rc != RBFM_EOF)
      return rc;

  rbfm->closeFile(fileHandle);
  rbfm_si.close();

  return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  // Clear out any old values
  attrs.clear();
  RC rc;

  int32_t id;
  rc = getTableID(tableName, id);
  if (rc)
      return rc;

  void *value = &id;

  // We need to get the three values that make up an Attribute: name, type, length
  // We also need the position of each attribute in the row
  RBFM_ScanIterator rbfm_si;
  vector<string> projection;
  projection.push_back(COLUMNS_COL_COLUMN_NAME);
  projection.push_back(COLUMNS_COL_COLUMN_TYPE);
  projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
  projection.push_back(COLUMNS_COL_COLUMN_POSITION);

  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // Scan through the Column table for all entries whose table-id equals tableName's table id.
  rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
  if (rc)
      return rc;

  RID rid;
  void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

  // IndexedAttr is an attr with a position. The position will be used to sort the vector
  vector<IndexedAttr> iattrs;
  while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
  {
      // For each entry, create an IndexedAttr, and fill it with the 4 results
      IndexedAttr attr;
      unsigned offset = 0;

      // For the Columns table, there should never be a null column
      char null;
      memcpy(&null, data, 1);
      if (null)
          rc = RM_NULL_COLUMN;

      // Read in name
      offset = 1;
      int32_t nameLen;
      memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
      offset += VARCHAR_LENGTH_SIZE;
      char name[nameLen + 1];
      name[nameLen] = '\0';
      memcpy(name, (char*) data + offset, nameLen);
      offset += nameLen;
      attr.attr.name = string(name);

      // read in type
      int32_t type;
      memcpy(&type, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.attr.type = (AttrType)type;

      // Read in length
      int32_t length;
      memcpy(&length, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.attr.length = length;

      // Read in position
      int32_t pos;
      memcpy(&pos, (char*) data + offset, INT_SIZE);
      offset += INT_SIZE;
      attr.pos = pos;

      iattrs.push_back(attr);
  }
  // Do cleanup
  rbfm_si.close();
  rbfm->closeFile(fileHandle);
  free(data);
  // If we ended on an error, return that error
  if (rc != RBFM_EOF)
      return rc;

  // Sort attributes by position ascending
  auto comp = [](IndexedAttr first, IndexedAttr second) 
      {return first.pos < second.pos;};
  sort(iattrs.begin(), iattrs.end(), comp);

  // Fill up our result with the Attributes in sorted order
  for (auto attr : iattrs)
  {
      attrs.push_back(attr.attr);
  }

  return SUCCESS;
}

// also need to insert into index file
RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  // If this is a system table, we cannot modify it
  bool isSystem;
  rc = isSystemTable(isSystem, tableName);
  if (rc)
      return rc;
  if (isSystem)
      return RM_CANNOT_MOD_SYS_TBL;

  // Get recordDescriptor
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc)
      return rc;

  // And get fileHandle
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(tableName), fileHandle);
  if (rc)
      return rc;

  // Let rbfm do all the work
  rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);


  // copying internal rep of getAttributes
  // modifying to read all matching entries in Index Table
  // need to insert rid into index file
  int32_t id;
  rc = getTableID (tableName, id);
  if (rc) {
    return rc;
  }

  void *value = &id;

  // we need to get the three values that make up an Index Entry: id, attributeName, and fileName

  RBFM_ScanIterator rbfm_si;
  vector<string> projection;
  projection.push_back(INDEX_COL_TABLE_ID);
  projection.push_back(INDEX_COL_ON_ATTR);
  projection.push_back(INDEX_COL_FILE_NAME);

  ///FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);
  if (rc) {
    return rc;
  }

  // Scan through the Index Table for all entries whose table-id equals tableName's table id
  rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
  if (rc) {
    return rc;
  }

  int indexTableDataEntrySize = 1 + sizeof(int) + (sizeof(int) + 50) + (sizeof(int) + 50) + 3;
  void *indexData = malloc (indexTableDataEntrySize);
  RID indexRID;

  vector<IndexTableEntry> indexEntries;
  while ((rc = rbfm_si.getNextRecord(indexRID, indexData)) == SUCCESS) {
    // For each entry, create an IndexEntry, and fill it with the 3 results
    IndexTableEntry indexEntry;
    unsigned offset = 0;

    // For Index Table, there should never be a null column
    char null;
    memcpy (&null, indexData, 1);
    if (null) {
      rc = RM_NULL_COLUMN;
    }
    offset += 1;

    // Read in table-id
    int tableID;
    memcpy (&tableID, (char*)indexData + offset, sizeof(int));
    offset += sizeof(int);
    indexEntry.id = tableID;

    // read in attrName
    int attrNameLen;
    memcpy (&attrNameLen, (char*)indexData + offset, sizeof(int));
    offset += sizeof(int);
    char attrName[attrNameLen + 1];
    attrName[attrNameLen] = '\0';
    memcpy (attrName, (char*)indexData + offset, attrNameLen);
    offset += attrNameLen;
    indexEntry.attrName = string(attrName);

    // read in fileName
    int fileNameLen;
    memcpy (&fileNameLen, (char*)indexData + offset, sizeof(int));
    offset += sizeof(int);
    char fileName[fileNameLen + 1];
    fileName[fileNameLen] = '\0';
    memcpy (fileName, (char*)indexData + offset, fileNameLen);
    offset += fileNameLen;
    indexEntry.fileName = string(fileName);

    indexEntries.push_back(indexEntry);
  }
  // cleanup
  rbfm_si.close();
  rbfm->closeFile(fileHandle);
  free (indexData);

  // if we ended on an error, return error
  if (rc != RBFM_EOF) {
    return rc;
  }

  // if no indexes exist, no need to insert to index
  // exit with SUCCESS since tuple was inserted into Table already
  if (indexEntries.size() == 0) {
    return SUCCESS;
  }

  IXFileHandle ixfileHandle;
  IndexManager *ixm = IndexManager::instance();

  void *keyBuffer = malloc (PAGE_SIZE); // used to insert into index

  int i;
  for (i = 0; i < (int)indexEntries.size(); ++i) {
    // clear the buffer
    memset (keyBuffer, 0, PAGE_SIZE);
    //find the attribute.
    auto pred = [&](Attribute a) {return a.name == indexEntries[i].attrName;};
    auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
    unsigned index = distance(recordDescriptor.begin(), iterPos);
    if (index == recordDescriptor.size()){
      continue;
    }

    getKeyValue (recordDescriptor, index, data, keyBuffer);

    rc = ixm->openFile (indexEntries[i].fileName, ixfileHandle);
    if (rc)
      break;
    rc = ixm->insertEntry(ixfileHandle, recordDescriptor[index], keyBuffer, rid);
    if (rc)
      break;
    rc = ixm->closeFile(ixfileHandle);
    if (rc)
      break;
  }

  free (keyBuffer);
  return rc;
}


RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  // If this is a system table, we cannot modify it
  bool isSystem;
  rc = isSystemTable(isSystem, tableName);
  if (rc)
      return rc;
  if (isSystem)
      return RM_CANNOT_MOD_SYS_TBL;

  // Get recordDescriptor
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc)
      return rc;

  // And get fileHandle
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(tableName), fileHandle);
  if (rc)
      return rc;

  // Let rbfm do all the work
  rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
  rbfm->closeFile(fileHandle);

  return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  // If this is a system table, we cannot modify it
  bool isSystem;
  rc = isSystemTable(isSystem, tableName);
  if (rc)
      return rc;
  if (isSystem)
      return RM_CANNOT_MOD_SYS_TBL;

  // Get recordDescriptor
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc)
      return rc;

  // And get fileHandle
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(tableName), fileHandle);
  if (rc)
      return rc;

  // Let rbfm do all the work
  rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
  rbfm->closeFile(fileHandle);

  return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  // Get record descriptor
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc)
      return rc;

  // And get fileHandle
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(tableName), fileHandle);
  if (rc)
      return rc;

  // Let rbfm do all the work
  rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
  rbfm->closeFile(fileHandle);
  return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;

  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc)
      return rc;

  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(tableName), fileHandle);
  if (rc)
      return rc;

  rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
  rbfm->closeFile(fileHandle);
  return rc;
}

string RelationManager::getFileName(const char *tableName)
{
  return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
  return tableName + string(TABLE_FILE_EXTENSION);
}

vector<Attribute> RelationManager::createTableDescriptor()
{
  vector<Attribute> td;

  Attribute attr;
  attr.name = TABLES_COL_TABLE_ID;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  td.push_back(attr);

  attr.name = TABLES_COL_TABLE_NAME;
  attr.type = TypeVarChar;
  attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
  td.push_back(attr);

  attr.name = TABLES_COL_FILE_NAME;
  attr.type = TypeVarChar;
  attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
  td.push_back(attr);

  attr.name = TABLES_COL_SYSTEM;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  td.push_back(attr);

  return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
  vector<Attribute> cd;

  Attribute attr;
  attr.name = COLUMNS_COL_TABLE_ID;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  cd.push_back(attr);

  attr.name = COLUMNS_COL_COLUMN_NAME;
  attr.type = TypeVarChar;
  attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
  cd.push_back(attr);

  attr.name = COLUMNS_COL_COLUMN_TYPE;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  cd.push_back(attr);

  attr.name = COLUMNS_COL_COLUMN_LENGTH;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  cd.push_back(attr);

  attr.name = COLUMNS_COL_COLUMN_POSITION;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  cd.push_back(attr);

  return cd;
}

vector<Attribute> RelationManager::createIndexDescriptor()
{
  vector<Attribute> ixd;

  Attribute attr;
  attr.name = INDEX_COL_TABLE_ID;
  attr.type = TypeInt;
  attr.length = (AttrLength)INT_SIZE;
  ixd.push_back(attr);

  attr.name = INDEX_COL_ON_ATTR;
  attr.type = TypeVarChar;
  attr.length = (AttrLength)INDEX_COL_FILE_NAME_SIZE;
  ixd.push_back(attr);

  attr.name = INDEX_COL_FILE_NAME;
  attr.type = TypeVarChar;
  attr.length = (AttrLength)INDEX_COL_FILE_NAME_SIZE;
  ixd.push_back(attr);

  return ixd;
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
  unsigned offset = 0;

  int32_t name_len = tableName.length();

  string table_file_name = getFileName(tableName);
  int32_t file_name_len = table_file_name.length();

  int32_t is_system = system ? 1 : 0;

  // All fields non-null
  char null = 0;
  // Copy in null indicator
  memcpy((char*) data + offset, &null, 1);
  offset += 1;
  // Copy in table id
  memcpy((char*) data + offset, &id, INT_SIZE);
  offset += INT_SIZE;
  // Copy in varchar table name
  memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char*) data + offset, tableName.c_str(), name_len);
  offset += name_len;
  // Copy in varchar file name
  memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
  offset += file_name_len; 
  // Copy in system indicator
  memcpy((char*) data + offset, &is_system, INT_SIZE);
  offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
  unsigned offset = 0;
  int32_t name_len = attr.name.length();

  // None will ever be null
  char null = 0;

  memcpy((char*) data + offset, &null, 1);
  offset += 1;

  memcpy((char*) data + offset, &id, INT_SIZE);
  offset += INT_SIZE;

  memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char*) data + offset, attr.name.c_str(), name_len);
  offset += name_len;

  int32_t type = attr.type;
  memcpy((char*) data + offset, &type, INT_SIZE);
  offset += INT_SIZE;

  int32_t len = attr.length;
  memcpy((char*) data + offset, &len, INT_SIZE);
  offset += INT_SIZE;

  memcpy((char*) data + offset, &pos, INT_SIZE);
  offset += INT_SIZE;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
  RC rc;

  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
  RID rid;
  for (unsigned i = 0; i < recordDescriptor.size(); i++)
  {
      int32_t pos = i+1;
      prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
      rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
      if (rc)
          return rc;
  }

  rbfm->closeFile(fileHandle);
  free(columnData);
  return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
  FileHandle fileHandle;
  RID rid;
  RC rc;
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
  prepareTablesRecordData(id, system, tableName, tableData);
  rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

  rbfm->closeFile(fileHandle);
  free (tableData);
  return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fileHandle;
  RC rc;

  rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // Grab only the table ID
  vector<string> projection;
  projection.push_back(TABLES_COL_TABLE_ID);

  // Scan through all tables to get largest ID value
  RBFM_ScanIterator rbfm_si;
  rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

  RID rid;
  void *data = malloc (1 + INT_SIZE);
  int32_t max_table_id = 0;
  while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
  {
      // Parse out the table id, compare it with the current max
      int32_t tid;
      fromAPI(tid, data);
      if (tid > max_table_id)
          max_table_id = tid;
  }
  // If we ended on eof, then we were successful
  if (rc == RM_EOF)
      rc = SUCCESS;

  free(data);
  // Next table ID is 1 more than largest table id
  table_id = max_table_id + 1;
  rbfm->closeFile(fileHandle);
  rbfm_si.close();
  return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fileHandle;
  RC rc;

  rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // We only care about the table ID
  vector<string> projection;
  projection.push_back(TABLES_COL_TABLE_ID);

  // Fill value with the string tablename in api format (without null indicator)
  void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
  int32_t name_len = tableName.length();
  memcpy(value, &name_len, INT_SIZE);
  memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

  // Find the table entries whose table-name field matches tableName
  RBFM_ScanIterator rbfm_si;
  rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

  // There will only be one such entry, so we use if rather than while
  RID rid;
  void *data = malloc (1 + INT_SIZE);
  if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
  {
      int32_t tid;
      fromAPI(tid, data);
      tableID = tid;
  }

  free(data);
  free(value);
  rbfm->closeFile(fileHandle);
  rbfm_si.close();
  return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fileHandle;
  RC rc;

  rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
  if (rc)
      return rc;

  // We only care about system column
  vector<string> projection;
  projection.push_back(TABLES_COL_SYSTEM);

  // Set up value to be tableName in API format (without null indicator)
  void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
  int32_t name_len = tableName.length();
  memcpy(value, &name_len, INT_SIZE);
  memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

  // Find table whose table-name is equal to tableName
  RBFM_ScanIterator rbfm_si;
  rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

  RID rid;
  void *data = malloc (1 + INT_SIZE);
  if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
  {
      // Parse the system field from that table entry
      int32_t tmp;
      fromAPI(tmp, data);
      system = tmp == 1;
  }
  if (rc == RBFM_EOF)
      rc = SUCCESS;

  free(data);
  free(value);
  rbfm->closeFile(fileHandle);
  rbfm_si.close();
  return rc;   
}

void RelationManager::toAPI(const string &str, void *data)
{
  int32_t len = str.length();
  char null = 0;

  memcpy(data, &null, 1);
  memcpy((char*) data + 1, &len, INT_SIZE);
  memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
  char null = 0;

  memcpy(data, &null, 1);
  memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
  char null = 0;

  memcpy(data, &null, 1);
  memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
  char null = 0;
  int32_t len;

  memcpy(&null, data, 1);
  if (null)
      return;

  memcpy(&len, (char*) data + 1, INT_SIZE);

  char tmp[len + 1];
  tmp[len] = '\0';
  memcpy(tmp, (char*) data + 5, len);

  str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
  char null = 0;

  memcpy(&null, data, 1);
  if (null)
      return;

  int32_t tmp;
  memcpy(&tmp, (char*) data + 1, INT_SIZE);

  integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
  char null = 0;

  memcpy(&null, data, 1);
  if (null)
      return;

  float tmp;
  memcpy(&tmp, (char*) data + 1, REAL_SIZE);
  
  real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
  // Open the file for the given tableName
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
  if (rc)
      return rc;

  // grab the record descriptor for the given tableName
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc){
    return rc;
  }

  // Use the underlying rbfm_scaniterator to do all the work
  rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                   compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
  if (rc){
    return rc;
  }

  return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

string RelationManager::getIndexName(const char *tableName, const char *attributeName)
{
  return string(tableName) + string("_") + string(attributeName) + string(INDEX_FILE_EXTENSION);
}

string RelationManager::getIndexName(const string &tableName, const string &attributeName)
{
  return tableName + string("_") + attributeName + string(INDEX_FILE_EXTENSION);
}

RC RelationManager::insertIndex(const string &tableName, const string &attributeName, Attribute attribute) {
  RID rid;
  RC rc;
  FileHandle fileHandle;
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  rc = rbfm->openFile (getFileName(INDEX_TABLE_NAME), fileHandle);
  if (rc) {
    return rc;
  }

  vector<Attribute> attrs;
  attrs.push_back (attribute);

  // get Table tableName's table ID in order to uniquely identify 
  // indexes to a given table in the Index Catalog
  // ex. (3, name, filename)
  //     (3, age, filename)

  int32_t id;
  rc = getTableID(tableName, id);
  if (rc) {
    return rc;
  }

  // actual insertion into the index catalog
  void *data = malloc (PAGE_SIZE);
  memset (data, 0, PAGE_SIZE);

  int32_t attrName_len = attribute.name.length();
  int32_t fileName_len = attrName_len + tableName.length() + 3; // 3 = _ + .i
  string fileName = getIndexName (tableName, attributeName);

  // no fields will be set to null
  char null = 0;
  unsigned offset = 0;

  memcpy ((char*)data, &null, sizeof(char));
  offset += sizeof(char);

  // changed this to tableID to match indexes to tables
  memcpy ((char*)data + offset, &id, INT_SIZE);
  offset += INT_SIZE;

  // attribute name
  memcpy ((char*)data + offset, &attrName_len, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy ((char*)data + offset, attribute.name.c_str(), attrName_len);
  offset += attrName_len;

  // Index file name
  memcpy((char*) data + offset, &fileName_len, VARCHAR_LENGTH_SIZE);
  offset += VARCHAR_LENGTH_SIZE;
  memcpy((char*) data + offset, fileName.c_str(), fileName_len);
  offset += fileName_len;

  // insert entry into the Index Catalog
  rc = rbfm->insertRecord (fileHandle, indexDescriptor, data, rid);

  free (data);

  rbfm->closeFile(fileHandle);
  return rc;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName) {
  RC rc;
  IndexManager *ixm = IndexManager::instance();
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  // check if Table exists
  vector<Attribute> recordDescriptor;
  rc = getAttributes (tableName, recordDescriptor);
  if (rc) {
    return rc;
  }

  // create the index file
  rc = ixm->createFile (getIndexName(tableName, attributeName));
  if (rc) {
    return rc;
  }

  // Open rbfm file holding Table tableName's records
  FileHandle fileHandle;
  rc = rbfm->openFile (getFileName(tableName), fileHandle);

  // open the Index File
  IXFileHandle ixfileHandle;
  rc = ixm->openFile (getIndexName(tableName, attributeName), ixfileHandle);

  vector<string> attVector;
  attVector.push_back(attributeName);
  RBFM_ScanIterator rmsi;
  rc = rbfm->scan (fileHandle, recordDescriptor, attributeName, NO_OP, NULL, attVector, rmsi);

  //find the attribute.
  auto pred = [&](Attribute a) {return a.name == attributeName;};
  auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
  unsigned index = distance(recordDescriptor.begin(), iterPos);
  if (index == recordDescriptor.size()){
    return RM_NO_SUCH_ATTRIBUTE;
  }

  // Populate the index file by inserting one tuple at a time
  RID rid;
  void *data = malloc (PAGE_SIZE);
  memset (data, 0, PAGE_SIZE);
  while (rmsi.getNextRecord (rid, data) == SUCCESS) {
    rc = ixm->insertEntry(ixfileHandle, recordDescriptor[index], data, rid);
    if (rc) {
      free (data);
      return rc;
    }
  }
  free (data);

  rbfm->closeFile (fileHandle);
  ixm->closeFile(ixfileHandle);
  rmsi.close();

  rc = insertIndex (tableName, attributeName, recordDescriptor[index]);
  if (rc) {
    return rc;
  }

  return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  RC rc;
  string fileName = getIndexName(tableName, attributeName);

  // If this is a system table, we cannot delete it
  bool isSystem;
  rc = isSystemTable(isSystem, tableName);
  if (rc){
    return rc;
  }
  if (isSystem){
    return RM_CANNOT_MOD_SYS_TBL;
  }

  // Delete the rbfm file holding this table's entries
  rc = rbfm->destroyFile(fileName);
  if (rc){
    return rc;
  }

  rc = deleteTable(tableName); // this should be fileName not table name since we want to destroy the index based on the attribute

  //Delete from the index catolog
  FileHandle fileHandle;
  rc = rbfm->openFile(getFileName(INDEX_TABLE_NAME), fileHandle);

  // Use empty projection because we only care about RID
  RBFM_ScanIterator rbfm_si;
  vector<string> projection; // Empty
  void *value = malloc(fileName.length() + sizeof(int));
  memset(value, 0, fileName.length() + sizeof(int));
  toAPI(fileName, value);

  rc = rbfm->scan(fileHandle, indexDescriptor, INDEX_COL_FILE_NAME, EQ_OP, value, projection, rbfm_si);

  RID rid;
  rc = rbfm_si.getNextRecord(rid, NULL);
  if (rc){
    return rc;
  }

  // Delete RID from table and close file
  rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
  rbfm->closeFile(fileHandle);
  rbfm_si.close();

  return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
  // Open the file for the given tableName
  IndexManager *ixm = IndexManager::instance();
  RC rc = ixm->openFile(getIndexName(tableName, attributeName), rm_IndexScanIterator.ixfileHandle);
  if (rc)
      return rc;

  // grab the record descriptor for the given tableName
  vector<Attribute> recordDescriptor;
  rc = getAttributes(tableName, recordDescriptor);
  if (rc){
    return rc;
  }

  //find the attribute.
  auto pred = [&](Attribute a) {return a.name == attributeName;};
  auto iterPos = find_if(recordDescriptor.begin(), recordDescriptor.end(), pred);
  unsigned index = distance(recordDescriptor.begin(), iterPos);
  if (index == recordDescriptor.size()){
    return RM_NO_SUCH_ATTRIBUTE;
  }

  // Use the underlying ixm_scaniterator to do all the work
  rc = ixm->scan(rm_IndexScanIterator.ixfileHandle, 
                  recordDescriptor[index], lowKey, highKey, 
                  lowKeyInclusive, highKeyInclusive,
                  rm_IndexScanIterator.ix_iter);
  if (rc){
    return rc;
  }

  return SUCCESS;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
  return ix_iter.getNextEntry(rid, key);
}
RC RM_IndexScanIterator::close() {
  IndexManager *ixm = IndexManager::instance();
  ix_iter.close();
  ixm->closeFile(ixfileHandle);
  return SUCCESS;
}

RC RelationManager::getKeyValue (vector<Attribute> attrs, int index, const void *data, void *leftValue) {
  // check for a null
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  int nullIndicatorSize = rbfm->getNullIndicatorSize (attrs.size());
  char nullIndicator[nullIndicatorSize];
  memset (nullIndicator, 0, nullIndicatorSize);
  memcpy (nullIndicator, (char*)data, nullIndicatorSize);

  int offset = nullIndicatorSize;
  for (int i = 0; i < attrs.size(); ++i) {
    if (rbfm->fieldIsNull(nullIndicator, i))
      continue;

    if (attrs[i].type == TypeVarChar) {
      int length = 0;
      memcpy (&length, (char*)data + offset, sizeof(int));
      memcpy (leftValue, (char*)data + offset, sizeof(int) + length);
      offset += sizeof(int) + length;
    }
    else {
      memcpy (leftValue, (char*)data + offset, 4); // works for int or float
      offset += sizeof(int);
    }

    if (i == index)
      return SUCCESS;
  }
  return -1;
}
