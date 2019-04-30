#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <cstring>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
private:
  RBFM_ScanIterator rbfmsi;
  // static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  static PagedFileManager *_pf_manager;
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  RC initialize(FileHandle &fileH, const vector<Attribute> recDes, const string condAtt, const CompOp cOp, const void *v, const vector<string> attNames);

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return rbfmsi.getNextRecord(rid, data); };
  RC close() { return rbfmsi.close(); };
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  void table_rd(vector<Attribute> table_recordDescriptor);

  void column_rd(vector<Attribute> column_recordDescriptor);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  static PagedFileManager *_pf_manager;

  void columnsInsert(int table_id, string &name, int type, int length, int position, 
    FileHandle &tables_file, const vector<Attribute> &column_recordDescriptor);
  void tablesInsert(string &name, 
    int id, 
    vector<Attribute> &table_recordDescriptor, 
    vector<Attribute> &column_recordDescriptor, 
    FileHandle &tables_file);

};

#endif