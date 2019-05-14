#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define ROOT_CHAR 'r'
# define LEAF_CHAR 'l'
# define INTERNAL_CHAR 'i'
# define EMPTY_CHAR 'e'

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        bool checkCondition(int checkInt, const void *value);
        bool checkCondition(float checkReal, const void *value);
        bool checkCondition(void *checkString, const void *value);

        //helper functions 
        RC findLeaf(IXFileHandle &ixfileHandle, const Attribute att, int &pageNum, const void* val);
        bool isSpaceLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val);
        bool isSpaceNonLeaf(IXFileHandle &ixfileHandle, const PageNum pageNum, const Attribute att, const void* val);
        void insertToLeafSorted(IXFileHandle &ixfileHandle, const Attribute &att, const void *key, const RID &rid);
        void splitLeaf(IXFileHandle &ixfileHandle, PageNum pageID, const void * key, const Attribute &att, const RID &rid);
        //helper functions ^

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        static PagedFileManager *_pf_manager;
};


class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    int rootPageNum = -1;
    FileHandle fh;
    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif