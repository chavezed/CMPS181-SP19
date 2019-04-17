#include "pfm.h"


bool fileExists (const string &fileName);


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName))
        return -1;

    FILE *fh = fopen (fileName.c_str(), "wb");

    if (fh == NULL)
        return -1;

    fclose(fh);

    return SUCCESS;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (remove (fileName.c_str()) != 0)
        return -1;

    return SUCCESS;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (fileHandle.getFile() != NULL)
        return -1;

    if (not fileExists(fileName))
        return -1;

    FILE *file = fopen (fileName.c_str(), "rb+");
    if (file == NULL)
        return -1;

    fileHandle.setFile(file);

    return SUCCESS;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    FILE *file = fileHandle.getFile();

    // file not open
    if (file == NULL)
        return -1;

    fflush(file);
    fclose(file);

    fileHandle.setFile(NULL);
    return SUCCESS;
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
    _fd = NULL;
}


FileHandle::~FileHandle()
{
}

/*
    int fseek (FILE *stream, long int offset, int origin);
        stream  - pointer to a FILE object that identifies the stream
        offset  - number of bytes to offset from origin
        origin  - position used as reference for the offset. Specified
                  by one of the following constants:
                        SEEK_SET: Beginning of file
                        SEEK_CUR: Current position of the file pointer
                        SEEK_END: End of file

    size_t fread (void *ptr, size_t size, size_t nmemb, FILE *stream);
        ptr     - pointer to a block of memory with a minimum size of
                  size * nmemb bytes
        size    - size in bytes of each element to be read
        nmemb   - number of elements, each one with a size of size bytes
        stream  - pointer to a FILE object that specifies input stream
    Returns:
        Total number of elements successfully read are returned as a size_t
        object, which is an integral type. If this number differes from the
        nmemb paramter, then either an error had occurred or the EOF was reached.

*/
RC FileHandle::readPage(PageNum pageNum, void *data)
{
    // pageNum doesn't exist
    if (pageNum > getNumberOfPages())
        return -1;

    if (fseek(_fd, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return -1;

    if (fread(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return -1;

    ++readPageCounter;
    return SUCCESS;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum > getNumberOfPages())
        return -1;

    if (fseek (_fd, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return -1;

    if (fwrite(data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return -1;

    fflush(_fd);
    ++writePageCounter;
    return SUCCESS;
}


RC FileHandle::appendPage(const void *data)
{
    if (fseek (_fd, 0, SEEK_END) != 0)
        return -1;

    if (fwrite (data, 1, PAGE_SIZE, _fd) != PAGE_SIZE)
        return -1;

    fflush(_fd);
    ++appendPageCounter;
    return SUCCESS;
}

/*
Source: https://linux.die.net/man/2/fstat

    Declaration:
        int fstat (int fd, struct stat *statbuf);
    Description:
        fstat() is identical to stat(), except that the file about which information is to be
        retrieved is specified by the file descriptor fd.
    Return Value:
        On success, zero is returned. On error, -1 is returned, and errno is set appropriately.

    off_t   st_size;
        Field that gives the size of the file (if it is a regular file or a symbolic link) in bytes.

    int fileno (FILE *stream)
        examines the argument stream and returns its integer description

*/
unsigned FileHandle::getNumberOfPages()
{
    struct stat sb;
    if (fstat (fileno(_fd), &sb) != 0)
        return 0;

    return sb.st_size / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return SUCCESS;
}

bool fileExists (const string &fileName)
{
    struct stat sb;
    return (stat (fileName.c_str(), &sb) == 0);
}

void FileHandle::setFile(FILE *fd)
{
    _fd = fd;
}

FILE* FileHandle::getFile()
{
    return _fd;
}