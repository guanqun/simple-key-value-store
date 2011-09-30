#ifndef _FIXED_DATABASE_H
#define _FIXED_DATABASE_H

#include "MemoryFile.h"

#include <string.h>

class CFixedDatabase
{
public:
    CFixedDatabase();
    ~CFixedDatabase() {}

    bool Open(const char *path,
              uint64_t fixedLength = 32ULL * 1024ULL /* 32K */);
    bool Close();

    bool Put(const uint64_t key, const std::string &value);
    bool Get(const uint64_t key, std::string &value);
    bool Remove(const uint64_t key);

    bool IterateInitialize();
    bool IterateNext(uint64_t &key, std::string &value);

    bool CleanUp();

    bool Flush();

    bool DeleteDatabase();
    std::string GetIndexFileName();
    std::string GetDataFileName();

private:
#define STC_ENTRY_SET_NUMBER (1 << 8)
    struct STC_ENTRY_SET
    {
        uint64_t offset[STC_ENTRY_SET_NUMBER];
        STC_ENTRY_SET()
        {
            memset(this, 0, sizeof(STC_ENTRY_SET));
        }
    };
#define STC_DATA_HEADER_MAGIC 0xa5a5a5a5
#define STC_DATA_HEADER_USED  0xdeadbeef
    struct STC_DATA_HEADER
    {
        uint32_t magic;
        uint32_t used;
        uint32_t key;
    };
    bool GetOffsetFromKey(const uint64_t key, uint64_t &offset);
    struct STC_DATAFILE_HEADER
    {
        uint64_t magic;
        uint64_t chunkSize;
    };
    bool LoadMeta();
    bool SaveMeta();
    bool CreateNewFile(uint64_t fixedLength);
private:
    CMemoryFile *m_pIndexFile;
    CMemoryFile *m_pDataFile;
    uint64_t m_nChunkSize;
    uint64_t m_nIteratorOffset;
    std::string m_strFilePath;
};

#endif /* _CFIXEDDATABASE_H */
