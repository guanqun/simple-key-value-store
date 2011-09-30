#ifndef _HASH_DATABASE_H
#define _HASH_DATABASE_H

#include <inttypes.h>
#include <string>

class CHashDatabase
{
public:
  CHashDatabase();
  ~CHashDatabase() {}

  bool Open(const char *path);
  bool Close();

  bool Put(const uint64_t key, const std::string &value);
  bool Get(const uint64_t key, std::string &value);
  bool Remove(const uint64_t key);

  bool IterateInitialize();
  bool IterateNext(uint64_t &key, std::string &value);

  bool Flush();

private:
  /* disallow copy constructor and assign operator */
  CHashDatabase(const CHashDatabase &other);
  CHashDatabase& operator=(const CHashDatabase &other);

  uint64_t HashFunction(uint64_t);

  bool SeekRead(off_t off, void *buf, size_t size);
  bool SeekWrite(off_t off, const void *buf, size_t size);

  void DumpMeta(char *buf);
  void LoadMeta(const char *buf);

  struct EntryRecord
  {
    uint64_t m_Offset;
    uint64_t m_LeftOffset;
    uint64_t m_RightOffset;
    uint64_t m_uKey;
    uint64_t m_uValueLength;
    uint64_t m_uRecordSize;
  };

  bool LoadPartialRecord(EntryRecord &rec, uint64_t *type);
  bool LoadRecordValue(const EntryRecord &rec, std::string &value);
  bool SaveRecord(EntryRecord &rec, const std::string &value);

private:
  // file descriptor of the database file
  int m_fd;
  // bucket number
  uint64_t m_uBucketNumber;
  // number of records
  uint64_t m_uRecordNumber;
  // the whole database file size
  uint64_t m_uFileSize;
  // maintain the last offset
  uint64_t m_LastOffset;
  // bucket index
  uint64_t *m_pBucket;
  // used for iteration
  uint64_t m_IteratorOffset;
  // pointer to the mapped memory
  char *m_pMapped;
  // size of the mapped memory
  uint64_t m_uMappedFileSize;
  // cache the file path
  std::string m_FilePath;
};

#endif  /* C_HASH_DATABASE_H */
