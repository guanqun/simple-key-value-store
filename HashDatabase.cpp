#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdlib.h>
#include <iostream>

#include "HashDatabase.h"

using namespace std;

#define HEADER_SECTION_MAGIC      "dadazui"
#define HEADER_SECTION_SIZE       40
#define DEFAULT_BUCKET_NUMBER     131071ULL
#define DEFAULT_FREE_POOL_NUMBER  1024ULL
#define RECORD_SIZE               40
#define RECORD_MAGIC              0x12345678
#define RECORD_MAGIC_FREE         0xdeadbeaf
#define MAX_MAPPED_SIZE           (1024ULL * 1024ULL * 128ULL)

CHashDatabase::CHashDatabase()
  : m_fd(0),
    m_uBucketNumber(0ULL),
    m_uRecordNumber(0ULL),
    m_uFileSize(0ULL),
    m_LastOffset(0ULL),
    m_pBucket(NULL),
    m_IteratorOffset(HEADER_SECTION_SIZE + DEFAULT_BUCKET_NUMBER * sizeof(uint64_t)),
    m_pMapped(NULL),
    m_uMappedFileSize(0ULL)
{
}

/**
 * open the database
 * @param path specify the file path for the hash database
 * @return true if successful, false if failed
 */
bool CHashDatabase::Open(const char *path)
{
  m_FilePath = path;

  m_fd = open(path, O_RDWR | O_CREAT, 0644);
  if (m_fd < 0)
    return false;

  struct stat st;
  if (fstat(m_fd, &st) < 0 || !S_ISREG(st.st_mode)) {
    close(m_fd);
    return false;
  }

  void *map = mmap(0, MAX_MAPPED_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd, 0);
  if (map == MAP_FAILED) {
    close(m_fd);
    return false;
  }
  m_pMapped = (char *)map;
  m_pBucket = (uint64_t *)(m_pMapped + HEADER_SECTION_SIZE);
  m_uMappedFileSize = MAX_MAPPED_SIZE;

  if (st.st_size == 0) {
    m_uBucketNumber = DEFAULT_BUCKET_NUMBER;
    m_uRecordNumber = 0ULL;
    m_LastOffset = HEADER_SECTION_SIZE +
                  DEFAULT_BUCKET_NUMBER * sizeof(uint64_t);
                  //DEFAULT_FREE_POOL_NUMBER * sizeof(FreePoolRecord);
    m_uFileSize = m_LastOffset;
    if (ftruncate(m_fd, m_uFileSize) < 0) {
      close(m_fd);
      return false;
    }
    DumpMeta(m_pMapped);
  } else {
    if (strncmp(m_pMapped, HEADER_SECTION_MAGIC, 7)) {
      close(m_fd);
      return false;
    }
    LoadMeta(m_pMapped);
    m_uFileSize = st.st_size;
  }

  return true;
}

/**
 * close the database
 * @return true if successful, false if failed
 */
bool CHashDatabase::Close()
{
  DumpMeta(m_pMapped);

  // sync for memory and file
  msync(m_pMapped, MAX_MAPPED_SIZE, MS_SYNC);
  fsync(m_fd);

  // unmap and close
  munmap(m_pMapped, MAX_MAPPED_SIZE);
  close(m_fd);

  return true;
}

uint64_t CHashDatabase::HashFunction(const uint64_t key)
{
  return (key % m_uBucketNumber);
}

/**
 * put key-value into the database, overwrite the value if key exists
 * @param key the key to be inserted
 * @param value the value to be inserted
 * @return true if successful, false if failed
 */
bool CHashDatabase::Put(const uint64_t key, const string &value)
{
  uint64_t index = HashFunction(key);
  uint64_t off = m_pBucket[index];
  uint64_t parent_off = HEADER_SECTION_SIZE + index * sizeof(uint64_t);

  EntryRecord rec;
  while (off) {
    rec.m_Offset = off;
    if (!LoadPartialRecord(rec, NULL))
      return false;
    if (rec.m_uKey == key) {
      // found the key-value pair
      break;
    } else if (rec.m_uKey > key) {
      // go left
      parent_off = off + 8;   /* offset of Left */
      off = rec.m_LeftOffset;
    } else {
      // go right
      parent_off = off + 16;  /* offset of Right */
      off = rec.m_RightOffset;
    }
  }

  if (off == 0 || rec.m_uValueLength < value.length()) {
    // allocate a new place
    // currently, we append to the tail of file
    rec.m_Offset = m_LastOffset;
    rec.m_LeftOffset = 0ULL;
    rec.m_RightOffset = 0ULL;
    rec.m_uKey = key;
  }
  rec.m_uValueLength = value.length();
  SaveRecord(rec, value);

  // change the parent pointer/offset
  if (!SeekWrite(parent_off, &rec.m_Offset, sizeof(uint64_t)))
    return false;

  m_uRecordNumber++;
  return true;
}

/**
 * get the corresponding value from database
 * @param key criteria for retrieval
 * @param value retrieved
 * @return true if successful, false if failed
 */
bool CHashDatabase::Get(const uint64_t key, string &value)
{
  uint64_t index = HashFunction(key);
  uint64_t off = m_pBucket[index];

  while (off) {
    EntryRecord rec;
    rec.m_Offset = off;
    if (!LoadPartialRecord(rec, NULL))
      return false;
    if (rec.m_uKey == key) {
      // found the key-value pair
      if (!LoadRecordValue(rec, value))
        return false;
      return true;
    } else if (rec.m_uKey > key) {
      // go left
      off = rec.m_LeftOffset;
    } else {
      // go right
      off = rec.m_RightOffset;
    }
  }
  return false;
}

/**
 * remove the key and the corresponding value from database
 * @param key criteria for removal
 * @return true if successful, false if failed
 */
bool CHashDatabase::Remove(const uint64_t key)
{
  uint64_t index = HashFunction(key);
  uint64_t off = m_pBucket[index];
  uint64_t parent_off = HEADER_SECTION_SIZE + index * sizeof(uint64_t);

  EntryRecord rec;
  while (off) {
    rec.m_Offset = off;
    if (!LoadPartialRecord(rec, NULL))
      return false;
    if (rec.m_uKey == key) {
      // found the key-value pair
      break;
    } else if (rec.m_uKey > key) {
      // go left
      parent_off = off + 8;   /* offset of Left */
      off = rec.m_LeftOffset;
    } else {
      // go right
      parent_off = off + 16;  /* offset of Right */
      off = rec.m_RightOffset;
    }
  }

  if (off == 0)
    return false;

  uint64_t magic = RECORD_MAGIC_FREE;
  if (!SeekWrite(rec.m_Offset, &magic, sizeof(uint64_t)))
    return false;
  if (rec.m_LeftOffset == 0 && rec.m_RightOffset == 0) {
    // leaf node
    if (!SeekWrite(parent_off, &rec.m_LeftOffset, sizeof(uint64_t)))
      return false;
  } else if (rec.m_LeftOffset) {
    // left side has children
    EntryRecord erec;
    erec.m_Offset = rec.m_LeftOffset;
    uint64_t left_parent_off = rec.m_Offset + 8; /* offset of Left */
    if (!LoadPartialRecord(erec, NULL))
      return false;
    while (erec.m_RightOffset) {
      left_parent_off = erec.m_Offset + 16;  /* offset of Right */
      // go straight right
      erec.m_Offset = erec.m_RightOffset;
      if (!LoadPartialRecord(erec, NULL))
        return false;
    }
    if (!SeekWrite(left_parent_off, &erec.m_LeftOffset, sizeof(uint64_t)) ||
        !LoadPartialRecord(rec, NULL) ||
        !SeekWrite(parent_off, &erec.m_Offset, sizeof(uint64_t)) ||
        !SeekWrite(erec.m_Offset + 8, &rec.m_LeftOffset, sizeof(uint64_t)) ||
        !SeekWrite(erec.m_Offset + 16, &rec.m_RightOffset, sizeof(uint64_t)))
      return false;
  } else {
    // right side has children
    EntryRecord erec;
    erec.m_Offset = rec.m_RightOffset;
    uint64_t right_parent_off = rec.m_Offset + 16; /* offset of Right */
    if (!LoadPartialRecord(erec, NULL))
      return false;
    while (erec.m_LeftOffset) {
      right_parent_off = erec.m_Offset + 8; /* offset of Left */
      // got straight left
      erec.m_Offset = erec.m_LeftOffset;
      if (!LoadPartialRecord(erec, NULL))
        return false;
    }
    if (!SeekWrite(right_parent_off, &erec.m_RightOffset, sizeof(uint64_t)) ||
        !LoadPartialRecord(rec, NULL) ||
        !SeekWrite(parent_off, &erec.m_Offset, sizeof(uint64_t)) ||
        !SeekWrite(erec.m_Offset + 8, &rec.m_LeftOffset, sizeof(uint64_t)) ||
        !SeekWrite(erec.m_Offset + 16, &rec.m_RightOffset, sizeof(uint64_t)))
    return false;
  }

  m_uRecordNumber--;
  return true;
}

/**
 * initialize the iteration
 * @return true if successful, false if failed
 */
bool CHashDatabase::IterateInitialize()
{
  // reset to the beginning of records
  m_IteratorOffset = HEADER_SECTION_SIZE + DEFAULT_BUCKET_NUMBER * sizeof(uint64_t);
  return true;
}

/**
 * go to the next iteration
 * @param key the next key retrieved is put here
 * @param value the next value retrieved is put here
 * @return true if the database has the next iteration and the parameters are valid;
 *         false if it goes to the end and parameters are invalid
 */
bool CHashDatabase::IterateNext(uint64_t &key, string &value)
{
  while (m_IteratorOffset < m_LastOffset) {
    uint64_t type;
    EntryRecord er;
    er.m_Offset = m_IteratorOffset;
    if (!LoadPartialRecord(er, &type))
      return false;
    m_IteratorOffset += er.m_uRecordSize;
    if (type == RECORD_MAGIC) {
      key = er.m_uKey;
      return Get(key, value);
    }
  }
  return false;
}

/**
 * flush all data to persistent storage
 * @return true if successful, false if failed
 */
bool CHashDatabase::Flush()
{
  /* TODO: do whatever it requires... */
  return true;
}

/**
 * seek and read data from the file
 * @param off the offset in the file
 * @param buf place where the data should be stored
 * @param size the size of the data
 * @return true if successful, false if failed somewhere
 */
bool CHashDatabase::SeekRead(off_t off, void *buf, size_t size)
{
  assert (off >= 0 && buf && size >= 0);
  uint64_t end = off + size;
  if (end <= m_uMappedFileSize) {
    memcpy(buf, m_pMapped + off, size);
    return true;
  }

  while (true) {
    ssize_t loaded = pread(m_fd, buf, size, off);
    if (loaded >= (ssize_t)size) {
      break;
    } else if (loaded > 0) {
      buf = (char *)buf + loaded;
      size -= loaded;
      off += loaded;
    } else if (loaded == -1) {
      if (errno != EINTR)
        return false;
    } else {
      if (size > 0) {
        /* no space */
        return false;
      }
    }
  }

  return true;
}

/**
 * seek and write data into the file
 * @param off the offset in the file
 * @param buf data to be written
 * @param size the size of the data
 * @return true if successful, false if failed somewhere
 */
bool CHashDatabase::SeekWrite(off_t off, const void *buf, size_t size)
{
  assert (off >= 0 && buf && size >= 0);
  uint64_t end = off + size;

  if (end > m_uFileSize) {
    uint64_t extend = end + 32768ULL;
    if (ftruncate(m_fd, extend) < 0)
      return false;
    m_uFileSize = extend;
  }

  if (end > m_LastOffset)
    m_LastOffset = end;

  if (end <= m_uMappedFileSize) {
    memcpy(m_pMapped + off, buf, size);
    return true;
  }

  while (true) {
    ssize_t written = pwrite(m_fd, buf, size, off);
    if (written >= (ssize_t)size) {
      break;
    } else if (written > 0) {
      buf = (char *)buf + written;
      size -= written;
      off += written;
    } else if (written == -1) {
      if (errno != EINTR)
        return false;
    } else {
      if (size > 0)
        return false;
    }
  }

  return true;
}

/**
 * dump fields to buffer
 *  the structure of meta data is as follows:
 *      <name>            <offset>  <length>
 *    magic number           0         8
 *    bucket number          8         8
 *    record number          16        8
 *    last available offset  32        8
 *  the total size of header section is 40 bytes.
 * @param buf the place where buffer is put
 */
void CHashDatabase::DumpMeta(char *buf)
{
  memset(buf, 0, HEADER_SECTION_SIZE);
  strcpy(buf, HEADER_SECTION_MAGIC);
  memcpy(buf + 8, &m_uBucketNumber, sizeof(uint64_t));
  memcpy(buf + 16, &m_uRecordNumber, sizeof(uint64_t));
  memcpy(buf + 24, &m_LastOffset, sizeof(uint64_t));
}

/**
 * load data into fields
 *   we don't check magic number here,
 *   since it will be checked before this function is invoked.
 * @param buf the place where we gets our data
 */
void CHashDatabase::LoadMeta(const char *buf)
{
  memcpy(&m_uBucketNumber, buf + 8, sizeof(uint64_t));
  memcpy(&m_uRecordNumber, buf + 16, sizeof(uint64_t));
  memcpy(&m_LastOffset, buf + 24, sizeof(uint64_t));
}

/**
 * this only load partial record except the actual content of value
 *   user provides the offset and this function returns other fields
 * @param rec data to place
 * @return true if successful, false if failed
 */
bool CHashDatabase::LoadPartialRecord(EntryRecord &rec, uint64_t *type)
{
  uint64_t magic;
  if (!SeekRead(rec.m_Offset, &magic, 8))
    return false;

  if (magic != RECORD_MAGIC && magic != RECORD_MAGIC_FREE)
    return false;

  if (!SeekRead(rec.m_Offset + 8, ((char *)&rec) + 8, 40))
    return false;

  if (type)
    *type = magic;
  return true;
}

/**
 * according to EntryRecord, this stores the actual value
 *   we assume the fields in rec are right
 *   LoadPartialRecord should be called before this one.
 * @param rec provides offset and value length information
 * @param value stores the actual value after this function returns
 * @return true if successful, false if failed
 */
bool CHashDatabase::LoadRecordValue(const EntryRecord &rec, string &value)
{
  char *buf = (char *)malloc(rec.m_uValueLength);
  if (!buf)
    return false;

  if (!SeekRead(rec.m_Offset + 48, buf, rec.m_uValueLength))
    return false;

  value.assign(buf, rec.m_uValueLength);
  free(buf);
  return true;
}

/**
 * save record to the database file
 * @param rec stores the useful information such as offset etc.
 * @param value stores the value part
 * @return true if successful, false if failed
 */
bool CHashDatabase::SaveRecord(EntryRecord &rec, const string &value)
{
  assert(rec.m_uValueLength == value.length());
  off_t off = rec.m_Offset;
  uint64_t magic = RECORD_MAGIC;
  if (!SeekWrite(off, &magic, sizeof(magic)))
    return false;
  off += sizeof(magic);
  rec.m_uRecordSize = 48 + rec.m_uValueLength;
  if (rec.m_uValueLength % 8)
    rec.m_uRecordSize += (8 - (rec.m_uValueLength % 8));
  if (!SeekWrite(off, (char *)&rec + 8, 40))
    return false;
  off += 40;
  if (!SeekWrite(off, value.c_str(), value.length()))
    return false;
  off += value.length();

  // padding if necessary
  if (rec.m_uValueLength % 8) {
    char padding[8];
    memset(padding, 0, sizeof(padding));
    if (!SeekWrite(off, padding, 8 - (rec.m_uValueLength % 8)))
      return false;
  }
  return true;
}
