#include "FixedDatabase.h"

#include <assert.h>
#include <stdio.h>
#include <string>
using std::string;

/**
 * constructor for CFixedDatabase
 */
CFixedDatabase::CFixedDatabase()
    : m_pIndexFile(NULL),
      m_pDataFile(NULL),
      m_nChunkSize(0),
      m_nIteratorOffset(0)
{
}

/**
 * open the database file
 * @param path specify the path for the database file,
 *             if this file is created, it'll be created
*  @param fixedLength the total size of value, it's fixed
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::Open(const char *path, uint64_t fixedLength)
{
    if (!path)
        return false;

    // open index file
    string index(path);
    m_strFilePath = index;
    index.append(".IDX");
    // map 128M into memory
    m_pIndexFile = new CMemoryFile(index.c_str(), 128);
    if (!m_pIndexFile->Open())
        return false;

    // open data file
    string data(path);
    data.append(".DAT");
    // map 320M into memory
    m_pDataFile = new CMemoryFile(data.c_str(), 320);
    if (!m_pDataFile->Open()) {
        m_pIndexFile->Close();
        return false;
    }

    if (m_pIndexFile->GetFileLastOffset() == 0)
        return CreateNewFile(fixedLength);
    else if (!LoadMeta())
        return false;
    return true;
}

/**
 * create the needed stuff for data file and index file
 * @param fixedLength specify the length that holds value
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::CreateNewFile(uint64_t fixedLength)
{
    // make chunk size align on eight bytes
    m_nChunkSize = fixedLength & ~0x7;
    if (fixedLength & 0x7)
        m_nChunkSize += 0x8;

    // write to data file
    if (!SaveMeta())
        return false;

    // write to index file
    STC_ENTRY_SET es;
    return m_pIndexFile->Write(0ULL, &es, sizeof(es));
}

/**
 * close the database file
 *     use Close() or CleanUp() at last
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::Close()
{
    if (m_pIndexFile && m_pDataFile) {
        if (!m_pIndexFile->Close() ||
            !m_pDataFile->Close())
            return false;
        delete m_pIndexFile;
        delete m_pDataFile;
        m_pIndexFile = NULL;
        m_pDataFile = NULL;
    }
    return true;
}

/**
 * put key-value into the database, overwrite the value if key exists
 * @param key the key to be inserted
 * @param value the value to be inserted
 * @return true if successful, false if failed
 */
bool CFixedDatabase::Put(const uint64_t key, const std::string &value)
{
    assert(m_pIndexFile && m_pDataFile);
    uint64_t offset = 0ULL;
    int i;
    for (i = 0; i < 64; i += 8) {
        STC_ENTRY_SET es;
        int index = (key >> i) & 0xff;
        if (!m_pIndexFile->Read(offset, &es, sizeof(es)))
            return false;
        if (!es.offset[index]) {
            // create the mapping
            uint64_t off = m_pDataFile->GetFileLastOffset();

            // store header section
            STC_DATA_HEADER dh;
            dh.magic = STC_DATA_HEADER_MAGIC;
            dh.used = STC_DATA_HEADER_USED;
            dh.key = key;
            if (!m_pDataFile->Write(off, &dh, sizeof(dh)))
                return false;

            // store value data
            uint64_t length = value.length();
            if (length > m_nChunkSize)
                length = m_nChunkSize;
            if (!m_pDataFile->Write(off + sizeof(dh), value.data(), length))
                return false;
            for (int j = 64 - 8; j > i; j -= 8) {
                STC_ENTRY_SET enew;
                enew.offset[(key >> j) & 0xff] = off;
                off = m_pIndexFile->GetFileLastOffset();
                if (!m_pIndexFile->Write(off, &enew, sizeof(enew)))
                    return false;
            }
            es.offset[index] = off;
            return m_pIndexFile->Write(offset, &es, sizeof(es));
        }
        offset = es.offset[index];
    }

    STC_DATA_HEADER dh;
    if (!m_pDataFile->Read(offset, &dh, sizeof(dh)))
        return false;
    if (dh.magic != STC_DATA_HEADER_MAGIC)
        return false;

    dh.used = STC_DATA_HEADER_USED;
    dh.key = key;
    if (!m_pDataFile->Write(offset, &dh, sizeof(dh)))
        return false;

    return m_pDataFile->Write(offset + sizeof(dh),
                              value.data(),
                              m_nChunkSize);
}

/**
 * get offset from key
 * @param key key used to retrieve
 * @param offset the value got is put here, only valid when this returns true
 * @return true if the current offset is found,
 *         false if such mapping is not set up yet
 */
bool CFixedDatabase::GetOffsetFromKey(const uint64_t key, uint64_t &offset)
{
    offset = 0ULL;
    for (int i = 0; i < 64; i += 8) {
        int index = (key >> i) & 0xff;
        if (!m_pIndexFile->Read(offset + index * sizeof(uint64_t), &offset, sizeof(offset)))
            return false;
        if (!offset)
            return false;
    }
    return true;
}

/**
 * get the corresponding value from database
 * @param key criteria for retrieval
 * @param value retrieved
 * @return true if successful, false if failed
 */
bool CFixedDatabase::Get(const uint64_t key, std::string &value)
{
    assert(m_pIndexFile && m_pDataFile);

    uint64_t offset = 0ULL;
    if (!GetOffsetFromKey(key, offset))
        return false;

    STC_DATA_HEADER dh;
    if (!m_pDataFile->Read(offset, &dh, sizeof(dh)))
        return false;
    if (dh.magic != STC_DATA_HEADER_MAGIC ||
        dh.used != STC_DATA_HEADER_USED ||
        dh.key != key)
        return false;

    value.resize(m_nChunkSize);
    return m_pDataFile->Read(offset + sizeof(dh),
                             (char *)value.data(),
                             m_nChunkSize);
}

/**
 * remove the key and the corresponding value from database
 * @param key criteria for removal
 * @return true if successful, false if failed
 */
bool CFixedDatabase::Remove(const uint64_t key)
{
    assert(m_pIndexFile && m_pDataFile);

    uint64_t offset = 0ULL;
    if (!GetOffsetFromKey(key, offset))
        return false;

    STC_DATA_HEADER dh;
    if (!m_pDataFile->Read(offset, &dh, sizeof(dh)))
        return false;
    if (dh.magic != STC_DATA_HEADER_MAGIC ||
        dh.used != STC_DATA_HEADER_USED ||
        dh.key != key)
        return false;

    // set 'used' flag to removed
    dh.used = 0;
    // set 'key' to '0'
    dh.key = 0;
    return m_pDataFile->Write(offset, &dh, sizeof(dh));
}

/**
 * initialize the iteration
 * @return true if successful, false if failed
 */
bool CFixedDatabase::IterateInitialize()
{
    assert(m_pIndexFile && m_pDataFile);
    // skip the data file header
    m_nIteratorOffset = sizeof(STC_DATAFILE_HEADER);
    return true;
}

/**
 * go to the next iteration
 * @param key the next key retrieved is put here
 * @param value the next value retrieved is put here
 * @return true if the database has the next iteration and the parameters are valid;
 *         false if it goes to the end and parameters are invalid
 */
bool CFixedDatabase::IterateNext(uint64_t &key, std::string &value)
{
    assert(m_pIndexFile && m_pDataFile);

    uint64_t end = m_pDataFile->GetFileLastOffset();
    while (m_nIteratorOffset < end) {
        STC_DATA_HEADER dh;
        if (!m_pDataFile->Read(m_nIteratorOffset, &dh, sizeof(dh)))
            return false;
        uint64_t valueOffset = m_nIteratorOffset + sizeof(dh);
        m_nIteratorOffset += sizeof(dh) + m_nChunkSize;

        if (dh.magic == STC_DATA_HEADER_MAGIC &&
            dh.used == STC_DATA_HEADER_USED) {
            key = dh.key;
            value.resize(m_nChunkSize);
            return m_pDataFile->Read(valueOffset,
                                     (char *)value.data(),
                                     m_nChunkSize);
        }
    }
    return false;
}

/**
 * flush the database
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::Flush()
{
    assert(m_pIndexFile && m_pDataFile);
    // TODO: do all the necessary stuff to flush
    if (!m_pIndexFile->Flush() ||
        !m_pDataFile->Flush())
        return false;
    return true;
}

/**
 * load metadata for data file
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::LoadMeta()
{
    assert(m_pIndexFile && m_pDataFile);

    STC_DATAFILE_HEADER dfh;
    if (!m_pDataFile->Read(0ULL, &dfh, sizeof(dfh)))
        return false;
    if (dfh.magic != 0x3f)
        return false;
    m_nChunkSize = dfh.chunkSize;
    return true;
}

/**
 * save metadata for data file
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::SaveMeta()
{
    assert(m_pIndexFile && m_pDataFile);
    STC_DATAFILE_HEADER dfh;
    dfh.magic = 0x3f;
    dfh.chunkSize = m_nChunkSize;
    return m_pDataFile->Write(0ULL, &dfh, sizeof(dfh));
}

/**
 * cleanup all removed data, make data file more compact
 *     use Close() or CleanUp() at last
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::CleanUp()
{
    string p(m_strFilePath);
    p.append(".tmp");

    Flush();

    CFixedDatabase *other = new CFixedDatabase;
    if (!other->Open(p.c_str(), m_nChunkSize)) {
        delete other;
        return false;
    }

    uint64_t left = 0;
    uint64_t k;
    string v;
    IterateInitialize();
    while (IterateNext(k, v)) {
        left++;
        if (!other->Put(k, v))
            return false;
    }

    Close();
    other->Close();

    if (left) {
        if (rename(other->GetIndexFileName().c_str(), GetIndexFileName().c_str()))
            return false;
        if (rename(other->GetDataFileName().c_str(), GetDataFileName().c_str()))
            return false;
    } else {
        // there is nothing left, remove them all
        DeleteDatabase();
        other->DeleteDatabase();
    }

    delete other;
    return true;
}

/**
 * delete the whole database, delete two files
 * @return true if it's successful, false if it fails
 */
bool CFixedDatabase::DeleteDatabase()
{
    unlink(GetIndexFileName().c_str());
    unlink(GetDataFileName().c_str());
    return true;
}

/**
 * return the index file name
 * @return index file name
 */
string CFixedDatabase::GetIndexFileName()
{
    string s(m_strFilePath);
    s.append(".IDX");
    return s;
}

/**
 * return the data file name
 * @return data file name
 */
string CFixedDatabase::GetDataFileName()
{
    string s(m_strFilePath);
    s.append(".DAT");
    return s;
}
