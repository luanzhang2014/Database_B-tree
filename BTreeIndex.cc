#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
  rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode) 
{
  if(pf.open(indexname, mode)){
    fprintf(stderr, "cannot open PageFile");
    return RC_FILE_OPEN_FAILED;
  }
  char buffer[PageFile::PAGE_SIZE];
  if(pf.endPid() == 0){
    treeHeight = 0;
    rootPid = -1;
    *(int *)buffer = treeHeight;
    *(PageId *)(buffer+sizeof(int)) = rootPid;
    if(pf.write(pf.endPid(), buffer)){
      fprintf(stderr, "Error: cannot write to the index file\n");
      return RC_FILE_WRITE_FAILED;
    }
  }
  if(pf.read(0, buffer)){
    fprintf(stderr, "Error: cannot read from the index file\n");
    return RC_FILE_READ_FAILED;      
  }
  treeHeight = *(int *)buffer;
  rootPid = *(PageId *)(buffer+sizeof(int));
  write = false;
  if(mode == 'w'){
    write = true;
  }
  return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
  if(write){                          //write into index file before close
    char buffer[PageFile::PAGE_SIZE];
    *(int *)buffer = treeHeight;
    *(PageId *)(buffer+sizeof(int)) = rootPid;
    if(pf.write(0, buffer)){
      fprintf(stderr, "Error: cannot write to the index file\n");
      return RC_FILE_WRITE_FAILED;      
    }
  }
  if(pf.close()){
    return RC_FILE_CLOSE_FAILED;
  }
  return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
  if(rootPid == -1){
    BTLeafNode node;
    node.insert(key, rid);
    rootPid = pf.endPid();
    treeHeight = 1;
    node.write(pf.endPid(),pf);
    return 0;
  }
  int level = 1;
  PageId pid = rootPid;
  PageId newPid;
  int midKey;
  if(subInsert(rootPid, key, rid, level, midKey, newPid)){
    BTNonLeafNode node;
    node.initializeRoot(rootPid, midKey, newPid);
    rootPid = pf.endPid();
    treeHeight++;
    node.write(pf.endPid(), pf);
  }
  return 0;
}


/*
 *return true, if overflow
 */
bool BTreeIndex::subInsert(PageId pid, int key, const RecordId& rid, int level, int& midKey, PageId& newPid)
{
  if(level < treeHeight){
    BTNonLeafNode node;
    if(node.read(pid, pf)){
      fprintf(stderr, "Error: cannot read from PageFile \n");
      return true;
    }
    PageId child;
    node.locateChildPtr(key, child);
    if(subInsert(child, key, rid, level+1, midKey, newPid)){
      if(node.getKeyCount() < BTNonLeafNode::MAXKEY_NUM){
        node.insert(midKey, newPid);
        node.write(pid, pf);
        return false;
      }
      else{
        BTNonLeafNode newNode;
        node.insertAndSplit(midKey, newPid, newNode, midKey);
        node.write(pid, pf);
        newPid = pf.endPid();
        newNode.write(pf.endPid(), pf);
        return true;
      }
    }
    return false;
  }
  else{
    BTLeafNode node;
    if(node.read(pid, pf)){
      fprintf(stderr, "Error: cannot read from PageFile \n");
      return RC_FILE_READ_FAILED;       
    }
    if(node.getKeyCount() < BTLeafNode::MAXKEY_NUM){
      node.insert(key, rid);
      node.write(pid, pf);
      return false;
    }
    else{
      BTLeafNode newNode;
      node.insertAndSplit(key, rid, newNode, midKey);
      node.setNextNodePtr(pf.endPid());
      node.write(pid, pf);
      newPid = pf.endPid();
      newNode.write(pf.endPid(), pf);
      return true;
    }
  }
}


/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
  if(rootPid == -1) return RC_NO_SUCH_RECORD;
  PageId pid = rootPid;
  for(int i = 1; i < treeHeight; i++){
    BTNonLeafNode node;
    node.read(pid, pf);
    node.locateChildPtr(searchKey, pid);
  }
  BTLeafNode node;
  node.read(pid, pf);
  if(node.locate(searchKey, cursor.eid)){
    return RC_NO_SUCH_RECORD;
  }
  cursor.pid = pid;
  return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
  BTLeafNode node;
  if(node.read(cursor.pid, pf)){
    return RC_FILE_READ_FAILED;
  }
  node.readEntry(cursor.eid, key, rid);
  if(cursor.eid < node.getKeyCount()-1)
    cursor.eid++;
  else{
    cursor.eid = 0;
    cursor.pid = node.getNextNodePtr();
  }
  return 0;
}
