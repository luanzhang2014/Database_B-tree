#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode()
{
  *(int *)buffer = 0;
  setNextNodePtr(-1);
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
  if(pf.read(pid, buffer)){
    fprintf(stderr, "cannot read from PageFile\n");
    return RC_FILE_READ_FAILED;
  }
  return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
  if(pf.write(pid, buffer)){
    fprintf(stderr, "cannot write into PageFile");
    return RC_FILE_WRITE_FAILED;
  }
  return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
  return *((int *)buffer); 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
  int n = getKeyCount();
  if (n >= MAXKEY_NUM) return RC_NODE_FULL;
  int eid;
  locate(key, eid);
  PageId pid = getNextNodePtr();

  for(int i = n; i > eid; i--){
    memcpy(buffer+sizeof(int)+i*ENTRY_SIZE, buffer+sizeof(int)+(i-1)*ENTRY_SIZE, ENTRY_SIZE);
  }

  *(RecordId *)(buffer+sizeof(int)+eid*ENTRY_SIZE) = rid;
  *(int *)(buffer+sizeof(int)+eid*ENTRY_SIZE+sizeof(RecordId)) = key;
  (*(int *)(buffer))++;
  setNextNodePtr(pid);
  return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
  int n = getKeyCount();
  int eid;
  locate(key, eid);

  PageId pid = getNextNodePtr();
  for(int i = n; i > eid; i--){
    memcpy(buffer+sizeof(int)+i*ENTRY_SIZE, buffer+sizeof(int)+(i-1)*ENTRY_SIZE, ENTRY_SIZE);
  }

  *(RecordId *)(buffer+sizeof(int)+eid*ENTRY_SIZE) = rid;
  *(int *) (buffer+sizeof(int)+eid*ENTRY_SIZE+sizeof(RecordId)) = key;

  int left = (MAXKEY_NUM+1)/2;
  int right = MAXKEY_NUM+1-left;

  *(int *)buffer = left;
  *(int *)sibling.buffer = right;

  memcpy(sibling.buffer+sizeof(int), buffer+sizeof(int)+left*ENTRY_SIZE, right*ENTRY_SIZE);
    sibling.setNextNodePtr(pid);
    siblingKey = *(int *)(sibling.buffer+sizeof(int)+sizeof(RecordId));
  return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
  int offset = sizeof(int)+sizeof(RecordId);
  for(eid = 0; eid < getKeyCount(); eid++){
    if(*(int *)(buffer+offset+eid*ENTRY_SIZE) >= searchKey)
      break;
  }
  if(eid == getKeyCount())	return RC_NO_SUCH_RECORD;
  return 0; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
  int offset = sizeof(int)+eid*ENTRY_SIZE;
  rid = *(RecordId *)(buffer+offset);
  key = *(int *)(buffer+offset+sizeof(RecordId));
  return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ return *(PageId *)(buffer+sizeof(int)+getKeyCount()*ENTRY_SIZE); }

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
  *(PageId *)(buffer+sizeof(int)+getKeyCount()*ENTRY_SIZE) = pid;
  return 0; 
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
  if(pf.read(pid, buffer)){
    fprintf(stderr, "cannot read from PageFile\n");
    return RC_FILE_READ_FAILED;
  }
  return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
  if(pf.write(pid, buffer)){
    fprintf(stderr, "cannot write into PageFile");
    return RC_FILE_WRITE_FAILED;
  }
  return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{	return *((int *)buffer); }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
  int n = getKeyCount();
  if (n >= MAXKEY_NUM) return RC_NODE_FULL;
  //find out where to insert the (key, pid) pair
  int eid = 0;
  for (eid = 0; eid < n; eid++){
    if ((*(int *)(buffer+sizeof(int)+sizeof(PageId)+eid*ENTRY_SIZE)) > key)
      break;
  }

  for(int i = n; i > eid; i--){
    memcpy(buffer+sizeof(int)+sizeof(PageId)+i*ENTRY_SIZE, buffer+sizeof(int)+sizeof(PageId)+(i-1)*ENTRY_SIZE, ENTRY_SIZE);
  }
  *(PageId *)(buffer+sizeof(int)+sizeof(PageId)+sizeof(int)+eid*ENTRY_SIZE) = pid;
  *(int *)(buffer+sizeof(int)+sizeof(PageId)+eid*ENTRY_SIZE) = key;
  (*(int *)(buffer))++;
  return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
  int n = getKeyCount();
  int eid = 0;
  for (eid = 0; eid < n; eid++){
    if ((*(int *)(buffer+sizeof(int)+sizeof(PageId)+eid*ENTRY_SIZE)) > key)
      break;
  }
  for(int i = n; i > eid; i--){
    memcpy(buffer+sizeof(int)+sizeof(PageId)+i*ENTRY_SIZE, buffer+sizeof(int)+sizeof(PageId)+(i-1)*ENTRY_SIZE, ENTRY_SIZE);
  }
  *(PageId *)(buffer+sizeof(int)+sizeof(PageId)+sizeof(int)+eid*ENTRY_SIZE) = pid;
  *(int *) (buffer+sizeof(int)+sizeof(PageId)+eid*ENTRY_SIZE) = key;

  int left = (MAXKEY_NUM+1)/2;
  int right = MAXKEY_NUM-left;

  *(int *)buffer = left;
  *(int *)sibling.buffer = right;

  memcpy(sibling.buffer+sizeof(int), buffer+sizeof(int)+(left+1)*ENTRY_SIZE, sizeof(PageId)+right*ENTRY_SIZE);
  midKey = *(int *)(sibling.buffer+sizeof(int)+sizeof(PageId));

  return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
  int offset = sizeof(int) + sizeof(PageId);
  int eid;
  for (eid = 0; eid < getKeyCount(); eid++){
    if ((*(int *)(buffer+offset+eid*ENTRY_SIZE)) > searchKey)
      break;
  }
  pid = *(PageId *)(buffer+offset+eid*ENTRY_SIZE-sizeof(PageId));

  return 0; 
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
  *(int *)buffer = 1;
  *(PageId *)(buffer+sizeof(int)) = pid1;
  *(int *)(buffer+sizeof(int)+sizeof(PageId)) = key;
  *(PageId *)(buffer+sizeof(int)+ENTRY_SIZE) = pid2;
  return 0; 
}
