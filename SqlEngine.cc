#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  BTreeIndex indexFile;
  vector<SelCond> valueCond;

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0){
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  //check the index file
  if(!indexFile.open(table+".idx", 'r')){
    count = 0;
    int keyMin = INT_MIN, keyMax = INT_MAX;
    // check boundary condition
    bool minequal = false, maxequal = false;
    vector<int> NElist;
    for (unsigned i = 0; i < cond.size(); i++){
      if (cond[i].attr == 2) valueCond.push_back(cond[i]);
      else{
        int val = atoi(cond[i].value);
        switch (cond[i].comp){
          case SelCond::EQ:   
            if (val>keyMin){
              keyMin = val;
              minequal = true;
            }
            if (val<keyMax){
              keyMax = val;
              maxequal = true;
            }
            break;
          case SelCond::NE:
            NElist.push_back(val);
            break;
          case SelCond::GT:
            if (val>keyMin || (val==keyMin && minequal)){
              keyMin = val;
              minequal = false;
            }
            break;
          case SelCond::LT:
            if (val<keyMax || (val==keyMax && minequal)){
              keyMax = val;
              maxequal = false;
            }
            break;
          case SelCond::GE:
            if (val>keyMin){
              keyMin = val;
              minequal = true;
            }
            break;
          case SelCond::LE:
            if (val<keyMax){
              keyMax = val;
              maxequal = true;
            }
            break;
        }
      }
    }
    if (keyMin > keyMax || (keyMax == keyMin && (!minequal || !maxequal))){
      rf.close();
      return rc;
    }
    IndexCursor startidx, endidx, temp, currentidx;
    indexFile.locate(keyMin, startidx);
    temp = startidx;
    indexFile.readForward(startidx, key, rid);
    if (key > keyMin || minequal) startidx = temp;

    if(!indexFile.locate(keyMax, endidx)){
      temp = endidx;
      indexFile.readForward(endidx, key, rid);
      if (key < keyMax || !maxequal) endidx = temp;
    }   

    currentidx = startidx;
    while (currentidx.pid!=-1 && (currentidx.pid!=endidx.pid || currentidx.eid!=endidx.eid)){
      indexFile.readForward(currentidx, key, rid);
      int i;
      for (i = 0; i<NElist.size(); i++)
        if (key == NElist[i]) break;
      if (i < NElist.size()) continue;
      if (valueCond.empty() && (attr==1||attr==4)){
        count++;
        if (attr == 1)
        fprintf(stdout, "%d\n", key);
      }
      else{
        if ((rc = rf.read(rid, key, value)) < 0){
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          rf.close();
          return rc;
        }
        if (meetCond(valueCond, key, value)){
          count++;                    
                    // print the tuple
          switch (attr){
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
          }
        }
      }
    }
    // print matching tuple count if "select count(*)"
    if (attr == 4){
      fprintf(stdout, "%d\n", count);
    }
    rf.close();
    return rc;
  }
}


bool SqlEngine::meetCond(const std::vector<SelCond>& cond, const int key, const string& value)
{
  int diff = 0;
  // check the conditions on the tuple
  for (unsigned i = 0; i < cond.size(); i++){
        // compute the difference between the tuple value and the condition value
    switch (cond[i].attr){
      case 1:
        diff = key - atoi(cond[i].value);
        break;
      case 2:
        diff = strcmp(value.c_str(), cond[i].value);
        break;
    }
        
    // skip the tuple if any condition is not met
    switch (cond[i].comp){
      case SelCond::EQ:
        if (diff != 0) return false;
        break;
      case SelCond::NE:
        if (diff == 0) return false;
        break;
      case SelCond::GT:
        if (diff <= 0) return false;
        break;
      case SelCond::LT:
        if (diff >= 0) return false;
        break;
      case SelCond::GE:
        if (diff < 0) return false;
        break;
      case SelCond::LE:
        if (diff > 0) return false;
        break;
    }
  }
  return true;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  std::ifstream infile;
  infile.open(loadfile.c_str());
  if(!infile.is_open()){
    fprintf(stderr, "Error: file %s doesn't exist or cannot open\n", loadfile.c_str());
    return RC_FILE_OPEN_FAILED;
  }
  RecordFile outfile;
  if(outfile.open(table + ".tbl", 'w')){
    fprintf(stderr, "Error: file %s doesn't exist or cannot be created\n", table.c_str());
    return RC_FILE_OPEN_FAILED;
  }
  BTreeIndex indexFile;
  if(index){
    if(indexFile.open(table+".idx", 'w')){
      fprintf(stderr, "Error: index file %s doesn't exist or cannot be created\n", table.c_str());
      return RC_FILE_OPEN_FAILED;      
    }
  }

  string line;
  int key;
  string value;
  RecordId rid;

  while(getline(infile, line)){
    parseLoadLine(line, key, value);
    if(outfile.append(key, value, rid)){
      fprintf(stderr, "Error: cannot append record into file %s \n", table.c_str()); 
      return RC_FILE_WRITE_FAILED;     
    }
    if(index){
      if(indexFile.insert(key, rid)){
        fprintf(stderr, "Error: cannot insert record into index file %s \n", table.c_str()); 
        return RC_FILE_WRITE_FAILED;             
      }
    }
  }
  infile.close();
  outfile.close();
  if(index){
    indexFile.close();
  }
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
