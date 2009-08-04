//**** Dummy EBOFS header ***

#include "common/object.h"
#include "common/pobject.h"

class Ebofs { 
public:
  Ebofs(char *devfn, char *jfn=0) { };
  int mount() { return(0); };
  int umount() { return(0); };
  int mkfs() {return(0); };
  int statfs(struct statfs *buf) { return(-1); };
};

