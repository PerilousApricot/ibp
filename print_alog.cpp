#include "ibp_server.h"
#include "activity_log.h"
#include "log.h"

//** Dummy routine and variable
void print_config(FILE *fd, Config_t *cfg) { }
Config_t *global_config; 

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
  long pos, eof_pos;

  _alog_init_constants();

  if (argc < 2) {
     printf("print_alog filename\n");
     return(0);
  }

  activity_log_t *alog =  activity_log_open(argv[1], 0, ALOG_READ);

  pos = ftell(alog->fd);
  fseek(alog->fd, 0, SEEK_END);
  eof_pos = ftell(alog->fd);
  fseek(alog->fd, pos, SEEK_SET);

  do {
    pos = ftell(alog->fd);
  } while (activity_log_read_next_entry(alog, stdout) == 0);

  pos = ftell(alog->fd);
  if (pos != eof_pos) {
     printf("print_alog: Processing aborted due to short record!  curr_pos = %ld eof = %ld\n", pos, eof_pos);
  }

  activity_log_close(alog);

  return(0);
}
