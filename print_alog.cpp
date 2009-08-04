#include <time.h>
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
  uint64_t ui;
  time_t t;
  alog_file_header_t ah;
  char start_time[256], end_time[256];

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

  //** Print the header **
  ah = get_alog_header(alog);
  printf("------------------------------------------------------------------\n");
  ui = eof_pos;
  printf("Activity log: %s  (" LU " bytes)\n", argv[1], ui);
  printf("Version: " LU "\n", ah.version);
  printf("Current State: " LU " (1=GOOD, 0=BAD)\n", ah.state);
  t = ah.start_time; ctime_r(&t, start_time); start_time[strlen(start_time)-1] = '\0';  //** chomp the \n from ctime_r
  t = ah.end_time; ctime_r(&t, end_time); end_time[strlen(end_time)-1] = '\0';  //** chomp the \n from ctime_r
  printf("Start date: %s (" LU ")\n", start_time, ah.start_time);
  printf("  End date: %s (" LU ")\n", end_time, ah.end_time);
  printf("------------------------------------------------------------------\n\n");
  

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
