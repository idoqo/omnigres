#include <signal.h>

#include "pg_yregress.h"

static struct sigaction *prev_interrupt_handler, *prev_term_handler, *prev_abort_handler, *prev_segv_handler;

static void signal_handler(int signum) {
  instances_cleanup();

  // force termination of remaining instances
  kill(0, signum);

  if (signum == SIGINT && prev_interrupt_handler) {
    prev_interrupt_handler->sa_handler(signum);
  } else if (signum == SIGTERM && prev_term_handler) {
    prev_term_handler->sa_handler(signum);
  } else if (signum == SIGABRT && prev_abort_handler) {
    prev_abort_handler->sa_handler(signum);
  } else if (signum == SIGSEGV && prev_segv_handler) {
    prev_segv_handler->sa_handler(signum);
  }
  exit(1);
}

void register_sighandler() {
  struct sigaction sa;
  sa.sa_handler = signal_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;

  sigaction(SIGINT, &sa, prev_interrupt_handler); // For Ctrl+C
  sigaction(SIGTERM, &sa, prev_term_handler);     // For termination request
  sigaction(SIGABRT, &sa, prev_abort_handler);    // For abort
  sigaction(SIGSEGV, &sa, prev_segv_handler);    // For crashes
}
