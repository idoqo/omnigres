#include <assert.h>
#include <errno.h>
#include <ftw.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <signal.h>

#include "pg_yregress.h"

typedef enum {
  POSTMASTER_READY,
  POSTMASTER_STILL_STARTING,
  POSTMASTER_FAILED,
} PostgresStatus;

static void retrieve_postmaster_pid(yinstance *instance) {
  if (instance->managed) {
    char *pid_filename;
    asprintf(&pid_filename, "%.*s/postmaster.pid",
             (int)IOVEC_STRLIT(instance->info.managed.datadir));
    FILE *fp = fopen(pid_filename, "r");
    assert(fp);
    char pid_str[32];
    assert(fgets(pid_str, sizeof(pid_str), fp));
    instance->pid = atol(pid_str);
    free(pid_filename);
    fclose(fp);
  }
}

static PostgresStatus wait_for_postmaster(yinstance *instance, pid_t pm_pid) {
  int timeout_secs = 60;
  int wait_per_sec = 20;
  char *conninfo;

  for (int i = 0; i < timeout_secs * wait_per_sec; i++) {
    usleep(1000000L/wait_per_sec);

    // attempt to connect to the default postgres database to check if the server is ready.
    asprintf(&conninfo, "host=127.0.0.1 port=%d dbname=postgres user=yregress",
             instance->info.managed.port);

    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) == CONNECTION_OK) {
      free(conninfo);
      return POSTMASTER_READY;
    }

    // return immediately if postgres failed to start
    if (waitpid(pm_pid, NULL, WNOHANG) == pm_pid) {
      free(conninfo);
      fprintf(stderr, "postgres failed to start, check \"postgresql.log\"\n");
      return POSTMASTER_FAILED;
    }

    free(conninfo);
  }

  fprintf(stderr, "postgres did not respond within %d seconds, check \"postgresql.log\"\n",
          timeout_secs);
  kill(pm_pid, SIGKILL);
  return POSTMASTER_FAILED;
}

default_yinstance_result default_instance(struct fy_node *instances, yinstance **instance) {
  switch (fy_node_mapping_item_count(instances)) {
  case 0:
    // There are no instances available
    return default_instance_not_found;
  case 1:
    // There is only one available, that'll be default
    *instance = (yinstance *)fy_node_get_meta(
        fy_node_pair_value(fy_node_mapping_get_by_index(instances, 0)));
    return default_instance_found;

  default:
    // There are more than one instance to choose from
    {
      // Try to see if any is set as default
      void *iter = NULL;

      struct fy_node_pair *instance_pair;
      while ((instance_pair = fy_node_mapping_iterate(instances, &iter)) != NULL) {
        struct fy_node *instance_node = fy_node_pair_value(instance_pair);
        yinstance *y_instance = (yinstance *)fy_node_get_meta(instance_node);
        if (y_instance->is_default) {
          *instance = y_instance;
          return default_instance_found;
        }
      }
    }
    return default_instance_ambiguous;
  }
}

iovec_t yinstance_name(yinstance *instance) {
  if (instance->name.base != NULL) {
    return instance->name;
  } else {
    char *path = fy_node_get_path(instance->node);
    return (iovec_t){.base = path, .len = strlen(path)};
  }
}

static uint16_t get_available_inet_port() {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return 0;
  }

  struct sockaddr_in addr;
  bzero((char *)&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = 0;

  if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    return 0;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(sock, (struct sockaddr *)&addr, &len) < 0) {
    return 0;
  }

  uint16_t port = ntohs(addr.sin_port);

  if (close(sock) < 0) {
    return 0;
  }

  return port;
}

static bool fetch_types(yinstance *instance) {
  const char *query = "select oid, typname from pg_type";

  PGresult *result = PQexec(instance->conn, query);

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    fprintf(stderr, "Failed to fetch type name: %s", PQerrorMessage(instance->conn));
    PQclear(result);
    return false;
  }

  for (int row = 0; row < PQntuples(result); row++) {
    const char *oids = PQgetvalue(result, row, 0);
    Oid oid = (Oid)atoi(oids);
    const char *type_name = PQgetvalue(result, row, 1);
    if (strncmp(type_name, "json", strlen(type_name)) == 0) {
      instance->types.json = oid;
    } else if (strncmp(type_name, "jsonb", strlen(type_name)) == 0) {
      instance->types.jsonb = oid;
    } else if (strncmp(type_name, "bool", strlen(type_name)) == 0) {
      instance->types.boolean = oid;
    }
  }
  PQclear(result);
  return true;
}

yinstance_connect_result yinstance_connect(yinstance *instance) {
  bool success = true;
  int init_step = 0;
  int old_tap_counter = tap_counter;
connect:
  instance->restarted = false;
  if (instance->conn != NULL && PQstatus(instance->conn) != CONNECTION_BAD) {
    return yinstance_connect_success;
  }
  char *conninfo;
  if (instance->managed) {
    asprintf(&conninfo, "host=127.0.0.1 port=%d dbname=yregress user=yregress",
             instance->info.managed.port);
  } else {
    if (instance->info.unmanaged.password != NULL) {
      asprintf(&conninfo, "host=%s port=%d dbname=%s user=%s password=%s",
               instance->info.unmanaged.host, instance->info.unmanaged.port,
               instance->info.unmanaged.dbname, instance->info.unmanaged.username,
               instance->info.unmanaged.password);
    } else {
      asprintf(&conninfo, "host=%s port=%d dbname=%s user=%s", instance->info.unmanaged.host,
               instance->info.unmanaged.port, instance->info.unmanaged.dbname,
               instance->info.unmanaged.username);
    }
  }
  instance->conn = PQconnectdb(conninfo);

  // Prepare the database if connected
  if (PQstatus(instance->conn) == CONNECTION_OK) {
    // Initialize
    if (fy_node_is_mapping(instance->node)) {
      struct fy_node *init = fy_node_mapping_lookup_by_string(instance->node, STRLIT("init"));
      if (init != NULL) {
        assert(fy_node_is_sequence(init));
        int init_steps = fy_node_sequence_item_count(init);
        if (init_steps > 0) {
          // if we are following a restart, don't report the subtest again
          if (init_step == 0) {
            fprintf(tap_file, "# Subtest: initialize instance `%.*s`\n",
                    (int)IOVEC_STRLIT(yinstance_name(instance)));
            fprintf(tap_file, "    1..%d\n", init_steps);
            tap_counter = 0;
          }
          {
            int cur_step = 0;
            void *iter = NULL;
            struct fy_node *step;
            while ((step = fy_node_sequence_iterate(init, &iter)) != NULL) {
              if (cur_step >= init_step) {
                // We want to keep the effects of these steps and therefore we don't
                // wrap them into a rolled back transaction.
                success = ytest_run_without_transaction((ytest *)fy_node_get_meta(step), 1);
                if (!success) {
                  break;
                }
                if (instance->restarted) {
                  init_step = cur_step + 1;
                  goto connect;
                }
              }
              cur_step++;
            }
          }
          tap_counter = old_tap_counter;
        }
      }
      tap_counter++;
      if (success) {
        fprintf(tap_file, "ok %d - initialize instance `%.*s`\n", tap_counter,
                (int)IOVEC_STRLIT(yinstance_name(instance)));
      } else {
        fprintf(tap_file, "not ok %d - initialize instance `%.*s`\n", tap_counter,
                (int)IOVEC_STRLIT(yinstance_name(instance)));
        return yinstance_connect_error;
      }
    }

    return fetch_types(instance) ? yinstance_connect_success : yinstance_connect_error;
  }

  return yinstance_connect_failure;
}

void start_postgres(yinstance *instance, char *datadir) {
  pid_t start_command_pid = fork();
  if (start_command_pid < 0) {
    fprintf(stderr, "Failed to setup instance: %s\n", strerror(errno));
    return;
  } else if (start_command_pid == 0) {
    if (setpgid(0, getppid()) < 0) {
      fprintf(stderr, "Failed to add child to process group: %s\n", strerror(errno));
      _exit(1);
    }

    // use a shell instead of passing in running postgres directly to allow us redirect output.
    char *pg_cmd;
    asprintf(&pg_cmd, "exec \"%s/postgres\" -p %d -D %s < \"%s\" >> \"%s\" 2>&1",
                   bindir, instance->info.managed.port, datadir, "/dev/null", "postgresql.log");

    (void) execl("/bin/sh", "/bin/sh", "-c", pg_cmd, (char *) NULL);
    // if exec fails
    fprintf(stderr, "Failed to start instance: %s\n", strerror(errno));
    exit(1);
  } else {
    if (wait_for_postmaster(instance, start_command_pid) != POSTMASTER_READY) {
      fprintf(stderr, "Time-out while waiting for instance\n");
      instances_cleanup();
      return;
    }
  }
}

void yinstance_start(yinstance *instance) {
  if (instance->managed) {

    char datadir[] = "pg_yregress_XXXXXX";
    mkdtemp(datadir);

    // Initialize the cluster
    char *initdb_command;
    asprintf(&initdb_command,
             "%s/pg_ctl initdb -o '-A trust -U yregress --no-clean --no-sync --encoding=%.*s "
             "--locale=%.*s' -s -D %s",
             bindir, (int)IOVEC_STRLIT(instance->info.managed.encoding),
             (int)IOVEC_STRLIT(instance->info.managed.locale), datadir);
    system(initdb_command);

    // Add configuration
    struct fy_node *config = fy_node_mapping_lookup_by_string(instance->node, STRLIT("config"));
    if (config != NULL) {
      char *config_file;
      asprintf(&config_file, "%s/postgresql.auto.conf", datadir);
      if (fy_node_is_scalar(config)) {
        FILE *cfg = fopen(config_file, "w");
        fprintf(cfg, "%s\n", fy_node_get_scalar(config, NULL));
        fclose(cfg);
      } else if (fy_node_is_mapping(config)) {
        FILE *cfg = fopen(config_file, "w");
        {
          void *iter = NULL;
          struct fy_node_pair *cfg_pair;
          while ((cfg_pair = fy_node_mapping_iterate(config, &iter)) != NULL) {
            struct fy_node *key = fy_node_pair_key(cfg_pair);
            struct fy_node *value = fy_node_pair_value(cfg_pair);
            size_t keylen, valuelen;
            if (fy_node_is_scalar(key) && fy_node_is_scalar(value)) {
              const char *keystring = fy_node_get_scalar(key, &keylen);
              const char *valuestring = fy_node_get_scalar(value, &valuelen);
              fprintf(cfg, "%.*s = '%.*s'\n", (int)keylen, keystring, (int)valuelen, valuestring);
            }
          }
        }
        fclose(cfg);
      }
    }

    // Add pg_hba
    struct fy_node *hba = fy_node_mapping_lookup_by_string(instance->node, STRLIT("hba"));
    if (hba != NULL) {
      char *hba_file;
      asprintf(&hba_file, "%s/pg_hba.conf", datadir);
      if (fy_node_is_scalar(hba)) {
        FILE *hbaf = fopen(hba_file, "w");
        fprintf(hbaf, "%s", fy_node_get_scalar(hba, NULL));
        printf("%s", fy_node_get_scalar(hba, NULL));
        fclose(hbaf);
      }
    }

    // Start the database
    instance->restarted = false;
    instance->info.managed.port = get_available_inet_port();

    // Important to capture datadir before we try to start as init steps may restart the instance
    // and it'll need the path
    char *heap_datadir = strdup(datadir);
    instance->info.managed.datadir = (iovec_t){.base = heap_datadir, .len = strlen(heap_datadir)};

    start_postgres(instance, datadir);

    // Create the database
    char *createdb_command;
    asprintf(&createdb_command, "%s/createdb -U yregress -O yregress -p %d yregress", bindir,
             instance->info.managed.port);
    system(createdb_command);
  }

  // Wait until it is ready
  bool ready = false;
  while (!ready) {
    switch (yinstance_connect(instance)) {
    case yinstance_connect_success:
      ready = true;
      break;
    case yinstance_connect_failure:
      if (PQstatus(instance->conn) == CONNECTION_BAD) {
        fprintf(stderr, "can't connect: %s\n", PQerrorMessage(instance->conn));
        ready = true;
        break;
      }
      break;
    case yinstance_connect_error: {
      return;
    }
    }
  }

  // Get postmaster PID
  retrieve_postmaster_pid(instance);

  instance->ready = true;
}

static int remove_entry(const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  return remove(path);
}

void instances_cleanup() {
  if (instances != NULL) {
    void *iter = NULL;
    struct fy_node_pair *instance_pair;

    while ((instance_pair = fy_node_mapping_iterate(instances, &iter)) != NULL) {
      struct fy_node *instance = fy_node_pair_value(instance_pair);
      yinstance *y_instance = (yinstance *)fy_node_get_meta(instance);

      if (y_instance != NULL && y_instance->managed) {
        if (y_instance->ready) {
          char *stop_command;
          asprintf(&stop_command, "%s/pg_ctl stop -D %.*s -m immediate -s", bindir,
                   (int)IOVEC_STRLIT(y_instance->info.managed.datadir));
          system(stop_command);
          y_instance->ready = false;
        }

        // Cleanup the directory
        nftw(strndup(IOVEC_RSTRLIT(y_instance->info.managed.datadir)), remove_entry, FOPEN_MAX,
             FTW_DEPTH | FTW_PHYS);
      }
    }
  }
}

void register_instances_cleanup() { atexit(instances_cleanup); }

void restart_instance(yinstance *instance) {
  if (instance->managed) {
    char *restart_command;
    asprintf(&restart_command,
             "%s/pg_ctl stop "
             "-D %.*s -s >/dev/null",
             bindir, (int)IOVEC_STRLIT(instance->info.managed.datadir));
    system(restart_command);

    start_postgres(instance, (char *)instance->info.managed.datadir.base);
    // Capture new PID
    retrieve_postmaster_pid(instance);
  }
  instance->restarted = true;
}