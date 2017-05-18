// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fstream>
#include <yaml-cpp/yaml.h>
#include <sys/inotify.h>
#include <unistd.h>
#include <signal.h>

#include "common/debug.h"
#include "common/Timer.h"
#include "common/Thread.h"
#include "include/atomic.h"
#include "global/signal_handler.h"
#include "rgw_op.h"

#include "rgw_rate_limit.h"

#define dout_subsys ceph_subsys_rgw

#define RATE_LIMIT_CONFIG_FILE "/etc/ceph/rgw-rate-limit.yml"

enum Period { UNDEF=0, SECOND=1, MINUTE=2, HOUR=3 };


// Globals

typedef struct {
  atomic64_t recvd;
  atomic64_t limit;
  Period period;
  int accept_idx;
  int reject_idx;
} api_counter_t;

typedef unordered_map<string, api_counter_t *> RGW_api_ctr_t;
typedef RGW_api_ctr_t::iterator api_ctr_it;
typedef RGW_api_ctr_t::const_iterator api_ctr_cit;

#define DEFAULT_LIMIT_USER "default.user"
api_counter_t default_obj_get, default_obj_put, default_obj_del;
api_counter_t default_bucket_create, default_bucket_delete;
Mutex *default_limit_lock;

// Maps of user counters
RGW_api_ctr_t *obj_get_ctr_map, *obj_put_ctr_map, *obj_del_ctr_map;
RGW_api_ctr_t *bucket_create_ctrs, *bucket_delete_ctrs;


// Helpers

static Period parse_period_str(const string period_str)
{
  Period retval = UNDEF;
  if ( period_str == "sec" ) {
    retval = SECOND;
  } else if ( period_str == "min" ) {
    retval = MINUTE;
  } else if ( period_str == "hour" ) {
    retval = HOUR;
  }
  return retval;
}

static string get_period_str(const Period period)
{
  string retval = "UNDEF";
  if ( period == SECOND ) {
    retval = "sec";
  } else if ( period == MINUTE ) {
    retval = "min";
  } else if ( period == HOUR ) {
    retval = "hour";
  }
  return retval;
}

static void init_api_limit(api_counter_t *ctr,
			   const unsigned long long limit,
			   const Period period)
{
  ctr->recvd.set(0);
  ctr->limit.set(limit);
  ctr->period = period;
  ctr->accept_idx = -1;
  ctr->reject_idx = -1;
}

static void dump_limit_config(RGW_api_ctr_t *ctrs)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(0) << "User: " << user \
	    << "\tlimit: " << ctr->limit.read() << "/" \
	    << get_period_str(ctr->period) << dendl;
  }
}

static void dump_ctr_stats(RGW_api_ctr_t *ctrs)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    dout(0) << "User: " << user << "\tlimit: " << ctr->limit.read() \
	    << "/" << get_period_str(ctr->period) \
	    << ", recvd: " << ctr->recvd.read() << dendl;
  }
}

static void rgw_api_ctr_dump(int signum)
{
  dout(0) << "*** Object GET stats dump" << dendl;
  dump_ctr_stats(obj_get_ctr_map);
  dout(0) << "*** Object PUT stats dump" << dendl;
  dump_ctr_stats(obj_put_ctr_map);
  dout(0) << "*** Object DELETE stats dump" << dendl;
  dump_ctr_stats(obj_del_ctr_map);
  dout(0) << "*** Bucket CREATE stats dump" << dendl;
  dump_ctr_stats(bucket_create_ctrs);
  dout(0) << "*** Bucket DELETE stats dump" << dendl;
  dump_ctr_stats(bucket_delete_ctrs);
}

static void update_one_default_limit(api_counter_t *default_ctr, RGW_api_ctr_t *ctrs)
{
  api_ctr_cit iter;
  if ( (iter = ctrs->find(DEFAULT_LIMIT_USER)) != ctrs->end() ) {
    api_counter_t *ctr = iter->second;
    default_ctr->limit.set(ctr->limit.read());
    default_ctr->period = ctr->period;
  }
}

static void update_default_limits()
{
  update_one_default_limit(&default_obj_get, obj_get_ctr_map);
  update_one_default_limit(&default_obj_put, obj_put_ctr_map);
  update_one_default_limit(&default_obj_del, obj_del_ctr_map);
  update_one_default_limit(&default_bucket_create, bucket_create_ctrs);
  update_one_default_limit(&default_bucket_delete, bucket_delete_ctrs);
}

static api_counter_t *parse_limit(const YAML::Node *limit_data)
{
  api_counter_t *api_ctr = new api_counter_t();
  try {
    if ( !limit_data || !(limit_data->size()) ) {
      delete api_ctr;
      return NULL;
    }

    unsigned long long limit;
    (*limit_data)["limit"] >> limit;
    api_ctr->limit.set(limit);
    string period_str;
    (*limit_data)["period"] >> period_str;
    if ( (api_ctr->period = parse_period_str(period_str)) == UNDEF ) {
      delete api_ctr;
      return NULL;
    }

  } catch (...) {
    delete api_ctr;
    return NULL;
  }
  return api_ctr;
}

// Parse config file into tmp map & swap with current map
static int load_limit_config_file()
{
  YAML::Node doc;
  try {
    ifstream config_file(RATE_LIMIT_CONFIG_FILE);
    YAML::Parser parser(config_file);
    parser.GetNextDocument(doc);
  } catch (...) {
    dout(0) << "Error parsing rate limit config" << dendl;
    return -1;
  }

  dout(0) << "*** Rate limit config parsing ***" << dendl;

  for ( unsigned int i=0; i < doc.size(); i++ ) {
    const YAML::Node& node = doc[i];
    if ( !node.size() ) {
      continue;
    }

    string user;
    try {
      node["user"] >> user;
    } catch (...) {
      dout(0) << "Key 'user' not found, skipping stanza" << dendl;
      continue;
    }

    api_counter_t *obj_get_ctr, *obj_put_ctr, *obj_del_ctr;
    api_counter_t *create_ctr, *del_ctr;
    obj_get_ctr = obj_put_ctr = obj_del_ctr = NULL;
    create_ctr = del_ctr = NULL;
    try {
      const YAML::Node *object = node.FindValue("object");
      const YAML::Node *bucket = node.FindValue("bucket");

      if ( object && object->size() ) {
	const YAML::Node *get = object->FindValue("get");
	obj_get_ctr = parse_limit(get);
	const YAML::Node *put = object->FindValue("put");
	obj_put_ctr = parse_limit(put);
	const YAML::Node *delete_obj = object->FindValue("delete");
	obj_del_ctr = parse_limit(delete_obj);
      }

      if ( bucket && bucket->size() ) {
	const YAML::Node *create = bucket->FindValue("create");
	create_ctr = parse_limit(create);
	const YAML::Node *delete_bucket = bucket->FindValue("delete");
	del_ctr = parse_limit(delete_bucket);
      }
    } catch (...) {
      dout(0) << "Error parsing limits, using defaults, user: " << user << dendl;
      continue;
    }

    if ( obj_get_ctr ) (*obj_get_ctr_map)[user] = obj_get_ctr;
    if ( obj_put_ctr ) (*obj_put_ctr_map)[user] = obj_put_ctr;
    if ( obj_del_ctr ) (*obj_del_ctr_map)[user] = obj_del_ctr;
    if ( create_ctr )  (*bucket_create_ctrs)[user] = create_ctr;
    if ( del_ctr )     (*bucket_delete_ctrs)[user] = del_ctr;
  }

  dout(0) << "*** Final object GET rate limit config ***" << dendl;
  dump_limit_config(obj_get_ctr_map);
  dout(0) << "*** Final object PUT rate limit config ***" << dendl;
  dump_limit_config(obj_put_ctr_map);
  dout(0) << "*** Final object DELETE rate limit config ***" << dendl;
  dump_limit_config(obj_del_ctr_map);
  dout(0) << "*** Final bucket CREATE rate limit config ***" << dendl;
  dump_limit_config(bucket_create_ctrs);
  dout(0) << "*** Final bucket DELETE rate limit config ***" << dendl;
  dump_limit_config(bucket_delete_ctrs);

  update_default_limits();

  return 0;
}


// Timer setup, callbacks, counter reset

#define ONE_SEC 1
#define ONE_MIN 60
#define ONE_HOUR (60 * 60)

Mutex *sec_timer_lock, *min_timer_lock, *hour_timer_lock;
SafeTimer *rate_ctr_timer_sec, *rate_ctr_timer_min, *rate_ctr_timer_hour;

static void reset_ctrs(RGW_api_ctr_t *ctrs, Period period)
{
  for (api_ctr_cit iter = ctrs->begin(); iter != ctrs->end(); ++iter) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    if ( ctr->period == period ) {
      ctr->recvd.set(0);
    }
  }
}

class C_RateCtrSecTimeout : public Context {
public:
  C_RateCtrSecTimeout() {}
  void finish(int r) {
    reset_ctrs(obj_get_ctr_map, SECOND);
    reset_ctrs(obj_put_ctr_map, SECOND);
    reset_ctrs(obj_del_ctr_map, SECOND);
    reset_ctrs(bucket_create_ctrs, SECOND);
    reset_ctrs(bucket_delete_ctrs, SECOND);
    rate_ctr_timer_sec->add_event_after(ONE_SEC, new C_RateCtrSecTimeout);
  }
};

class C_RateCtrMinTimeout : public Context {
public:
  C_RateCtrMinTimeout() {}
  void finish(int r) {
    reset_ctrs(obj_get_ctr_map, MINUTE);
    reset_ctrs(obj_put_ctr_map, MINUTE);
    reset_ctrs(obj_del_ctr_map, MINUTE);
    reset_ctrs(bucket_create_ctrs, MINUTE);
    reset_ctrs(bucket_delete_ctrs, MINUTE);
    rate_ctr_timer_min->add_event_after(ONE_MIN, new C_RateCtrMinTimeout);
  }
};

class C_RateCtrHourTimeout : public Context {
public:
  C_RateCtrHourTimeout() {}
  void finish(int r) {
    reset_ctrs(obj_get_ctr_map, HOUR);
    reset_ctrs(obj_put_ctr_map, HOUR);
    reset_ctrs(obj_del_ctr_map, HOUR);
    reset_ctrs(bucket_create_ctrs, HOUR);
    reset_ctrs(bucket_delete_ctrs, HOUR);
    rate_ctr_timer_hour->add_event_after(ONE_HOUR, new C_RateCtrHourTimeout);
  }
};

static void setup_timers()
{
  sec_timer_lock = new Mutex("timer_sec");
  rate_ctr_timer_sec = new SafeTimer(g_ceph_context, *sec_timer_lock);
  rate_ctr_timer_sec->init();
  sec_timer_lock->Lock();
  rate_ctr_timer_sec->add_event_after(ONE_SEC, new C_RateCtrSecTimeout);
  sec_timer_lock->Unlock();

  min_timer_lock = new Mutex("timer_min");
  rate_ctr_timer_min = new SafeTimer(g_ceph_context, *min_timer_lock);
  rate_ctr_timer_min->init();
  min_timer_lock->Lock();
  rate_ctr_timer_min->add_event_after(ONE_MIN, new C_RateCtrMinTimeout);
  min_timer_lock->Unlock();

  hour_timer_lock = new Mutex("timer_hour");
  rate_ctr_timer_hour = new SafeTimer(g_ceph_context, *hour_timer_lock);
  rate_ctr_timer_hour->init();
  hour_timer_lock->Lock();
  rate_ctr_timer_hour->add_event_after(ONE_HOUR, new C_RateCtrHourTimeout);
  hour_timer_lock->Unlock();
}


// API calls 'perf' counters
// Per-account counters for accept + reject GET / PUT call
// Counter names: rgw_api_{accept,reject}_{obj,bucket}_{get,put,del}_<user>
#define RGW_API_CTR_IDX   (17000)
#define RGW_API_ACCEPT_CTR_NAME_PREFIX string("rgw_api_accept_")
#define RGW_API_REJECT_CTR_NAME_PREFIX string("rgw_api_reject_")

PerfCountersBuilder *rgw_api_ctr_pcb;
PerfCounters *rgw_api_ctrs;

static void add_api_ctr(int ctr_idx, string ctr_name)
{
  char *ctr_name_str = new char[ctr_name.length() + 1];
  strcpy(ctr_name_str, ctr_name.c_str());
  rgw_api_ctr_pcb->add_u64_counter(ctr_idx, ctr_name_str);
}

static void add_api_accept_ctr(int ctr_idx, string ctr_name)
{
  ctr_name = RGW_API_ACCEPT_CTR_NAME_PREFIX + ctr_name;
  add_api_ctr(ctr_idx, ctr_name);
}

static void add_api_reject_ctr(int ctr_idx, string ctr_name)
{
  ctr_name = RGW_API_REJECT_CTR_NAME_PREFIX + ctr_name;
  add_api_ctr(ctr_idx, ctr_name);
}

static int init_ctr_set(RGW_api_ctr_t *ctr_map, const string ctr_prefix, int ctr_idx)
{
  for ( api_ctr_it iter = ctr_map->begin();
	iter != ctr_map->end();
	++iter ) {
    string user = iter->first;
    api_counter_t *ctr = iter->second;
    add_api_accept_ctr(ctr_idx, ctr_prefix + user);
    ctr->accept_idx = ctr_idx;
    ctr_idx++;
    add_api_reject_ctr(ctr_idx, ctr_prefix + user);
    ctr->reject_idx = ctr_idx;
    ctr_idx++;
  }
  return (ctr_idx);
}

static void rgw_init_api_ctrs(CephContext *cct)
{
  int num_rgw_api_ctrs = (obj_get_ctr_map->size() + obj_put_ctr_map->size() + obj_del_ctr_map->size() + \
			  bucket_create_ctrs->size() + bucket_delete_ctrs->size()) * 2;

  // PerfCountersBuilder does a ()-style bounds check, hence the +1's
  rgw_api_ctr_pcb = new PerfCountersBuilder(cct, "rgw_api_counters",
	RGW_API_CTR_IDX, RGW_API_CTR_IDX + num_rgw_api_ctrs + 1);
  int next_ctr_idx = RGW_API_CTR_IDX + 1;

  next_ctr_idx = init_ctr_set(obj_get_ctr_map, string("obj_get_"), next_ctr_idx);
  next_ctr_idx = init_ctr_set(obj_put_ctr_map, string("obj_put_"), next_ctr_idx);
  next_ctr_idx = init_ctr_set(obj_del_ctr_map, string("obj_del_"), next_ctr_idx);
  next_ctr_idx = init_ctr_set(bucket_create_ctrs, string("bucket_create_"), next_ctr_idx);
  next_ctr_idx = init_ctr_set(bucket_delete_ctrs, string("bucket_delete_"), next_ctr_idx);

  rgw_api_ctrs = rgw_api_ctr_pcb->create_perf_counters();
  cct->get_perfcounters_collection()->add(rgw_api_ctrs);
}

static api_counter_t *add_default_rate_limit(RGW_api_ctr_t *ctr_map,
					     api_counter_t *default_ctr,
					     string user)
{
  api_counter_t *ctr = new api_counter_t();
  init_api_limit(ctr, default_ctr->limit.read(), default_ctr->period);

  default_limit_lock->Lock();
  api_ctr_cit iter;
  if ( (iter = ctr_map->find(user)) != ctr_map->end() ) {
    // ... in case someone else added it to the map
    delete ctr;
    ctr = iter->second;
  } else {
    dout(0) << user << " : adding default limit = " << ctr->limit.read() << dendl;
    (*ctr_map)[user] = ctr;
  }
  default_limit_lock->Unlock();
  return ctr;
}

// Actual rate limit / api count check

static bool rgw_op_rate_limit_ok(RGW_api_ctr_t *ctr_map,
				 api_counter_t *default_ctr,
				 string user)
{
  if ( !ctr_map ) {
    return true;
  }

  api_ctr_cit iter;
  api_counter_t *ctr;
  if ( (iter = ctr_map->find(user)) != ctr_map->end() ) {
    ctr = iter->second;
  } else {
    ctr = add_default_rate_limit(ctr_map, default_ctr, user);
  }

  ctr->recvd.inc();
  dout(20) << user << " : recvd = " << ctr->recvd.read() \
	   << ", limit = " << ctr->limit.read() << dendl;
  if ( ctr->recvd.read() > ctr->limit.read() ) {
    if ( ctr->reject_idx != -1 ) rgw_api_ctrs->inc(ctr->reject_idx);
    return false;
  }
  if ( ctr->reject_idx != -1 ) rgw_api_ctrs->inc(ctr->accept_idx);
  return true;
}

bool rgw_rate_limit_ok(string& user, RGWOp *op)
{
  RGW_api_ctr_t *ctr_map = NULL;
  api_counter_t *default_ctr = NULL;

  RGWOpType op_type = op->get_type();
  if ( op_type == RGW_OP_GET_OBJ ) {
    ctr_map = obj_get_ctr_map;
    default_ctr = &default_obj_get;
  }
  if ( op_type == RGW_OP_PUT_OBJ ) {
    ctr_map = obj_put_ctr_map;
    default_ctr = &default_obj_put;
  }
  if ( op_type == RGW_OP_DELETE_OBJ ) {
    ctr_map = obj_del_ctr_map;
    default_ctr = &default_obj_del;
  }
  if ( op_type == RGW_OP_DELETE_MULTI_OBJ ) {
    // XXX: exhaust all current delete tokens ?
    ctr_map = obj_del_ctr_map;
    default_ctr = &default_obj_del;
  }
  if ( op_type == RGW_OP_CREATE_BUCKET ) {
    ctr_map = bucket_create_ctrs;
    default_ctr = &default_bucket_create;
  }
  if ( op_type == RGW_OP_DELETE_BUCKET ) {
    ctr_map = bucket_delete_ctrs;
    default_ctr = &default_bucket_delete;
  }

  return (ctr_map ? rgw_op_rate_limit_ok(ctr_map, default_ctr, user) : true);
}


// file watcher

static void watch_config_file()
{
  dout(0) << "file watcher thread" << dendl;

  int ifd;
  if ( (ifd = inotify_init()) == -1 ) {
    dout(0) << "inotify_init fail" << dendl;
    return;
  }
  int wfd;
  uint32_t mask = IN_DELETE_SELF | IN_MODIFY;
  if ( (wfd = inotify_add_watch(ifd, RATE_LIMIT_CONFIG_FILE, mask)) == -1 ) {
    dout(0) << "inotify add watch fail" << dendl;
    return;
  }

  while ( true ) {
    struct inotify_event event;
    if ( read(ifd, (void *)(&event), sizeof(event)) == -1 ) {
      if ( errno == EINTR ) {
	dout(0) << "inotify read intr" << dendl;
	break;
      }
    }
    // TODO: assert event.wd == wfd

    if ( event.mask & IN_MODIFY ) {
      dout(0) << "inotify: config file modified" << dendl;
      // load_limit_config_file();
    }

    if ( event.mask & IN_IGNORED ) {
      dout(0) << "inotify watch rm'ed" << dendl;
      break;
    }
  }

  inotify_rm_watch(ifd, wfd);
  close(ifd);
}

class FileWatchThread : public Thread {
public:
  FileWatchThread() {}
  void *entry() {
    watch_config_file();
    return NULL;
  }
};


// Setup maps, timers, file-watch

static void setup_default_limits()
{
  init_api_limit(&default_obj_get, 1, MINUTE);
  init_api_limit(&default_obj_put, 1, MINUTE);
  init_api_limit(&default_obj_del, 1, HOUR);
  init_api_limit(&default_bucket_create, 1, HOUR);
  init_api_limit(&default_bucket_delete, 1, HOUR);
}

int rgw_rate_limit_init(CephContext *cct)
{
  default_limit_lock = new Mutex("default_limit_lock");
  setup_default_limits();

  obj_put_ctr_map = new RGW_api_ctr_t;
  obj_get_ctr_map = new RGW_api_ctr_t;
  obj_del_ctr_map = new RGW_api_ctr_t;
  bucket_create_ctrs = new RGW_api_ctr_t;
  bucket_delete_ctrs = new RGW_api_ctr_t;

  if ( load_limit_config_file() < 0 ) {
    return -1;
  }

  rgw_init_api_ctrs(cct);

  FileWatchThread *fwthd = new FileWatchThread();
  fwthd->create("Rate-Limiter-File-watcher");

  setup_timers();

  register_async_signal_handler(SIGUSR2, rgw_api_ctr_dump);

  return 0;
}
