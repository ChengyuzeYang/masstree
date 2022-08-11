#include "config.h"
// DO NOT REORDER (config.h needs to be included before other headers)
#include "compiler.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_stats.hh"
#include "masstree_tcursor.hh"
#include "string.hh"

using KeyType = uint64_t;
using ValueType = uint64_t;

// scan result struct
struct index_scan {
   KeyType *keys;
   ValueType *values;
   size_t num;
};

static nodeversion32 global_epoch_lock(false);
volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;
kvepoch_t global_log_epoch = 0;

volatile bool recovering = false; // so don't add log entries, and free old value immediately
kvtimestamp_t initial_timestamp;

class key_unparse_unsigned {
public:
  static int unparse_key(Masstree::key<uint64_t> key, char *buf, int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

// table parameters
struct table_params : public Masstree::nodeparams<15, 15> {
    using value_type = ValueType;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = threadinfo;
    using key_unparse_type = key_unparse_unsigned;
};

using Str = Masstree::Str;
using table_type = Masstree::basic_table<table_params>;
using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
using cursor_type = Masstree::tcursor<table_params>;
using leaf_type = Masstree::leaf<table_params>;
using internode_type = Masstree::internode<table_params>;
using node_type = typename table_type::node_type;

template <typename V>
struct Masstree_client{
public:
    void set_table(table_type* table, threadinfo* ti){
        table_ = table;
        ti_ = ti;
    }

    bool __put(const char* key, std::size_t len_key, V value){
        cursor_type lp(*table_, key, len_key);

        bool found = lp.find_insert(*ti_);
        if (found) {
            lp.finish(0, *ti_); // release lock
            return 0;          // not inserted
        }

        lp.value() = value;
        fence();
        lp.finish(1, *ti_); // finish insert
        return 1;          // inserted
    }

    bool put(KeyType key, V value){
        KeyType key_buf{__builtin_bswap64(key)};
        return __put(reinterpret_cast<char *>(&key_buf), sizeof(key_buf), value);
    }

    bool __get(const char *key, std::size_t len_key, V *ret_value) {
        unlocked_cursor_type lp(*table_, key, len_key);
        bool found = lp.find_unlocked(*ti_);
        if (found){
            *ret_value = lp.value();
            return 1;
        }else{
            ret_value = nullptr;
            return 0;
        }
    }

    bool get(KeyType key, V *value_p){
        KeyType key_buf{__builtin_bswap64(key)};
        return __get(reinterpret_cast<char *>(&key_buf), sizeof(key_buf), value_p);
    }

    bool __update(const char* key, std::size_t len_key, V value){
        cursor_type lp(*table_, key, len_key);

        bool found = lp.find_locked(*ti_);
        if (found) {
            lp.value() = value;
            fence();
            lp.finish(0, *ti_); // release lock
            return 1;          // updated
        }
        lp.finish(0, *ti_); // finish insert
        return 0;          // not updated
    }

    bool update(KeyType key, V value){
        KeyType key_buf{__builtin_bswap64(key)};
        return __update(reinterpret_cast<char *>(&key_buf), sizeof(key_buf), value);
    }

    bool __remove(const char *key, std::size_t len_key) {
        cursor_type lp(*table_, key, len_key);
        bool found = lp.find_locked(*ti_); // lock a node which potentailly contains the value

        if (found) {
            lp.finish(-1, *ti_); // finish remove
            return 1;           // removed
        }

        lp.finish(0, *ti_); // release lock
        return 0;          // not removed
    }

    bool remove(KeyType key){
        KeyType key_buf{__builtin_bswap64(key)};
        return __remove(reinterpret_cast<char *>(&key_buf), sizeof(key_buf));
    }

    class Callback {
    public:
        std::function<void(const leaf_type *, uint64_t, bool &)> per_node_func;
        std::function<void(const Str &, const V &, bool &)> per_kv_func;
    };
    class SearchRangeScanner {
    public:
        SearchRangeScanner(const char *const rkey, const std::size_t len_rkey,
                       const bool r_exclusive, Callback &callback,
                       int64_t max_scan_num = -1)
        : rkey_(rkey), len_rkey_(len_rkey), r_exclusive_(r_exclusive),
          callback_(callback), max_scan_num_(max_scan_num) {}

    template <typename ScanStackElt, typename Key>
    void visit_leaf(const ScanStackElt &iter, const Key &key, threadinfo &) {
        (void)key;
        callback_.per_node_func(iter.node(), iter.full_version_value(),
                              continue_flag);
    }

    bool visit_value(const Str key, V val, threadinfo &) {
        if (!continue_flag || (max_scan_num_ >= 0 && scan_num_cnt_ >= max_scan_num_)) {
            return false;
        }
        ++scan_num_cnt_;

        bool endless_key = (rkey_ == nullptr);

        if (endless_key) {
            callback_.per_kv_func(key, val, continue_flag);
            return true;
        }

        // compare key with end key
        const int res = memcmp(
            rkey_, key.s, std::min(len_rkey_, static_cast<std::size_t>(key.len)));

        bool smaller_than_end_key = (res > 0);
        bool same_as_end_key_but_shorter =
            ((res == 0) && (len_rkey_ > static_cast<std::size_t>(key.len)));
        bool same_as_end_key_inclusive =
            ((res == 0) && (len_rkey_ == static_cast<std::size_t>(key.len)) &&
                (!r_exclusive_));

        if (smaller_than_end_key || same_as_end_key_but_shorter ||
          same_as_end_key_inclusive) {
            callback_.per_kv_func(key, val, continue_flag);
            return true;
        }

        return false;
    }

    private:
        const char *const rkey_{};
        const std::size_t len_rkey_{};
        const bool r_exclusive_{};
        std::vector<V> scan_buffer_{};
        Callback &callback_;
        const bool limited_scan_{false};
        int64_t scan_num_cnt_ = 0;
        int64_t max_scan_num_ = -1;

        bool continue_flag = true;
    };

    void __scan(const char *const lkey, const std::size_t len_lkey,
            const bool l_exclusive, const char *const rkey,
            const std::size_t len_rkey, const bool r_exclusive,
            Callback &&callback, int64_t max_scan_num = -1) {

        Str mtkey = (lkey == nullptr ? Str() : Str(lkey, len_lkey));

        SearchRangeScanner scanner(rkey, len_rkey, r_exclusive, callback,
                               max_scan_num);
        table_->scan(mtkey, !l_exclusive, scanner, *ti_);
    }

    void scan(KeyType l_key, KeyType r_key, int64_t count, index_scan *res) {
        KeyType l_key_buf{__builtin_bswap64(l_key)};
        KeyType r_key_buf{__builtin_bswap64(r_key)};

        bool l_exclusive = false;
        bool r_exclusive = false;

        uint64_t n_cnt = 0;
        uint64_t v_cnt = 0;

    __scan(
        reinterpret_cast<char *>(&l_key_buf), sizeof(l_key_buf), l_exclusive,
        reinterpret_cast<char *>(&r_key_buf), sizeof(r_key_buf), r_exclusive,
        {[&n_cnt](const leaf_type *leaf, uint64_t version,
            bool &continue_flag) {
                n_cnt++;
                (void)leaf;
                (void)version;
                (void)continue_flag;
                return;
            },
        [&v_cnt,res](const Str &key, const V &val, bool &continue_flag) {
            KeyType actual_key{
                __builtin_bswap64(*(reinterpret_cast<const uint64_t *>(key.s)))};
            printf("scanned key: %lu\n", actual_key);
            res->keys[res->num] = actual_key;
            res->values[res->num] = val;
            printf("%lu\n", val);
            res->num++;
            (void)val;
            (void)continue_flag;
            v_cnt++;
            return;
        }},
        count);

        //printf("n_cnt: %ld\n", n_cnt);
        //printf("v_cnt: %ld\n", v_cnt);
    }

    table_type* table_;
    threadinfo* ti_;
};

enum {
    initialize = 1,
    destroy = 2
};

template <typename V>
struct Masstree_manager{
    Masstree_manager(threadinfo *ti){
        client_.set_table(table_,ti);
        client_.ti_->rcu_start();
    }
    ~Masstree_manager(){
        client_.ti_->rcu_stop();
    }
    static void setup(threadinfo* ti, int action) {
        if (action == initialize) {
            table_ = new table_type;
            table_->initialize(*ti);
        } else if (action == destroy) {
            delete table_;
            table_ = 0;
        }
    }
    
    static table_type* table_;
    Masstree_client<V> client_;
};

template <typename V> table_type* Masstree_manager<V>::table_;

typedef Masstree_manager<ValueType> masstree_manager_test;