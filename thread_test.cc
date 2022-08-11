#include "wrapper.hh"
#include <stdlib.h>
#include <time.h>

#define MAX_RAND 100

void* go(void* x) {
    threadinfo* ti = reinterpret_cast<threadinfo*>(x);
    ti->pthread() = pthread_self();
    masstree_manager_test mtt(ti);

    uint64_t rand_search_key = rand() % MAX_RAND;
    uint64_t value;
    mtt.client_.put(101,rand_search_key);
    mtt.client_.get(rand_search_key,&value);
    printf("ti_id = %lu,find key = %lu, value = %lu\n",ti->pthread(),rand_search_key,value);
    mtt.client_.get(101,&value);
    printf("ti_id = %lu,puts key = %lu, value = %lu\n",ti->pthread(),101,value);

    int scan_n = 5;
    struct index_scan res;
    res.keys = (KeyType*) malloc(scan_n*sizeof(*res.keys));
    res.values = (ValueType*) malloc(scan_n*sizeof(*res.values));
    res.num = 0;
    mtt.client_.scan(rand_search_key,MAX_RAND,scan_n,&res);
    for(int i = 0; i < scan_n; i++){
        printf("ti_id = %lu,scan key = %lu, value = %lu\n",ti->pthread(),res.keys[i],res.values[i]);
    }
    return 0;
}

void runtest(int nthreads, void* (*func)(void*)) {
    std::vector<threadinfo*> tis;
    for (int i = 0; i < nthreads; ++i)
        tis.push_back(threadinfo::make(threadinfo::TI_PROCESS, i));
    printf("tis finished!\n");
    for (int i = 0; i < nthreads; ++i) {
        srand((unsigned)time(0));
        int r = pthread_create(&tis[i]->pthread(), 0, func, tis[i]);
    }
    for (int i = 0; i < nthreads; ++i)
        pthread_join(tis[i]->pthread(), 0);
}

int main(){
    threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    main_ti->pthread() = pthread_self();
    masstree_manager_test::setup(main_ti,initialize);

    masstree_manager_test main_mtt(main_ti);
    for(uint64_t i = 0; i < MAX_RAND; i++){
        main_mtt.client_.put(i,MAX_RAND - i);
    }
    int nthreads = 10;
    runtest(nthreads,go);

    return 0;
}