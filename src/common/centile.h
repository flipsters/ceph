/*
 * centile.h
 *
 *  Created on: 25-Aug-2016
 *      Author: prashant.kr
 */
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <map>
#include <sstream>
#include <errno.h>
#include <string.h>
#include <string>
#include <include/atomic.h>
#include "Mutex.h"
#include "Clock.h"
using namespace std;

#ifndef CENTILE_H_
#define CENTILE_H_

namespace centile {
  class Centile {
    public:
      Centile(unsigned int start, unsigned int end, unsigned int inc);
      void insert(unsigned int);
      atomic64_t& get_size();
      unsigned int query(double);
      ~Centile();

    private:
      atomic64_t *buckets;
      atomic64_t size;
      unsigned int start, end, inc, firstBucket, lastBucket, numBuckets;
  };

  class CentileCollection {
    public:
      CentileCollection(unsigned int, unsigned int, unsigned int , vector<unsigned int>);
      void insert(unsigned int, unsigned int);
      unsigned int query(CephContext *, unsigned int, double);
      ~CentileCollection();

    private:
      Mutex m_lock;
      map<pair<unsigned int, double>, unsigned int> cache;
      map<pair<unsigned int, double>, time_t> cache_clock;
      vector<Centile*> centile_buckets;
      vector<unsigned int> object_sizes;
  };
}





#endif /* CENTILE_H_ */
