//
// Created by Harper on 4/20/20.
//

#include <gtest/gtest.h>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include "container.h"
#include "threadpool.h"

using namespace lqf;
using namespace lqf::threadpool;
using namespace lqf::container;

TEST(PhaseConcurrentHashSetTest, Insert) {
    PhaseConcurrentHashSet<Int64> hashSet;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_set<int> serial;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 1000000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        values.push_back(((uint32_t) rand()) % 1000);
        serial.insert(values.back());
    }
    function<int(int)> task = [&hashSet, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            hashSet.add(values[input * sliver + i]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }

    executor->invokeAll(tasks);


    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }
    executor->shutdown();
}

TEST(PhaseConcurrentHashSetTest, Resize) {
    PhaseConcurrentHashSet<Int32> hashSet(2000);

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_set<int> serial;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 10000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        values.push_back(rand() % 1000);
        serial.insert(values.back());
    }
    function<int(int)> task = [&hashSet, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            hashSet.add(values[input * sliver + i]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }

    hashSet.resize(3000);

    EXPECT_EQ(hashSet.limit(), 4096);

    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }

    executor->shutdown();
}

TEST(PhaseConcurrentHashSetTest, Delete) {
    PhaseConcurrentHashSet<Int32> set;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        set.add(i);
    }
    EXPECT_EQ(5000, set.size());

    auto executor = Executor::Make(20);

    function<int(int)> task = [&set](int index) {
        for (int i = 0; i < 200; ++i) {
            set.remove(index * 200 + i);
        }
        return index + 100;
    };
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, set.size());
    for (int i = 0; i < 3000; ++i) {
        EXPECT_TRUE(set.test(2000 + i));
    }

    function<int(int)> task2 = [&set](int index) {
        for (int i = 0; i < 200; ++i) {
            set.remove(5000 + index * 200 + i);
        }
        return 0;
    };

    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task2, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, set.size());

    executor->shutdown();
}


TEST(PhaseConcurrentHashSetTest, Iterator) {
    PhaseConcurrentHashSet<Int32> set;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        set.add(i + 35200);
    }
    EXPECT_EQ(5000, set.size());

    auto iterator = set.iterator();
    auto counter = 0;
    unordered_set<int32_t> ref_set;

    while (iterator->hasNext()) {
        auto next = iterator->next();
        ref_set.insert(next);
        ++counter;
    }
    EXPECT_EQ(5000, counter);
    for (int i = 0; i < 5000; ++i) {
        EXPECT_TRUE(ref_set.find(i + 35200) != ref_set.end());
    }
}

TEST(PhaseConcurrentIntHashMapTest, Insert) {

    PhaseConcurrentIntHashMap hashMap;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_map<int, int> serial;
    vector<int32_t> keys;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 10000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        int key = i;
        int value = rand();

        keys.push_back(key);
        values.push_back(value);

        serial[key] = value;
    }
    function<int(int)> task = [&hashMap, &keys, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            auto index = input * sliver + i;
            hashMap.put(keys[index], values[index]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);


    EXPECT_EQ(hashMap.size(), serial.size());
    for (auto &entry:serial) {
        EXPECT_EQ(hashMap.get(entry.first), entry.second);
    }

    executor->shutdown();
}

TEST(PhaseConcurrentIntHashMapTest, Delete) {
    PhaseConcurrentIntHashMap map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i, i + 2);
    }
    EXPECT_EQ(5000, map.size());

    auto executor = Executor::Make(20);

    function<int(int)> task = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item, key + 2);
        }
        return 0;
    };
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());
    for (int i = 0; i < 3000; ++i) {
        auto key = 2000 + i;
        auto item = map.get(key);
        EXPECT_EQ(item, key + 2);
    }

    function<int(int)> task1p = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item, INT32_MIN);
        }
        return 0;
    };

    function<int(int)> task2 = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto notExist = map.remove(5000 + index * 200 + i);
            EXPECT_EQ(notExist, INT32_MIN);
        }
        return 0;
    };

    tasks.clear();
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task1p, i));
        tasks.push_back(bind(task2, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());

    executor->shutdown();
}

TEST(PhaseConcurrentIntHashMapTest, Iterator) {
    PhaseConcurrentIntHashMap map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i + 35200, i + 2);
    }
    EXPECT_EQ(5000, map.size());

    auto iterator = map.iterator();
    auto counter = 0;
    unordered_set<int32_t> ref_set;

    while (iterator->hasNext()) {
        auto next = iterator->next();
        EXPECT_EQ(next.second, next.first + 2 - 35200);
        ref_set.insert(next.first - 35200);
        ++counter;
    }
    EXPECT_EQ(5000, counter);
    for (int i = 0; i < 5000; ++i) {
        EXPECT_TRUE(ref_set.find(i) != ref_set.end());
    }
}


TEST(PhaseConcurrentInt64HashMapTest, Insert) {

    PhaseConcurrentInt64HashMap hashMap;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_map<int64_t, int> serial;
    vector<int64_t> keys;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 100000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        int64_t key = i;
        int value = rand();

        keys.push_back(key);
        values.push_back(value);

        serial[key] = value;
    }
    function<int(int)> task = [&hashMap, &keys, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            auto index = input * sliver + i;
            hashMap.put(keys[index], values[index]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(hashMap.size(), serial.size());
    for (auto &entry:serial) {
        EXPECT_EQ(hashMap.get(entry.first), entry.second);
    }

    executor->shutdown();
}

TEST(PhaseConcurrentInt64HashMapTest, Delete) {
    PhaseConcurrentInt64HashMap map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i, i + 2);
    }
    EXPECT_EQ(5000, map.size());

    auto executor = Executor::Make(20);

    function<int(int)> task = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item, key + 2);
        }
        return 0;
    };
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());
    for (int i = 0; i < 3000; ++i) {
        auto key = 2000 + i;
        auto item = map.get(key);
        EXPECT_EQ(item, key + 2);
    }

    function<int(int)> task1p = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item, INT32_MIN);
        }
        return 0;
    };

    function<int(int)> task2 = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto notExist = map.remove(5000 + index * 200 + i);
            EXPECT_EQ(notExist, INT32_MIN);
        }
        return 0;
    };

    tasks.clear();
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task1p, i));
        tasks.push_back(bind(task2, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());

    executor->shutdown();
}

TEST(PhaseConcurrentInt64HashMapTest, Iterator) {
    PhaseConcurrentInt64HashMap map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i + 35200, i + 2);
    }
    EXPECT_EQ(5000, map.size());

    auto iterator = map.iterator();
    auto counter = 0;
    unordered_set<int32_t> ref_set;

    while (iterator->hasNext()) {
        auto next = iterator->next();
        EXPECT_EQ(next.second, next.first + 2 - 35200);
        ref_set.insert(next.first - 35200);
        ++counter;
    }
    EXPECT_EQ(5000, counter);
    for (int i = 0; i < 5000; ++i) {
        EXPECT_TRUE(ref_set.find(i) != ref_set.end());
    }
}

struct DemoObject {
    int value1;
    int value2;
};

TEST(PhaseConcurrentHashMapTest, Insert) {

    PhaseConcurrentHashMap<Int32, DemoObject *> hashMap;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_map<int, DemoObject *> serial;
    vector<int32_t> keys;
    vector<DemoObject *> values;

    srand(time(NULL));

    int total = 10000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        int key = i;
        DemoObject *value = new DemoObject{rand(), rand()};

        keys.push_back(key);
        values.push_back(value);

        serial[key] = value;
    }
    function<int(int)> task = [&hashMap, &keys, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            auto index = input * sliver + i;
            hashMap.put(keys[index], values[index]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);


    EXPECT_EQ(hashMap.size(), serial.size());
    for (auto &entry:serial) {
        EXPECT_EQ(hashMap.get(entry.first), entry.second);
    }

    executor->shutdown();
}

TEST(PhaseConcurrentHashMapTest, Delete) {
    PhaseConcurrentHashMap<Int32, DemoObject *> map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i, new DemoObject{i, i + 2});
    }
    EXPECT_EQ(5000, map.size());

    auto executor = Executor::Make(20);

    function<int(int)> task = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item->value1, key);
            EXPECT_EQ(item->value2, key + 2);
            delete item;
        }
        return 0;
    };
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());
    for (int i = 0; i < 3000; ++i) {
        auto key = 2000 + i;
        auto item = map.get(key);
        EXPECT_EQ(item->value1, key);
        EXPECT_EQ(item->value2, key + 2);
    }

    function<int(int)> task1p = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto key = index * 200 + i;
            auto item = map.remove(index * 200 + i);
            EXPECT_EQ(item, nullptr);
        }
        return 0;
    };

    function<int(int)> task2 = [&map](int index) {
        for (int i = 0; i < 200; ++i) {
            auto notExist = map.remove(5000 + index * 200 + i);
            EXPECT_EQ(notExist, nullptr);
        }
        return 0;
    };

    tasks.clear();
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task1p, i));
        tasks.push_back(bind(task2, i));
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(3000, map.size());

    executor->shutdown();
}

TEST(PhaseConcurrentHashMapTest, Iterator) {
    PhaseConcurrentHashMap<Int32, DemoObject *> map;
//    srand(time(NULL));
    for (int i = 0; i < 5000; ++i) {
        map.put(i + 35200, new DemoObject{i, i + 2});
    }
    EXPECT_EQ(5000, map.size());

    auto iterator = map.iterator();
    auto counter = 0;
    unordered_set<int32_t> ref_set;

    while (iterator->hasNext()) {
        auto next = iterator->next();
        EXPECT_EQ(next.second->value1, next.first - 35200);
        EXPECT_EQ(next.second->value2, next.first + 2 - 35200);
        ref_set.insert(next.first - 35200);
        ++counter;
    }
    EXPECT_EQ(5000, counter);
    for (int i = 0; i < 5000; ++i) {
        EXPECT_TRUE(ref_set.find(i) != ref_set.end());
    }
}