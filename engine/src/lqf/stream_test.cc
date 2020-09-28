//
// Created by harper on 2/13/20.
//

#include <vector>
#include <gtest/gtest.h>
#include "stream.h"

using namespace lqf;
using namespace std;

TEST(IntStreamTest, CreateStream) {
    auto stream = IntStream::Make(1, 10);
    auto buffer = stream->collect();

    ASSERT_EQ(9, buffer->size());
    for (int i = 1; i < 10; i++) {
        ASSERT_EQ(i, (*buffer)[i - 1]);
    }
}

class TestClass {
protected:
    int32_t value_;
public:
    static int32_t counter;

    TestClass(int32_t value) : value_(value) {}

    virtual ~TestClass() {
        counter++;
    }

    int32_t GetValue() {
        return value_;
    }
};

int32_t TestClass::counter = 0;

TEST(StreamTest, Map) {
    TestClass::counter = 0;

    auto stream = IntStream::Make(1, 10);

    function<unique_ptr<TestClass>(const int32_t &)> func = [](const int32_t &val) {
        return unique_ptr<TestClass>(new TestClass(val));
    };

    auto mapped = stream->map(func);

    auto buffer = mapped->collect();

    ASSERT_EQ(9, buffer->size());

    for (int i = 0; i < 9; ++i) {
        ASSERT_EQ(i + 1, (*buffer)[i]->GetValue());
    }

    ASSERT_EQ(0, TestClass::counter);

    buffer->clear();

    ASSERT_EQ(9, TestClass::counter);
}

class TestHolder {
public:
    int value_;
public:
    TestHolder(int val) : value_(val) {}

    int getValue() {
        return value_;
    }
};

TEST(StreamTest, Collect) {
    auto source = IntStream::Make(0, 10);
    function<shared_ptr<TestHolder>(const int &)> mapper = [](const int &value) {
        return make_shared<TestHolder>(value);
    };
    auto mapped = source->map(mapper);
    auto collected = mapped->collect();
    EXPECT_EQ(collected->size(), 10);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ((*collected)[i]->getValue(), i);
    }
}

TEST(StreamTest, Foreach) {
    auto source = IntStream::Make(0, 10);
    function<shared_ptr<TestHolder>(const int &)> mapper = [](const int &value) {
        return make_shared<TestHolder>(value);
    };
    vector<int32_t> buffer;

    function<void(const shared_ptr<TestHolder>)> exec = [&buffer](const shared_ptr<TestHolder> t) {
        buffer.push_back(t->getValue());
    };

    source->map(mapper)->foreach(exec);
    EXPECT_EQ(buffer.size(), 10);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(buffer[i], i);
    }
}

TEST(StreamTest, Reduce) {
    auto source = IntStream::Make(0, 11);
    function<shared_ptr<TestHolder>(const int &)> mapper = [](const int &value) {
        return make_shared<TestHolder>(value);
    };
    auto mapped = source->map(mapper);

    function<shared_ptr<TestHolder>(const shared_ptr<TestHolder> &, const shared_ptr<TestHolder> &)> reducer =
            [](const shared_ptr<TestHolder> &a, const shared_ptr<TestHolder> &b) {
                a->value_ += b->value_;
                return a;
            };

    auto reduced = mapped->reduce(reducer);
    EXPECT_EQ(reduced->value_, 55);
}

TEST(StreamTest, ReduceParallel) {
    auto source = IntStream::Make(0, 11);
    function<shared_ptr<TestHolder>(const int &)> mapper = [](const int &value) {
        return make_shared<TestHolder>(value);
    };
    auto mapped = source->parallel()->map(mapper);

    function<shared_ptr<TestHolder>(const shared_ptr<TestHolder> &, const shared_ptr<TestHolder> &)> reducer =
            [](const shared_ptr<TestHolder> &a, const shared_ptr<TestHolder> &b) {
                a->value_ += b->value_;
                return a;
            };

    auto reduced = mapped->reduce(reducer);
    EXPECT_EQ(reduced->value_, 55);
}


TEST(StreamTest, Parallel) {
    auto source = IntStream::Make(0, 10);
    function<shared_ptr<TestHolder>(const int &)> mapper = [](const int &value) {
        return make_shared<TestHolder>(value);
    };
    auto mapped = source->map(mapper)->parallel();
    mutex lock;
    vector<int32_t> buffer;
    function<void(const shared_ptr<TestHolder>)> exec = [&buffer, &lock](const shared_ptr<TestHolder> t) {
        lock.lock();
        buffer.push_back(t->getValue());
        lock.unlock();
    };

    mapped->foreach(exec);
    EXPECT_EQ(buffer.size(), 10);

    array<int, 10> result = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(result[buffer[i]], 0);
        result[buffer[i]] = 1;
    }
}

TEST(TypelessStreamTest, MapNode) {
    using namespace typeless;

    auto node = MapHead([](int a) {
        return (uint32_t) a * 2;
    });
    auto nodechain = MapNode(node, [](uint32_t a) {
        return (uint64_t) (a + 1);
    });
    auto nodechain2 = MapNode(nodechain, [](uint64_t a) {
        return TestHolder((int) a);
    });

    int value = 2;
    EXPECT_EQ(4, node(value));
    EXPECT_EQ(5, nodechain(value));
    auto res = nodechain2(value);
    EXPECT_EQ(res.getValue(), 5);
}