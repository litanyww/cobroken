all : tests

CFLAGS = -Og -g
CXXFLAGS = -I. -std=c++20

SRCS = $(wildcard *.cpp)
OBJS = $(patsubst %.cpp,%.o,$(SRCS))

TEST_CFLAGS = -

GTEST_SRCS = /usr/src/googletest/googletest

TEST_SRCS = $(wildcard test/*.cpp)
TEST_OBJS = $(patsubst %.cpp,%.o,$(TEST_SRCS))

test/%.o : test/%.cpp $(wildcard *.h)
	$(CXX) $(CXXFLAGS) $(CFLAGS) -I$(GTEST_SRCS)/include -I. -c -o $@ $<

tests : $(TEST_OBJS) libgtest.a
	$(CXX) $(LDFLAGS) -o $@ $^ -lpthread

libgtest.a : gtest/gtest-all.o
	$(AR) -rc $@ $^

gtest/%.o : $(GTEST_SRCS)/src/%.cc
	-mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(CFLAGS) -I$(GTEST_SRCS) -I$(GTEST_SRCS)/include -c -o $@ $<

.PHONY : clean all

clean :
	rm -f $(OBJS) $(TEST_OBJS) libgtest.all
	rm -rf gtest
