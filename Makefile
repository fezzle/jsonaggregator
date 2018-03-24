
all: main.cpp
	clang++ --std=c++14 -L/usr/local/Cellar/tbb/2017_U6/lib -ltbb -ltbbmalloc -I/usr/local/Cellar/tbb/2017_U6/include/ main.cpp xxHash/xxhash.c -o jsonaggregator


