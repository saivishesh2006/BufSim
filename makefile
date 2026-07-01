CXX      = g++
CXXFLAGS = -Wall -Wextra -O2 -std=c++17

SIM_TARGET  = bufsim
SIM_SOURCES = main.cpp bufferManager.cpp catalog.cpp sqlParser.cpp queryExecutor.cpp

all: $(SIM_TARGET)

$(SIM_TARGET): $(SIM_SOURCES) bufferManager.hpp catalog.hpp sqlParser.hpp queryExecutor.hpp
	$(CXX) $(CXXFLAGS) $(SIM_SOURCES) -o $(SIM_TARGET)

clean:
	rm -f bufsim data/*.bin output.txt *.o catalog.json

.PHONY: all clean
