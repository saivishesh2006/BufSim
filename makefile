# Compiler and Flags
CXX = g++
CXXFLAGS = -Wall -Wextra -O3 -std=c++17

# Executable Names
SIM_TARGET = bufsim
GEN_TARGET = uploader

# Source Files
SIM_SOURCES = main.cpp bufferManager.cpp queryProcessor.cpp
GEN_SOURCES = databaseUploader.cpp

# Default target: builds everything
all: $(GEN_TARGET) $(SIM_TARGET)

# Build the main simulator
$(SIM_TARGET): $(SIM_SOURCES) bufferManager.hpp queryProcessor.hpp
	$(CXX) $(CXXFLAGS) $(SIM_SOURCES) -o $(SIM_TARGET)

# Build the data uploader/generator
$(GEN_TARGET): $(GEN_SOURCES)
	$(CXX) $(CXXFLAGS) $(GEN_SOURCES) -o $(GEN_TARGET)

# Clean target: removes executables and generated data files
clean:
	rm -f $(SIM_TARGET) $(GEN_TARGET) fileBinary.bin results.csv

.PHONY: all clean