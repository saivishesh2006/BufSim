#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <cstdlib>

#define PAGE_SIZE 4096
using namespace std;

struct Student {
    char name[20];
    int age;
    int weight;
};

int main() {
    FILE *fp = fopen("fileBinary.bin", "wb");
    if (!fp) {
        cerr << "Failed to open file." << endl;
        return 1;
    }

    const int TOTAL_RECORDS = 1000;
    // Using vector keeps the large array off the stack, preventing stack overflows
    vector<Student> stu(TOTAL_RECORDS); 

    for (int i = 0; i < TOTAL_RECORDS; i++) {
        string name = "abc" + to_string(i);
        strcpy(stu[i].name, name.c_str());
        stu[i].age = rand() % 20 + 5;
        stu[i].weight = rand() % 50 + 20;
    }

    int rSize = sizeof(Student);
    int max_records_per_page = (PAGE_SIZE - sizeof(int)) / rSize; // 146
    int records_written = 0;

    while (records_written < TOTAL_RECORDS) {
        // Calculate exactly how many records belong in THIS specific page
        int records_in_this_page = min(max_records_per_page, TOTAL_RECORDS - records_written);
        
        // 1. Write the 4-byte header
        fwrite(&records_in_this_page, sizeof(int), 1, fp);
        int bytes_written = sizeof(int);

        // 2. Write exactly that many records
        for (int j = 0; j < records_in_this_page; j++) {
            fwrite(&stu[records_written], rSize, 1, fp);
            bytes_written += rSize;
            records_written++;
        }

        // 3. Pad the remainder of the 4096-byte block with zeros
        char pad = '\0';
        while (bytes_written < PAGE_SIZE) {
            fwrite(&pad, sizeof(char), 1, fp);
            bytes_written++;
        }
    }

    fclose(fp);
    cout << "Successfully formatted and wrote " << TOTAL_RECORDS << " records to fileBinary.bin" << endl;
    return 0;
}