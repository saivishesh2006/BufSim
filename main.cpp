#include <iostream>
#include <fstream>
#include <iomanip>
#include "queryProcessor.hpp"

using namespace std;

// ==========================================
// MODE 1: Interactive Manual Testing
// ==========================================
void runInteractiveMode(FILE *fp1, FILE *fp2) {
    cout << "\n========== INTERACTIVE MODE ==========\n";
    cout << "Select a Replacement Algorithm:\n";
    cout << "1: LRU\n2: MRU\n3: CLOCK\nChoice: ";
    int choice;
    cin >> choice;

    if (choice < 1 || choice > 3) {
        cerr << "Invalid choice. Returning to menu.\n";
        return;
    }

    cout << "Enter number of frames for the Buffer Pool: ";
    int num_Frames;
    cin >> num_Frames;

    QueryProcessor qp(num_Frames, choice);

    cout << "\nSelect Query Type:\n";
    cout << "1: Select Query\n2: Join Query\nChoice: ";
    int queryType;
    cin >> queryType;

    if (queryType == 1) {
        cout << "Enter column to filter by (1=Name, 2=Age, 3=Weight): ";
        int col;
        cin >> col;
        cout << "Enter value to match: ";
        string value;
        cin >> value;
        
        // Assuming processSelectQuery still prints its own stats
        qp.processSelectQuery(fp1, col, value); 
    } 
    else if (queryType == 2) {
        cout << "Enter column to join on for Database 1 (1=Name, 2=Age, 3=Weight): ";
        int col1;
        cin >> col1;
        cout << "Enter column to join on for Database 2 (1=Name, 2=Age, 3=Weight): ";
        int col2;
        cin >> col2;
        
        BufStats stats = qp.processJoinQuery(fp1, fp2, col1, col2);
        
        // Print the stats here since we removed them from the class!
        cout << "\n--- Join Query Stats ---\n";
        cout << "Page Accesses: " << stats.accesses << "\n";
        cout << "Disk Reads: " << stats.diskreads << "\n";
        cout << "Page Hits: " << stats.pageHits << "\n";
    } 
    else {
        cerr << "Invalid query type.\n";
    }
}

// ==========================================
// MODE 2: Automated CSV Benchmarking
// ==========================================
void runAutomatedBenchmark(FILE *fp1, FILE *fp2) {
    cout << "\n========== AUTOMATED BENCHMARK ==========\n";
    ofstream csvFile("results.csv");
    csvFile << "Buffer_Size,LRU_Reads,MRU_Reads,CLOCK_Reads\n";

    cout << left << setw(12) << "Frames" 
         << setw(15) << "LRU Reads" 
         << setw(15) << "MRU Reads" 
         << setw(15) << "CLOCK Reads\n";
    cout << "--------------------------------------------------------\n";

    for (int frames = 3; frames <= 50; frames++) {
        // Reset file pointers to the beginning for fair tests
        fseek(fp1, 0, SEEK_SET);
        fseek(fp2, 0, SEEK_SET);

        QueryProcessor qp_lru(frames, LRU);
        BufStats lru_stats = qp_lru.processJoinQuery(fp1, fp2, 1, 1);

        fseek(fp1, 0, SEEK_SET);
        fseek(fp2, 0, SEEK_SET);
        QueryProcessor qp_mru(frames, MRU);
        BufStats mru_stats = qp_mru.processJoinQuery(fp1, fp2, 1, 1);

        fseek(fp1, 0, SEEK_SET);
        fseek(fp2, 0, SEEK_SET);
        QueryProcessor qp_clock(frames, CLOCK);
        BufStats clock_stats = qp_clock.processJoinQuery(fp1, fp2, 1, 1);

        csvFile << frames << "," 
                << lru_stats.diskreads << "," 
                << mru_stats.diskreads << "," 
                << clock_stats.diskreads << "\n";

        cout << left << setw(12) << frames 
             << setw(15) << lru_stats.diskreads 
             << setw(15) << mru_stats.diskreads 
             << setw(15) << clock_stats.diskreads << "\n";
    }

    csvFile.close();
    cout << "\nBenchmark complete! Data safely written to results.csv\n";
}

// ==========================================
// MAIN ENTRY POINT
// ==========================================
int main() {
    FILE *fp1 = fopen("fileBinary.bin", "rb");
    FILE *fp2 = fopen("fileBinary.bin", "rb");

    if (!fp1 || !fp2) {
        cerr << "Error: Could not open fileBinary.bin. Run the generator first.\n";
        return 1;
    }

    while (true) {
        cout << "\n=========================================\n";
        cout << "        BUFSIM: Main Menu                \n";
        cout << "=========================================\n";
        cout << "1. Run Interactive Mode (Test single queries)\n";
        cout << "2. Run Automated Benchmark (Generate CSV)\n";
        cout << "3. Exit\n";
        cout << "Choice: ";
        
        int mode;
        cin >> mode;

        if (mode == 1) {
            runInteractiveMode(fp1, fp2);
        } else if (mode == 2) {
            runAutomatedBenchmark(fp1, fp2);
        } else if (mode == 3) {
            cout << "Exiting BufSim. Goodbye!\n";
            break;
        } else {
            cout << "Invalid choice. Try again.\n";
        }
    }

    fclose(fp1);
    fclose(fp2);
    return 0;
}