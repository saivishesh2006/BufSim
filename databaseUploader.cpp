#include <bits/stdc++.h>
#define PAGE_SIZE 4096
using namespace std;


// make a struct for database
typedef struct{

    char name[20];
    int age;
    int weight;
}Student;

int main(){

    FILE *fp;
    fp = fopen("fileBinary.bin", "wb");


    Student stu[1000];
    for(int i=0; i<1000; i++){
        string name = "abc" + to_string(i);
        strcpy(stu[i].name, name.c_str());
        stu[i].age = rand()%20+5;
        stu[i].weight = rand()%50+20;
        cout<<stu[i].name<<" "<<stu[i].age<<" "<<stu[i].weight<<endl;
    }

    int rSize = sizeof(Student);
    // add first 4 bytes in page 1 as number of records in last page
    int i=0;
    int total_Rec = 1000;

    while(1){
        int num_Left = PAGE_SIZE;
        // starting of each page add number of records in this page
        int pos = (PAGE_SIZE-4)/rSize;
        int records_In_Page = min(pos, total_Rec);
        fwrite(&records_In_Page, sizeof(int), 1, fp);
        total_Rec -= records_In_Page;
        num_Left -= sizeof(int);

        while(num_Left >= rSize){
            fwrite(&stu[i], rSize, 1, fp);
            num_Left -= rSize;
            i++;
            if(i==1000){
                break;
            }
        }
        while(num_Left > 0){
            char c = '\0';
            fwrite(&c, sizeof(char), 1, fp);
            num_Left--;
        }
        if(i==1000){
            break;
        }
    }
    fclose(fp);
}