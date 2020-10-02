#include <iostream>
#include <fstream>
#include <vector>
#include <iterator> 
#include <map> 
#include <algorithm>
using namespace std;


//number of tasks need to be processed
int MAX_TIME = 5000;

/* Task class
 * Store each task's information
 * like timestamp, jobID, taskIndex, proiority, CPU, RAM and localDiskSpace
 */
class Task {

    public:

    string timestamp, CPU, RAM, localDiskSpace;

    long jobID;
    
    int taskIndex, proiority;

    Task(string timestamp, string jobID, string taskIndex, string proiority, string CPU, string RAM, string localDiskSpace) {
    
        this->timestamp = timestamp;
    
        this->jobID = stol(jobID);
    
        this->taskIndex = stoi(taskIndex);
    
        this->proiority = stoi(proiority);
    
        this->CPU = CPU;
    
        this->RAM = RAM;
    
        this->localDiskSpace = localDiskSpace;
    
    }

};


/* Job class
 * Put tasks in a Job map: map<long, vector<Task> > tasks
 * The key of map is Job Id, and the value of map is tasks
 * Put the tasks with same Job Id together and sort individually by task index
 */
class Job {

    public:
        
        map<long, vector<Task> > tasks;

        void addTask(Task task){ tasks[task.jobID].push_back(task); }
 
        //override the comparator for vector sort method
        static bool compTaskIndex(Task &task1, Task &task2){ return task1.taskIndex < task2.taskIndex;}

        void writeToFile(ofstream & out);

};

//Sort tasks within the same Job Id and write to a output file
void Job::writeToFile(ofstream & out) {

    map<long, vector<Task> >::iterator it = tasks.begin();

    int num = 0;

    for (pair<long, vector<Task> > element : tasks) {

        vector<Task> temp = element.second;

        sort(temp.begin(), temp.end(), Job::compTaskIndex); 
        
        out << "Job ID: " << temp.at(0).jobID << endl;

        int prev = -1;
        
        for(Task task : temp) {

            if(num == MAX_TIME) break;

            if(prev == task.taskIndex) {
                
                continue;
           
            }

            out << task.timestamp << " " << task.jobID << " " 

            << task.taskIndex  << " " << task.proiority << " " 

            << task.CPU  << " " << task.RAM << " " 

            << task.localDiskSpace << endl;

            prev = task.taskIndex;

            num++;

        }

        if(num == MAX_TIME) break;
    }

    // cout << "task number: " << num << endl;
}

int main(int argc, char* argv[]) {

    //Define the input and output files

    string infilename = "input.txt";
    string outfilename = "output_5000.txt";

    if(argc == 3) {
        infilename = argv[1];
        outfilename = argv[2];
    } else {
        cout << "Wrong input" << endl;
    }

    ifstream input(infilename);
    ofstream output(outfilename);

    //Define temp variables to store input variables
    string timestamp, jobID, taskIndex, proiority, CPU, RAM, localDiskSpace;

    //Define a job object
    Job job;

    //Read until the end of the file
    while(!input.eof()) {

        //Get input of one task
        input >> timestamp >> jobID

        >> taskIndex >> proiority 

        >> CPU >> RAM >> localDiskSpace;

        //Define a task object
        Task task(timestamp, jobID, taskIndex, proiority, CPU, RAM, localDiskSpace);

        //Add task to job object
        job.addTask(task);

    }

    //Output processed tasks into output file
    job.writeToFile(output);

    output.close();

    return 0;


    //For test purposes
    // timestamp = "0";
    // jobID = "1";
    // taskIndex = "3";
    // proiority = "0";
    // CPU = "0";
    // RAM = "0";
    // localDiskSpace = "0";
    // Task task1(timestamp, jobID, taskIndex, proiority, CPU, RAM, localDiskSpace);
    // job.addTask(task1);

    // timestamp = "0";
    // jobID = "2";
    // taskIndex = "0";
    // proiority = "0";
    // CPU = "0";
    // RAM = "0";
    // localDiskSpace = "0";
    // Task task2(timestamp, jobID, taskIndex, proiority, CPU, RAM, localDiskSpace);
    // job.addTask(task2);

    // timestamp = "0";
    // jobID = "1";
    // taskIndex = "2";
    // proiority = "0";
    // CPU = "0";
    // RAM = "0";
    // localDiskSpace = "0";
    // Task task3(timestamp, jobID, taskIndex, proiority, CPU, RAM, localDiskSpace);
    // job.addTask(task3);

}


// /* Set the order of the map in class Job
//  * Sort as Job ID
//  */
// class cmp_key {
//     public:
//     bool operator()(const vector<long> &v1, const vector<long> &v2)const{
//         if(v1.at(1) < v2.at(1)) return false;
//         return v1.at(0) < v2.at(0);
//     } 
// };