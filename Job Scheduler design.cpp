#include <iostream>
#include <vector>
#include <fstream>
#include <algorithm>
#include <queue>

struct Job {
    int id;
    int arrivalTime;
    int coresRequired;
    int memoryRequired;
    int executionTime;

    Job(int id, int arrival, int cores, int memory, int execTime)
        : id(id), arrivalTime(arrival), coresRequired(cores), memoryRequired(memory), executionTime(execTime) {}

    int grossValue() const {
        return executionTime * coresRequired * memoryRequired;
    }
};

struct WorkerNode {
    int id;
    int availableCores;
    int availableMemory;
    int currentJobEndTime;

    WorkerNode(int id) : id(id), availableCores(24), availableMemory(64), currentJobEndTime(0) {}

    bool canAccommodate(const Job &job) const {
        return availableCores >= job.coresRequired && availableMemory >= job.memoryRequired;
    }

    void assignJob(const Job &job, int currentTime) {
        availableCores -= job.coresRequired;
        availableMemory -= job.memoryRequired;
        currentJobEndTime = currentTime + job.executionTime;
    }

    void releaseResources(const Job &job) {
        availableCores += job.coresRequired;
        availableMemory += job.memoryRequired;
    }
};

// Sorting functions for queue policies
bool fcfsComparator(const Job &a, const Job &b) {
    return a.arrivalTime < b.arrivalTime;
}

bool smallestJobFirstComparator(const Job &a, const Job &b) {
    return a.grossValue() < b.grossValue();
}

bool shortDurationFirstComparator(const Job &a, const Job &b) {
    return a.executionTime < b.executionTime;
}

// Node selection policies
WorkerNode* firstFit(std::vector<WorkerNode> &nodes, const Job &job) {
    for (auto &node : nodes) {
        if (node.canAccommodate(job)) return &node;
    }
    return nullptr;
}

WorkerNode* bestFit(std::vector<WorkerNode> &nodes, const Job &job) {
    WorkerNode* bestNode = nullptr;
    for (auto &node : nodes) {
        if (node.canAccommodate(job) && (!bestNode || node.availableCores < bestNode->availableCores)) {
            bestNode = &node;
        }
    }
    return bestNode;
}

WorkerNode* worstFit(std::vector<WorkerNode> &nodes, const Job &job) {
    WorkerNode* worstNode = nullptr;
    for (auto &node : nodes) {
        if (node.canAccommodate(job) && (!worstNode || node.availableCores > worstNode->availableCores)) {
            worstNode = &node;
        }
    }
    return worstNode;
}

void writeToCSV(const std::vector<WorkerNode> &nodes, const std::string &filename) {
    std::ofstream file(filename);
    file << "Node ID,Available Cores,Available Memory,Job End Time\n";
    for (const auto &node : nodes) {
        file << node.id << "," << node.availableCores << "," << node.availableMemory << "," << node.currentJobEndTime << "\n";
    }
    file.close();
}

void simulateScheduler(std::vector<Job> &jobs, std::vector<WorkerNode> &nodes, 
                       bool (*jobQueuePolicy)(const Job&, const Job&),
                       WorkerNode* (*nodeSelectPolicy)(std::vector<WorkerNode>&, const Job&)) {
    
    std::sort(jobs.begin(), jobs.end(), jobQueuePolicy);
    int currentTime = 0;

    while (!jobs.empty()) {
        // Update current time to the arrival time of the next job in the queue
        currentTime = std::max(currentTime, jobs.front().arrivalTime);

        for (auto &node : nodes) {
            if (node.currentJobEndTime <= currentTime) {
                // Release resources of any completed jobs
                for (const Job &job : jobs) {
                    if (node.currentJobEndTime == currentTime) {
                        node.releaseResources(job);
                    }
                }
            }
        }

        bool jobScheduled = false;
        for (auto it = jobs.begin(); it != jobs.end(); ) {
            if (it->arrivalTime <= currentTime) {
                WorkerNode* assignedNode = nodeSelectPolicy(nodes, *it);
                if (assignedNode) {
                    assignedNode->assignJob(*it, currentTime);
                    std::cout << "Job ID " << it->id << " assigned to Node ID " << assignedNode->id << " at time " << currentTime << "\n";
                    it = jobs.erase(it);
                    jobScheduled = true;
                } else {
                    ++it;
                }
            } else {
                ++it;
            }
        }

        // Increment time if no jobs were scheduled in the current cycle
        if (!jobScheduled) {
            currentTime++;
        }
    }
}

int main() {
    int numJobs;
    std::vector<Job> jobs;
    std::vector<WorkerNode> nodes;

    // Initialize worker nodes
    for (int i = 0; i < 128; ++i) {
        nodes.push_back(WorkerNode(i + 1));
    }

    std::cout << "Enter the number of jobs: ";
    std::cin >> numJobs;

    for (int i = 0; i < numJobs; ++i) {
        int id = i + 1;
        int arrival, cores, memory, execTime;

        std::cout << "Enter arrival time, cores required, memory required (GB), and execution time (hours) for job " << id << ":\n";
        std::cin >> arrival >> cores >> memory >> execTime;

        jobs.emplace_back(id, arrival, cores, memory, execTime);
    }

    // Choose job queue policy
    int queuePolicyChoice;
    std::cout << "\nSelect Job Queue Policy:\n1. FCFS\n2. Smallest Job First\n3. Short Duration First\nChoice: ";
    std::cin >> queuePolicyChoice;

    bool (*jobQueuePolicy)(const Job&, const Job&) = fcfsComparator;
    if (queuePolicyChoice == 2) {
        jobQueuePolicy = smallestJobFirstComparator;
    } else if (queuePolicyChoice == 3) {
        jobQueuePolicy = shortDurationFirstComparator;
    }

    // Choose worker node selection policy
    int nodePolicyChoice;
    std::cout << "\nSelect Worker Node Selection Policy:\n1. First Fit\n2. Best Fit\n3. Worst Fit\nChoice: ";
    std::cin >> nodePolicyChoice;

    WorkerNode* (*nodeSelectPolicy)(std::vector<WorkerNode>&, const Job&) = firstFit;
    if (nodePolicyChoice == 2) {
        nodeSelectPolicy = bestFit;
    } else if (nodePolicyChoice == 3) {
        nodeSelectPolicy = worstFit;
    }

    // Simulate scheduler
    simulateScheduler(jobs, nodes, jobQueuePolicy, nodeSelectPolicy);

    // Output results to CSV
    writeToCSV(nodes, "worker_node_utilization.csv");
    std::cout << "\nWorker node utilization data has been saved to 'worker_node_utilization.csv'.\n";

    return 0;
}
