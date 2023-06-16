
#include <iostream>
#include <chrono>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include <clipp.h>


using namespace std;
using namespace rocksdb;
using namespace clipp;
using namespace chrono;

string generateRandomString(size_t length) {
    static const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dist(0, chars.length() - 1);

    std::string result(length, ' ');
    for (auto& ch : result) {
        ch = chars[dist(gen)];
    }
    return result;
}

int getRandomNumber(int min, int max) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> distribution(min, max);
    return distribution(gen);
}

int main(int argc, char** argv) {
    string dbPath;
    size_t numRecordsToWrite, numRecordsToRead, numExistingRecords;

    auto cli = (
        value("dbPath", dbPath),
        value("numRecordsToWrite", numRecordsToWrite),
        value("numRecordsToRead", numRecordsToRead),
        value("numExistingRecords", numExistingRecords)
    );

    if (!parse(argc, argv, cli)) {
        cout << make_man_page(cli, argv[0]);
        return 1;
    }

    DB* db;
    Options options;
    options.create_if_missing = true;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    Status status = DB::Open(options, dbPath, &db);
    if (!status.ok()) {
        cerr << "Failed to open or create the database: " << status.ToString() << endl;
        return 1;
    }

    WriteBatch batch;
    for (size_t i = 0; i < numExistingRecords; i++) {
        string key = "key" + to_string(i);
        string value = "value" + to_string(i);
        batch.Put(key, value);
    }
    status = db->Write(WriteOptions(), &batch);
    if (!status.ok()) {
        cerr << "Failed to perform bulk load: " << status.ToString() << endl;
        return 1;
    }

    // Perform writes with random data
    auto startWriteTime = high_resolution_clock::now();
    for (size_t i = 0; i < numRecordsToWrite; i++) {
        string key = "key" + to_string(numExistingRecords + i);
        string value = generateRandomString(100);  // Generate a random value
        status = db->Put(WriteOptions(), key, value);
        assert(status.ok());
    }
    auto endWriteTime = high_resolution_clock::now();
    auto writeDuration = duration_cast<milliseconds>(endWriteTime - startWriteTime).count();

    // Perform reads
    auto startReadTime = high_resolution_clock::now();
    for (size_t i = 0; i < numRecordsToRead; i++) {
        string key = "key" + to_string(getRandomNumber(0,numExistingRecords+numRecordsToWrite));
        string value;
        status = db->Get(ReadOptions(), key, &value);
        assert(status.ok());
    }
    auto endReadTime = high_resolution_clock::now();
    auto readDuration = duration_cast<milliseconds>(endReadTime - startReadTime).count();

    // Print statistics
    uint64_t totalBytesWritten, totalBytesRead;
    db->GetIntProperty("rocksdb.bytes.written", &totalBytesWritten);
    db->GetIntProperty("rocksdb.bytes.read", &totalBytesRead);

    cout << "Total Bytes Written: " << totalBytesWritten << endl;
    cout << "Total Bytes Read: " << totalBytesRead << endl;
    cout << "Write Time: " << writeDuration << " milliseconds" << endl;
    cout << "Read Time: " << readDuration << " milliseconds" << endl;

    delete db;
    return 0;
}
