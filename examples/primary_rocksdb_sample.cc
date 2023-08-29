#include <iostream>
#include <cassert>
#include <rocksdb/db.h>
#include <ctime>
#include <unordered_set>
#include <random>
#include <sstream>
#include <chrono>
#include "clipp.h"

using namespace std;

string generateRandomKey(int keySize) {
  string key;
  for (int i = 0; i < keySize; i++) {
    int digit = rand() % 10;
    key += to_string(digit);
  }
  return key;
}


string generateRandomValue(size_t length) {
    static constexpr char chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static constexpr size_t charsCount = sizeof(chars) - 1;
    static mt19937 gen(random_device{}());
    uniform_int_distribution<size_t> dist(0, charsCount - 1);

    string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += chars[dist(gen)];
    }
    return result;
}


bool coinToss() {
  return rand() % 2 == 0;
}


int main(int argc, char* argv[]) {

  string database_path= "./testdb";
  int num_reads = 200;
  int num_entries = 1000;
  int num_writes = 300;
  int keySize = 8;

  using namespace clipp;

  auto cli = (
    value("database_path", database_path) % "database path",
    value("num_entries", num_entries) % "number of entries",
    value("num_writes", num_writes) % "number of writes issued",
    value("num_reads", num_reads) % "number of reads issued"
  );

  if (!parse(argc, argv, cli)) {
    cout << make_man_page(cli, argv[0]) << endl;
    return 1;
  }
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  // options.error_if_exists = true; // Set error_if_exists option to true

  rocksdb::Status status = rocksdb::DB::Open(options, database_path, &db);
  if (status.ok()) {
    cout << "Database opened successfully!" << endl;
  } else {
    cerr << "Failed to open database: " << status.ToString() << endl;
  }
  options.disable_auto_compactions = false;
  options.use_direct_reads = false; // or false
  options.IncreaseParallelism();


  // rocksdb::Status status = rocksdb::DB::Open(options, database_path, &db);
  // assert(status.ok());

  unordered_set<string> randomKeys;
  string value = "value";

  rocksdb::WriteBatch batch;

  for (int i = 0; i < num_entries; i++) {
    string key = generateRandomKey(keySize);
    string value = generateRandomValue(keySize);
    batch.Put(key, value);
  }

  status = db->Write(rocksdb::WriteOptions(), &batch);
  assert(status.ok());

  auto startWrite = chrono::steady_clock::now();

  for (int i = 0; i < num_writes; i++) {
    string key = generateRandomKey(keySize);
    string value = generateRandomValue(keySize);
    bool tossResult = coinToss();
    if (tossResult == 0 && randomKeys.size() < num_reads) {
       randomKeys.insert(key);
    }
    // cout << "Generated key: " << key << endl;
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
  }

  auto endWrite = chrono::steady_clock::now();

  // cout << "randomKeys size= " << randomKeys.size() << endl;

  string read_value;

  auto startRead = chrono::steady_clock::now();
  for (const string& key : randomKeys) {
    status = db->Get(rocksdb::ReadOptions(), key, &read_value);
    assert(status.ok());
    // cout << "Read val " << key << ": " << read_value << endl;
  }
  auto endRead = chrono::steady_clock::now(); 

  // delete db;

  uint64_t totalBytesWritten, totalBytesRead;
  db->GetIntProperty("rocksdb.bytes.written", &totalBytesWritten);
  db->GetIntProperty("rocksdb.bytes.read", &totalBytesRead);


  cout << "Total Bytes Written: " << totalBytesWritten << endl;
  cout << "Total Bytes Read: " << totalBytesRead << endl;

  auto writeDuration = chrono::duration_cast<chrono::milliseconds>(endWrite - startWrite);
  auto readDuration = chrono::duration_cast<chrono::milliseconds>(endRead - startRead);

  cout << "Write Time: " << writeDuration.count() << " ms" << endl;
  cout << "Read Time: " << readDuration.count();
  return 0;
}
