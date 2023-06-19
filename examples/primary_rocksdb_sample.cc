#include <iostream>
#include <cassert>
#include <rocksdb/db.h>
#include <ctime>
#include <unordered_set>
#include <random>
#include <sstream>
#include <chrono>
#include "clipp.h"


std::string generateRandomKey(int keySize) {
  std::string key;
  for (int i = 0; i < keySize; i++) {
    int digit = rand() % 10;
    key += std::to_string(digit);
  }
  return key;
}


std::string generateRandomValue(size_t length) {
    static constexpr char chars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static constexpr size_t charsCount = sizeof(chars) - 1;
    static std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, charsCount - 1);

    std::string result;
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

  std::string database_path= "./testdb";
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
    std::cout << make_man_page(cli, argv[0]) << std::endl;
    return 1;
  }
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  // rocksdb::Status status = rocksdb::DB::Open(options, "./testdb", &db);
  rocksdb::Status status = rocksdb::DB::Open(options, database_path, &db);
  assert(status.ok());

  std::unordered_set<std::string> randomKeys;
  std::string value = "value";

  rocksdb::WriteBatch batch;

  for (int i = 0; i < num_entries; i++) {
    std::string key = generateRandomKey(keySize);
    std::string value = generateRandomValue(keySize);
    batch.Put(key, value);
  }

  status = db->Write(rocksdb::WriteOptions(), &batch);
  assert(status.ok());

  auto startWrite = std::chrono::steady_clock::now();

  for (int i = 0; i < num_writes; i++) {
    std::string key = generateRandomKey(keySize);
    std::string value = generateRandomValue(keySize);
    bool tossResult = coinToss();
    if (tossResult == 0 && randomKeys.size() < num_reads) {
       .insert(key);
    }
    std::cout << "Generated key: " << key << std::endl;
    status = db->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
  }

  auto endWrite = std::chrono::steady_clock::now();

  std::cout << "randomKeys size= " << randomKeys.size() << std::endl;

  std::string read_value;

  auto startRead = std::chrono::steady_clock::now();
  for (const std::string& key : randomKeys) {
    status = db->Get(rocksdb::ReadOptions(), key, &read_value);
    assert(status.ok());
    std::cout << "Read val " << key << ": " << read_value << std::endl;
  }
  auto endRead = std::chrono::steady_clock::now(); 

  delete db;

  auto writeDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endWrite - startWrite);
  auto readDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endRead - startRead);

  std::cout << "Write Time: " << writeDuration.count() << " ms" << std::endl;
  std::cout << "Read Time: " << readDuration.count();
  return 0;
}
