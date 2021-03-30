//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
// TEST(CatalogTest, DISABLED_CreateTableTest) {
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it
  EXPECT_NE(nullptr, catalog->GetTable("potato"));

  delete catalog;
  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(GradingCatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  // Catalog lookups should succeed.
  {
    ASSERT_EQ(table_metadata, catalog->GetTable(table_metadata->oid_));
    ASSERT_EQ(table_metadata, catalog->GetTable(table_name));
  }

  // Basic empty table attributes.
  {
    ASSERT_EQ(table_metadata->table_->GetFirstPageId(), 0);
    ASSERT_EQ(table_metadata->name_, table_name);
    ASSERT_EQ(table_metadata->schema_.GetColumnCount(), columns.size());
    for (size_t i = 0; i < columns.size(); i++) {
      ASSERT_EQ(table_metadata->schema_.GetColumns()[i].GetName(), columns[i].GetName());
      ASSERT_EQ(table_metadata->schema_.GetColumns()[i].GetType(), columns[i].GetType());
    }
  }

  // Try inserting a tuple and checking that the catalog lookup gives us the right table.
  {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(15445));
    values.emplace_back(ValueFactory::GetBooleanValue(false));
    Tuple tuple(values, &schema);

    Transaction txn(0);
    RID rid;
    table_metadata->table_->InsertTuple(tuple, &rid, &txn);

    auto table_iter = catalog->GetTable(table_name)->table_->Begin(&txn);
    ASSERT_EQ((*table_iter).GetValue(&schema, 0).CompareEquals(tuple.GetValue(&schema, 0)), CmpBool::CmpTrue);
    ASSERT_EQ((*table_iter).GetValue(&schema, 1).CompareEquals(tuple.GetValue(&schema, 1)), CmpBool::CmpTrue);
    ASSERT_EQ(++table_iter, catalog->GetTable(table_name)->table_->End());
  }

  delete catalog;
  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(GradingCatalogTest, CreateIndexTest) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  Transaction txn(0);

  auto exec_ctx = std::make_unique<ExecutorContext>(&txn, catalog.get(), bpm.get());

  TableGenerator gen{exec_ctx.get()};
  gen.GenerateTestTables();

  auto table_info = exec_ctx->GetCatalog()->GetTable("test_1");

  Schema &schema = table_info->schema_;
  auto itr = table_info->table_->Begin(&txn);
  auto tuple = *itr;

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  auto index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(&txn, "index1", "test_1", schema,
                                                                                   *key_schema, {0}, 8);
  Tuple index_key = tuple.KeyFromTuple(schema, *key_schema, index_info->index_->GetKeyAttrs());
  std::vector<RID> index_rid;
  index_info->index_->ScanKey(index_key, &index_rid, &txn);
  ASSERT_EQ(tuple.GetRid().Get(), index_rid[0].Get());

  delete key_schema;
}

}
