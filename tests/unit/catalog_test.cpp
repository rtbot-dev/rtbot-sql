#include "rtbot_sql/catalog/catalog.h"

#include <gtest/gtest.h>

namespace rtbot_sql::catalog {
namespace {

class CatalogTest : public ::testing::Test {
 protected:
  Catalog catalog;

  StreamSchema make_stream(const std::string& name) {
    return StreamSchema{name, {{"a", 0}, {"b", 1}}};
  }

  ViewMeta make_view(const std::string& name, EntityType et = EntityType::VIEW) {
    ViewMeta v{};
    v.name = name;
    v.entity_type = et;
    v.view_type = ViewType::SCALAR;
    v.key_index = -1;
    return v;
  }

  TableSchema make_table(const std::string& name) {
    return TableSchema{name, {{"id", 0}}, name + "_changelog"};
  }
};

// --- Register & Lookup ---

TEST_F(CatalogTest, RegisterAndLookupStream) {
  catalog.register_stream("s1", make_stream("s1"));
  auto s = catalog.lookup_stream("s1");
  ASSERT_TRUE(s.has_value());
  EXPECT_EQ(s->name, "s1");
  EXPECT_EQ(s->columns.size(), 2u);
}

TEST_F(CatalogTest, LookupMissingStreamReturnsNullopt) {
  EXPECT_FALSE(catalog.lookup_stream("nope").has_value());
}

TEST_F(CatalogTest, RegisterAndLookupView) {
  catalog.register_view("v1", make_view("v1"));
  auto v = catalog.lookup_view("v1");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->name, "v1");
}

TEST_F(CatalogTest, RegisterAndLookupTable) {
  catalog.register_table("t1", make_table("t1"));
  auto t = catalog.lookup_table("t1");
  ASSERT_TRUE(t.has_value());
  EXPECT_EQ(t->name, "t1");
  EXPECT_EQ(t->changelog_stream, "t1_changelog");
}

// --- Resolve Entity ---

TEST_F(CatalogTest, ResolveEntityStream) {
  catalog.register_stream("s1", make_stream("s1"));
  auto et = catalog.resolve_entity("s1");
  ASSERT_TRUE(et.has_value());
  EXPECT_EQ(*et, EntityType::STREAM);
}

TEST_F(CatalogTest, ResolveEntityView) {
  catalog.register_view("v1", make_view("v1"));
  auto et = catalog.resolve_entity("v1");
  ASSERT_TRUE(et.has_value());
  EXPECT_EQ(*et, EntityType::VIEW);
}

TEST_F(CatalogTest, ResolveEntityMaterializedView) {
  catalog.register_view("mv1", make_view("mv1", EntityType::MATERIALIZED_VIEW));
  auto et = catalog.resolve_entity("mv1");
  ASSERT_TRUE(et.has_value());
  EXPECT_EQ(*et, EntityType::MATERIALIZED_VIEW);
}

TEST_F(CatalogTest, ResolveEntityTable) {
  catalog.register_table("t1", make_table("t1"));
  auto et = catalog.resolve_entity("t1");
  ASSERT_TRUE(et.has_value());
  EXPECT_EQ(*et, EntityType::TABLE);
}

TEST_F(CatalogTest, ResolveEntityMissingReturnsNullopt) {
  EXPECT_FALSE(catalog.resolve_entity("nope").has_value());
}

// --- Drop ---

TEST_F(CatalogTest, DropStream) {
  catalog.register_stream("s1", make_stream("s1"));
  catalog.drop_stream("s1");
  EXPECT_FALSE(catalog.lookup_stream("s1").has_value());
}

TEST_F(CatalogTest, DropView) {
  catalog.register_view("v1", make_view("v1"));
  catalog.drop_view("v1");
  EXPECT_FALSE(catalog.lookup_view("v1").has_value());
}

TEST_F(CatalogTest, DropTable) {
  catalog.register_table("t1", make_table("t1"));
  catalog.drop_table("t1");
  EXPECT_FALSE(catalog.lookup_table("t1").has_value());
}

TEST_F(CatalogTest, DropNonexistentIsNoOp) {
  catalog.drop_stream("nope");
  catalog.drop_view("nope");
  catalog.drop_table("nope");
}

// --- Snapshot ---

TEST_F(CatalogTest, SnapshotContainsAllEntities) {
  catalog.register_stream("s1", make_stream("s1"));
  catalog.register_view("v1", make_view("v1"));
  catalog.register_table("t1", make_table("t1"));

  auto snap = catalog.snapshot();
  EXPECT_EQ(snap.streams.size(), 1u);
  EXPECT_EQ(snap.views.size(), 1u);
  EXPECT_EQ(snap.tables.size(), 1u);
  EXPECT_EQ(snap.streams.count("s1"), 1u);
  EXPECT_EQ(snap.views.count("v1"), 1u);
  EXPECT_EQ(snap.tables.count("t1"), 1u);
}

TEST_F(CatalogTest, SnapshotIsCopy) {
  catalog.register_stream("s1", make_stream("s1"));
  auto snap = catalog.snapshot();
  catalog.drop_stream("s1");
  // Snapshot should still have it
  EXPECT_EQ(snap.streams.count("s1"), 1u);
}

// --- Key Management ---

TEST_F(CatalogTest, AddAndGetKnownKeys) {
  catalog.register_view("v1", make_view("v1"));
  catalog.add_key("v1", 42.0);
  catalog.add_key("v1", 99.0);
  auto keys = catalog.get_known_keys("v1");
  ASSERT_EQ(keys.size(), 2u);
  EXPECT_DOUBLE_EQ(keys[0], 42.0);
  EXPECT_DOUBLE_EQ(keys[1], 99.0);
}

TEST_F(CatalogTest, GetKnownKeysForMissingViewReturnsEmpty) {
  auto keys = catalog.get_known_keys("nope");
  EXPECT_TRUE(keys.empty());
}

TEST_F(CatalogTest, AddKeyToMissingViewIsNoOp) {
  catalog.add_key("nope", 1.0);  // Should not crash
  EXPECT_TRUE(catalog.get_known_keys("nope").empty());
}

// --- List Methods ---

TEST_F(CatalogTest, ListStreams) {
  catalog.register_stream("s1", make_stream("s1"));
  catalog.register_stream("s2", make_stream("s2"));
  auto names = catalog.list_streams();
  EXPECT_EQ(names.size(), 2u);
}

TEST_F(CatalogTest, ListViews) {
  catalog.register_view("v1", make_view("v1"));
  auto names = catalog.list_views();
  ASSERT_EQ(names.size(), 1u);
  EXPECT_EQ(names[0], "v1");
}

TEST_F(CatalogTest, ListTables) {
  catalog.register_table("t1", make_table("t1"));
  catalog.register_table("t2", make_table("t2"));
  auto names = catalog.list_tables();
  EXPECT_EQ(names.size(), 2u);
}

TEST_F(CatalogTest, ListMethodsReturnEmptyWhenNoneRegistered) {
  EXPECT_TRUE(catalog.list_streams().empty());
  EXPECT_TRUE(catalog.list_views().empty());
  EXPECT_TRUE(catalog.list_tables().empty());
}

}  // namespace
}  // namespace rtbot_sql::catalog
