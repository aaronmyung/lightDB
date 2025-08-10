#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <any>
#include <typeindex>
#include <stdexcept>
#include <memory>
#include <utility>
#include <optional>
#include <functional>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <sstream>
#include <mutex>
#include <variant>
#include <type_traits>
#include <filesystem>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace ldb
{

    // Forward declarations
    class LightDB;
    class Table;
    class Query;
    class Transaction;
    class Schema;
    class TransactionalView;

    class LightDBException : public std::runtime_error
    {
    public:
        using std::runtime_error::runtime_error;
    };

    using FieldValue = std::any;

    struct Row : public std::unordered_map<std::string, FieldValue>
    {
        using std::unordered_map<std::string, FieldValue>::unordered_map;
    };

    struct TypeHandler
    {
        std::function<size_t(const FieldValue &)> hash;
        std::function<bool(const FieldValue &, const FieldValue &)> equal;
        std::function<void(std::ostream &, const FieldValue &)> print;
        std::function<bool(const FieldValue &, const FieldValue &)> less_than;
        std::function<std::string(const FieldValue &)> serialise;
        std::function<FieldValue(const std::string &)> deserialise;
        std::string name;
    };

    class TypeRegistry
    {
    public:
        template <typename T>
        void register_type(const std::string &type_name)
        {
            handlers_[typeid(T)] = {
                [](const FieldValue &val) -> size_t
                {
                    if (!val.has_value())
                        return 0;
                    return std::hash<T>{}(std::any_cast<const T &>(val));
                },
                [](const FieldValue &lhs, const FieldValue &rhs)
                {
                    if (lhs.has_value() != rhs.has_value())
                        return false;
                    if (!lhs.has_value())
                        return true;
                    return std::any_cast<const T &>(lhs) == std::any_cast<const T &>(rhs);
                },
                [](std::ostream &os, const FieldValue &val)
                {
                    if (!val.has_value())
                        os << "NULL";
                    else
                        os << std::any_cast<const T &>(val);
                },
                [](const FieldValue &lhs, const FieldValue &rhs)
                {
                    if (lhs.has_value() != rhs.has_value()) {
                        return !lhs.has_value(); // NULLs are considered smallest
                    }
                    if (!lhs.has_value()) {
                        return false; // Both are NULL
                    }
                    return std::any_cast<const T &>(lhs) < std::any_cast<const T &>(rhs);
                },
                [](const FieldValue &val) -> std::string
                {
                    if (!val.has_value())
                        return "";
                    std::stringstream ss;
                    if constexpr (std::is_same_v<T, std::string>)
                    {
                        const auto& s = std::any_cast<const T&>(val);
                        bool needs_quoting = (s.find(',') != std::string::npos || s.find('"') != std::string::npos || s.find('\n') != std::string::npos);
                        if (!needs_quoting) return s;

                        ss << '"';
                        for (char c : s) {
                            if (c == '"') ss << "\"\"";
                            else ss << c;
                        }
                        ss << '"';
                    }
                    else if constexpr (std::is_same_v<T, bool>)
                    {
                        ss << (std::any_cast<T>(val) ? "true" : "false");
                    }
                    else
                    {
                        ss << std::any_cast<const T &>(val);
                    }
                    return ss.str();
                },
                [](const std::string &str) -> FieldValue
                {
                    if (str.empty())
                        return {};
                    T value;
                    if constexpr (std::is_same_v<T, std::string>)
                    {
                        if (str.length() >= 2 && str.front() == '"' && str.back() == '"') {
                            std::string content = str.substr(1, str.length() - 2);
                            std::string unescaped;
                            unescaped.reserve(content.length());
                            for (size_t i = 0; i < content.length(); ++i) {
                                if (content[i] == '"' && i + 1 < content.length() && content[i+1] == '"') {
                                    unescaped += '"';
                                    i++;
                                } else {
                                    unescaped += content[i];
                                }
                            }
                            value = unescaped;
                        } else {
                            value = str;
                        }
                    }
                    else if constexpr (std::is_same_v<T, bool>)
                    {
                        value = (str == "true" || str == "1");
                    }
                    else if constexpr (std::is_arithmetic_v<T>)
                    {
                        std::stringstream ss(str);
                        ss >> value;
                        if (ss.fail() || !ss.eof()) {
                            throw LightDBException("Failed to deserialise string '" + str + "' to an arithmetic type.");
                        }
                    }
                    else
                    {
                        throw LightDBException("Cannot deserialise type '" + std::string(typeid(T).name()) + "'. Automatic deserialisation is only supported for built-in arithmetic types, bool, and string.");
                    }
                    return value;
                },
                type_name};
            name_to_type_.emplace(type_name, typeid(T));
        }

        const TypeHandler *get_handler(const std::type_index &type) const
        {
            auto it = handlers_.find(type);
            if (it == handlers_.end())
            {
                return nullptr;
            }
            return &it->second;
        }

        const TypeHandler *get_handler_by_name(const std::string &name) const
        {
            auto it = name_to_type_.find(name);
            if (it == name_to_type_.end())
            {
                return nullptr;
            }
            return get_handler(it->second);
        }

        std::string get_type_name(const std::type_index &type) const
        {
            if (const auto *handler = get_handler(type))
            {
                return handler->name;
            }
            return "unknown";
        }

        std::type_index get_type_index_by_name(const std::string &name) const
        {
            auto it = name_to_type_.find(name);
            if (it == name_to_type_.end())
            {
                return typeid(void);
            }
            return it->second;
        }

    private:
        std::unordered_map<std::type_index, TypeHandler> handlers_;
        std::unordered_map<std::string, std::type_index> name_to_type_;
    };

    inline TypeRegistry &get_type_registry()
    {
        static TypeRegistry registry = []
        {
            TypeRegistry r;
            r.register_type<int>("int");
            r.register_type<double>("double");
            r.register_type<std::string>("string");
            r.register_type<bool>("bool");
            return r;
        }();
        return registry;
    }

    struct FieldValueHash
    {
        size_t operator()(const FieldValue &val) const
        {
            if (!val.has_value())
            {
                return 0;
            }
            if (const auto *handler = get_type_registry().get_handler(val.type()))
            {
                return handler->hash(val);
            }
            throw LightDBException("Attempted to hash an unregistered type.");
        }
    };

    struct FieldValueEqual
    {
        bool operator()(const FieldValue &lhs, const FieldValue &rhs) const
        {
            if (lhs.type() != rhs.type())
                return false;

            if (const auto *handler = get_type_registry().get_handler(lhs.type()))
            {
                return handler->equal(lhs, rhs);
            }
            // This case handles NULL == NULL, where type is `void` and no handler exists.
            if (!lhs.has_value() && !rhs.has_value()) {
                return true;
            }
            return false;
        }
    };

    inline std::ostream &operator<<(std::ostream &os, const FieldValue &value)
    {
        if (!value.has_value())
        {
            os << "NULL";
            return os;
        }
        if (const auto *handler = get_type_registry().get_handler(value.type()))
        {
            if (value.type() == typeid(std::string))
            {
                os << '"';
                handler->print(os, value);
                os << '"';
            }
            else
            {
                handler->print(os, value);
            }
        }
        else
        {
            os << "[unprintable type: " << value.type().name() << "]";
        }
        return os;
    }

    inline std::ostream &operator<<(std::ostream &os, const Row &row)
    {
        os << "{ ";
        bool first = true;
        for (const auto &[key, val] : row)
        {
             if (!first) {
                os << ", ";
            }
            os << key << ": " << val;
            first = false;
        }
        os << " }";
        return os;
    }

    enum class OnDeleteAction
    {
        Restrict,
        Cascade
    };

    class Schema
    {
    public:
        friend class LightDB;
        friend class Table;
        friend struct CreateTableCommand;

        struct ForeignKeyConstraint
        {
            std::string local_field;
            std::string target_table;
            std::string target_field;
            OnDeleteAction on_delete_action;
        };

        explicit Schema(std::string pk_field_name) : primary_key_field(std::move(pk_field_name))
        {
            fields.emplace(primary_key_field, typeid(int));
        }

        template <typename T>
        Schema &add_field(const std::string &name, bool nullable = false)
        {
            if (fields.count(name))
            {
                throw LightDBException("Field '" + name + "' already exists in schema.");
            }
            if (get_type_registry().get_handler(typeid(T)) == nullptr)
            {
                throw LightDBException("Type '" + std::string(typeid(T).name()) + "' is not registered. Please register it first.");
            }
            fields.emplace(name, typeid(T));
            if (nullable)
            {
                nullable_fields.insert(name);
            }
            return *this;
        }

        Schema &add_index(const std::string &field_name)
        {
            if (!fields.count(field_name))
            {
                throw LightDBException("Cannot create index on non-existent field '" + field_name + "'.");
            }
            indexed_fields.insert(field_name);
            return *this;
        }

        Schema &add_foreign_key(const std::string &field, const std::string &target_table, const std::string &target_field, OnDeleteAction action = OnDeleteAction::Restrict)
        {
            if (!fields.count(field))
            {
                throw LightDBException("Cannot create foreign key on non-existent field '" + field + "'.");
            }
            foreign_keys.push_back({field, target_table, target_field, action});
            return *this;
        }

        std::string get_validation_error(const Row &row) const
        {
            for (const auto &[name, value] : row)
            {
                if (fields.find(name) == fields.end())
                {
                    return "Row contains unrecognised field '" + name + "'.";
                }
            }

            for (const auto &[name, type] : fields)
            {
                auto it = row.find(name);
                if (it == row.end())
                {
                    if (nullable_fields.find(name) == nullable_fields.end()) {
                         return "Missing field '" + name + "'";
                    }
                }
                else if (!it->second.has_value())
                {
                    if (nullable_fields.find(name) == nullable_fields.end())
                    {
                        return "Field '" + name + "' cannot be null.";
                    }
                }
                else if (std::type_index(it->second.type()) != type)
                {
                    return "Field '" + name + "' has type '" + get_type_registry().get_type_name(it->second.type()) + "' but expected '" + get_type_registry().get_type_name(type) + "'";
                }
            }
            return "";
        }

        const std::string &get_primary_key_field() const { return primary_key_field; }
        const std::unordered_map<std::string, std::type_index> &get_fields() const { return fields; }
        const std::unordered_set<std::string> &get_indexed_fields() const { return indexed_fields; }
        const std::vector<ForeignKeyConstraint> &get_foreign_keys() const { return foreign_keys; }
        bool is_nullable(const std::string &field_name) const { return nullable_fields.count(field_name); }

    private:
        std::string primary_key_field;
        std::unordered_map<std::string, std::type_index> fields;
        std::unordered_set<std::string> indexed_fields;
        std::unordered_set<std::string> nullable_fields;
        std::vector<ForeignKeyConstraint> foreign_keys;
    };


    // Structs for journaling
    struct CreateTableCommand
    {
        std::string table_name;
        Schema schema;
    };

    using PrimaryKey = int;

    struct InsertCommand
    {
        std::string table_name;
        Row row;
    };

    struct UpdateCommand
    {
        std::string table_name;
        PrimaryKey pk;
        Row partial_row;
    };

    struct DeleteCommand
    {
        std::string table_name;
        PrimaryKey pk;
    };

    using Command = std::variant<CreateTableCommand, InsertCommand, UpdateCommand, DeleteCommand>;


    enum class SortOrder
    {
        Ascending,
        Descending
    };

    enum class LogicalOpType
    {
        AND,
        OR
    };

    class RowBuilder
    {
    public:
        RowBuilder(Table &table_ref, LightDB &db_ref);

        template <typename T>
        RowBuilder &set(const std::string &field, T &&value)
        {
            using DecayedT = std::decay_t<T>;
            if constexpr (std::is_same_v<DecayedT, const char*>) {
                row_[field] = std::string(value);
            } else {
                row_[field] = std::forward<T>(value);
            }
            return *this;
        }

        PrimaryKey insert();

    private:
        Row row_;
        Table &table_;
        LightDB &db_;
    };

    class Query
    {
    public:
        enum class PredicateOp
        {
            Equals,
            NotEquals,
            GreaterThan,
            LessThan
        };
        struct Predicate
        {
            std::string field;
            FieldValue value;
            PredicateOp op;
            std::function<bool(const Row &)> func;
        };

        Query(const Table &table, LightDB &db_context) : table_(table), db_context_(db_context) {}

        class Condition
        {
        public:
            Condition(Query &query, std::string field) : query_(query), field_(std::move(field)) {}

            template <typename T>
            Query &equals(T &&value) { return add_predicate(PredicateOp::Equals, std::forward<T>(value)); }
            template <typename T>
            Query &not_equals(T &&value) { return add_predicate(PredicateOp::NotEquals, std::forward<T>(value)); }
            template <typename T>
            Query &greater_than(T &&value) { return add_comparison_predicate(PredicateOp::GreaterThan, std::forward<T>(value)); }
            template <typename T>
            Query &less_than(T &&value) { return add_comparison_predicate(PredicateOp::LessThan, std::forward<T>(value)); }

        private:
            template <typename T>
            Query &add_predicate(PredicateOp op, T &&value)
            {
                FieldValue val;
                using DecayedT = std::decay_t<T>;
                if constexpr (std::is_same_v<DecayedT, const char*>) {
                    val = std::string(value);
                } else {
                    val = std::forward<T>(value);
                }

                std::function<bool(const FieldValue &, const FieldValue &)> p;
                if (op == PredicateOp::Equals) {
                    p = FieldValueEqual{};
                } else {
                    p = std::not_fn(FieldValueEqual{});
                }

                if (!query_.predicates_.empty()) {
                    const auto& last_item = query_.predicates_.back();
                    if(std::holds_alternative<Predicate>(last_item)) {
                        query_.predicates_.push_back(LogicalOpType::AND);
                    }
                }

                query_.predicates_.push_back(Predicate{field_, val, op,
                                              [field = field_, val, p](const Row &row)
                                              {
                                                  auto it = row.find(field);
                                                  if (it == row.end() || !it->second.has_value())
                                                      return false;
                                                  return p(it->second, val);
                                              }});
                return query_;
            }

            template <typename T>
            Query &add_comparison_predicate(PredicateOp op, T &&value)
            {
                FieldValue val;
                using DecayedT = std::decay_t<T>;
                if constexpr (std::is_same_v<DecayedT, const char*>) {
                    val = std::string(value);
                } else {
                    val = std::forward<T>(value);
                }

                if (!query_.predicates_.empty()) {
                    const auto& last_item = query_.predicates_.back();
                    if(std::holds_alternative<Predicate>(last_item)) {
                        query_.predicates_.push_back(LogicalOpType::AND);
                    }
                }

                query_.predicates_.push_back(Predicate{field_, val, op,
                                              [field = field_, val, op](const Row &row)
                                              {
                                                  auto it = row.find(field);
                                                  if (it == row.end() || !it->second.has_value())
                                                      return false;
                                                  const auto *handler = get_type_registry().get_handler(it->second.type());
                                                  if (!handler || !handler->less_than)
                                                      return false;

                                                  if (op == PredicateOp::GreaterThan)
                                                      return handler->less_than(val, it->second);
                                                  if (op == PredicateOp::LessThan)
                                                      return handler->less_than(it->second, val);
                                                  return false;
                                              }});
                return query_;
            }

            Query &query_;
            std::string field_;
        };

        Condition where(const std::string &field)
        {
            return Condition(*this, field);
        }

        Query &and_()
        {
            if (!predicates_.empty() && std::holds_alternative<Predicate>(predicates_.back())) {
                predicates_.push_back(LogicalOpType::AND);
            }
            return *this;
        }

        Query &or_()
        {
            if (!predicates_.empty() && std::holds_alternative<Predicate>(predicates_.back())) {
                predicates_.push_back(LogicalOpType::OR);
            }
            return *this;
        }

        Query &order_by(std::string field, SortOrder order = SortOrder::Ascending)
        {
            sorter_ = [field = std::move(field), order](const Row &a, const Row &b)
            {
                const auto &val_a = a.at(field);
                const auto &val_b = b.at(field);

                // If types are different, it's because one is NULL.
                // The less_than handler knows how to deal with this.
                // We just need to get a valid handler from a non-NULL value.
                const std::type_info& type_for_handler = val_a.has_value() ? val_a.type() : val_b.type();
                const auto* handler = get_type_registry().get_handler(type_for_handler);

                // If no handler (e.g., both values are NULL or type not registered), they are equivalent.
                if (!handler || !handler->less_than) {
                    return false;
                }

                if (order == SortOrder::Ascending) {
                    return handler->less_than(val_a, val_b);
                } else { // Descending
                    return handler->less_than(val_b, val_a);
                }
            };
            return *this;
        }

        Query &limit(size_t count)
        {
            limit_ = count;
            return *this;
        }

        std::vector<Row> execute() const;

        size_t count() const;
        std::optional<FieldValue> sum(const std::string &field) const;
        std::optional<FieldValue> avg(const std::string &field) const;
        std::optional<FieldValue> min(const std::string &field) const;
        std::optional<FieldValue> max(const std::string &field) const;

        int remove();
        int update(const Row &partial_row);

    private:
        friend class Table;
        std::vector<Row> get_filtered_rows() const;

        const Table &table_;
        LightDB &db_context_;
        std::vector<std::variant<Predicate, LogicalOpType>> predicates_;
        std::function<bool(const Row &, const Row &)> sorter_;
        std::optional<size_t> limit_;
    };

    class Table
    {
    public:
        friend class LightDB;
        friend class Transaction;
        friend class Query;
        friend class TransactionalView;
        using Index = std::unordered_map<FieldValue, std::vector<PrimaryKey>, FieldValueHash, FieldValueEqual>;

        Table(Schema s, std::string name) : schema_(std::move(s)), name_(std::move(name)), next_id_(1)
        {
            for (const auto &field_name : schema_.get_indexed_fields())
            {
                secondary_indices_[field_name] = Index{};
            }
        }

        Table(const Table &other)
            : schema_(other.schema_), name_(other.name_)
        {
            std::lock_guard<std::mutex> lock(other.table_mutex_);
            data_ = other.data_;
            next_id_ = other.next_id_;
            secondary_indices_ = other.secondary_indices_;
            journal_for_txn_ = other.journal_for_txn_;
        }

        Table &operator=(const Table &other)
        {
            if (this == &other) {
                return *this;
            }
            std::scoped_lock lock(table_mutex_, other.table_mutex_);
            schema_ = other.schema_;
            name_ = other.name_;
            data_ = other.data_;
            next_id_ = other.next_id_;
            secondary_indices_ = other.secondary_indices_;
            journal_for_txn_ = other.journal_for_txn_;
            return *this;
        }

        Table(Table &&other) noexcept
            : schema_(std::move(other.schema_)), name_(std::move(other.name_))
        {
            std::lock_guard<std::mutex> lock(other.table_mutex_);
            data_ = std::move(other.data_);
            next_id_ = other.next_id_;
            secondary_indices_ = std::move(other.secondary_indices_);
            journal_for_txn_ = other.journal_for_txn_;
            other.next_id_ = 1;
            other.journal_for_txn_ = nullptr;
        }

        Table &operator=(Table &&other) noexcept
        {
            if (this == &other) {
                return *this;
            }
            std::scoped_lock lock(table_mutex_, other.table_mutex_);
            schema_ = std::move(other.schema_);
            name_ = std::move(other.name_);
            data_ = std::move(other.data_);
            next_id_ = other.next_id_;
            secondary_indices_ = std::move(other.secondary_indices_);
            journal_for_txn_ = other.journal_for_txn_;
            other.next_id_ = 1;
            other.journal_for_txn_ = nullptr;
            return *this;
        }

        PrimaryKey insert(Row &row, LightDB &db_context);
        bool update(PrimaryKey id, const Row &partial_row, LightDB &db_context);
        bool remove(PrimaryKey id, LightDB &db_context);

        RowBuilder create_row(LightDB &db_context)
        {
            return RowBuilder(*this, db_context);
        }

        std::optional<Row> get(PrimaryKey id) const
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            return get_unlocked(id);
        }

        Query query(LightDB &db_context)
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            return Query(*this, db_context);
        }

        const Query query(LightDB &db_context) const
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            return Query(*this, db_context);
        }

        std::vector<Row> select_by_index(const std::string &field_name, const FieldValue &value) const
        {
            std::lock_guard<std::mutex> lock(table_mutex_);
            auto index_it = secondary_indices_.find(field_name);
            if (index_it == secondary_indices_.end())
            {
                throw LightDBException("Cannot select on non-indexed field '" + field_name + "'. Use query() for unindexed fields.");
            }

            std::vector<Row> results;
            const auto &index = index_it->second;
            auto pks_it = index.find(value);
            if (pks_it != index.end())
            {
                results.reserve(pks_it->second.size());
                for (PrimaryKey pk : pks_it->second)
                {
                    results.push_back(get_unlocked(pk).value());
                }
            }
            return results;
        }

        std::unordered_map<PrimaryKey, Row> get_all_rows() const {
            std::lock_guard<std::mutex> lock(table_mutex_);
            return data_; // Return a copy for thread safety
        }
        const Schema &get_schema() const { return schema_; }
        const std::string &get_name() const { return name_; }

    private:
        std::optional<Row> get_unlocked(PrimaryKey id) const
        {
            // Assumes table_mutex_ is already locked by the caller.
            auto it = data_.find(id);
            if (it != data_.end())
                return it->second;
            return std::nullopt;
        }
    
        bool remove_internal(PrimaryKey id, LightDB &db_context);
        bool update_internal(PrimaryKey id, const Row &partial_row, LightDB &db_context);
        void validate_foreign_keys(const Row &row, LightDB &db_context, const Row *partial_row = nullptr);
        void check_referential_integrity(PrimaryKey id_being_deleted, LightDB &db_context);

        void update_indices_for_insert(const Row &row, PrimaryKey id)
        {
            for (auto &[field_name, index] : secondary_indices_)
            {
                const auto &value_to_index = row.at(field_name);
                index[value_to_index].push_back(id);
            }
        }

        void update_indices_for_remove(const Row &row)
        {
            PrimaryKey id = std::any_cast<PrimaryKey>(row.at(schema_.get_primary_key_field()));
            for (auto &[field_name, index] : secondary_indices_)
            {
                const FieldValue &value = row.at(field_name);
                auto &pks = index.at(value);
                pks.erase(std::remove(pks.begin(), pks.end(), id), pks.end());
                if (pks.empty())
                    index.erase(value);
            }
        }

        Schema schema_;
        std::string name_;
        std::unordered_map<PrimaryKey, Row> data_;
        PrimaryKey next_id_;
        std::unordered_map<std::string, Index> secondary_indices_;
        mutable std::mutex table_mutex_;
        std::vector<Command>* journal_for_txn_ = nullptr;
    };

    struct JoinedRow
    {
        Row left;
        std::optional<Row> right;
    };

    class LightDB
    {
    public:
        enum class JoinType
        {
            Inner,
            Left
        };

        static void set_base_save_path(std::string base_path) {
            base_save_path_ = std::move(base_path);
        }

        LightDB() = default;
        virtual ~LightDB() = default;

        LightDB(const LightDB& other) {
            std::lock_guard<std::mutex> lock(other.db_mutex_);
            tables_ = other.tables_;
        }

        LightDB& operator=(const LightDB& other) {
            if (this == &other) return *this;
            std::scoped_lock lock(db_mutex_, other.db_mutex_);
            tables_ = other.tables_;
            return *this;
        }

        LightDB(LightDB&& other) noexcept {
            std::lock_guard<std::mutex> lock(other.db_mutex_);
            tables_ = std::move(other.tables_);
        }

        LightDB& operator=(LightDB&& other) noexcept {
            if (this == &other) return *this;
            std::scoped_lock lock(db_mutex_, other.db_mutex_);
            tables_ = std::move(other.tables_);
            return *this;
        }

        virtual void create_table(const std::string &name, const Schema &schema);

        virtual Table *get_table(const std::string &name);

        virtual const Table *get_table(const std::string &name) const;

        virtual std::vector<std::string> get_table_names() const;

        std::vector<JoinedRow> join(
            const std::string &left_table_name,
            const std::string &right_table_name,
            const std::string &left_join_field,
            const std::string &right_join_field,
            JoinType join_type = JoinType::Inner)
        {
            std::lock_guard<std::mutex> lock(db_mutex_);
            std::vector<JoinedRow> results;
            Table *left_table = get_table_unlocked(left_table_name);
            const Table *right_table = get_table_unlocked(right_table_name);
            if (!left_table || !right_table)
                throw LightDBException("Join Error: One or both tables not found.");

            if (right_table->get_schema().get_indexed_fields().count(right_join_field))
            {
                for (const auto &[pk, left_row] : left_table->get_all_rows())
                {
                    auto fk_it = left_row.find(left_join_field);
                    if (fk_it == left_row.end() || !fk_it->second.has_value())
                    {
                        if (join_type == JoinType::Left)
                            results.push_back({left_row, std::nullopt});
                        continue;
                    }
                    auto right_rows = right_table->select_by_index(right_join_field, fk_it->second);
                    if (!right_rows.empty())
                    {
                        for (const auto &right_row : right_rows)
                            results.push_back({left_row, right_row});
                    }
                    else if (join_type == JoinType::Left)
                    {
                        results.push_back({left_row, std::nullopt});
                    }
                }
            }
            else
            {
                auto left_rows_copy = left_table->get_all_rows();
                auto right_rows_copy = right_table->get_all_rows();
                for (const auto &[l_pk, l_row] : left_rows_copy)
                {
                    bool match_found = false;
                    auto l_val_it = l_row.find(left_join_field);
                    if (l_val_it == l_row.end())
                    {
                        if (join_type == JoinType::Left)
                            results.push_back({l_row, std::nullopt});
                        continue;
                    }
                    for (const auto &[r_pk, r_row] : right_rows_copy)
                    {
                        auto r_val_it = r_row.find(right_join_field);
                        if (r_val_it != r_row.end() && FieldValueEqual{}(l_val_it->second, r_val_it->second))
                        {
                            results.push_back({l_row, r_row});
                            match_found = true;
                        }
                    }
                    if (!match_found && join_type == JoinType::Left)
                    {
                        results.push_back({l_row, std::nullopt});
                    }
                }
            }
            return results;
        }

        void save(const std::string &path) const;
        static LightDB load(const std::string &path);

    protected:
        friend class TransactionalView;
        friend class Transaction;
        std::unordered_map<std::string, Table> tables_;
        mutable std::mutex db_mutex_;

    private:
        Table* get_table_unlocked(const std::string& name) {
            auto it = tables_.find(name);
            return (it != tables_.end()) ? &it->second : nullptr;
        }

        const Table* get_table_unlocked(const std::string& name) const {
            auto it = tables_.find(name);
            return (it != tables_.end()) ? &it->second : nullptr;
        }
        
        inline static std::string base_save_path_ = "ldb";
    };

    class TransactionalView : public LightDB
    {
    public:
        TransactionalView(LightDB& db, std::vector<Command>& j)
            : original_db_(db), journal_(j) {}

        void create_table(const std::string &name, const Schema &schema) override;
        Table* get_table(const std::string &name) override;
        const Table* get_table(const std::string &name) const override;
        std::vector<std::string> get_table_names() const override;

    private:
        LightDB& original_db_;
        std::vector<Command>& journal_;
    };

    class Transaction
    {
    public:
        explicit Transaction(LightDB &db) : original_db_(db) {}

        void execute(const std::function<void(LightDB &)> &operations);

    private:
        void commit();

        LightDB &original_db_;
        std::vector<Command> journal_;
    };

    inline RowBuilder::RowBuilder(Table &table_ref, LightDB &db_ref) : table_(table_ref), db_(db_ref) {}

    inline PrimaryKey RowBuilder::insert()
    {
        return table_.insert(row_, db_);
    }

    inline std::vector<Row> Query::get_filtered_rows() const
    {
        std::vector<Row> results;

        if (predicates_.empty())
        {
            const auto all_rows = table_.get_all_rows();
            results.reserve(all_rows.size());
            for (const auto& [id, row] : all_rows)
            {
                results.push_back(row);
            }
            return results;
        }

        const Predicate* indexed_predicate = nullptr;
        bool has_or = false;
        for (const auto& item : predicates_) {
            if(std::holds_alternative<LogicalOpType>(item) && std::get<LogicalOpType>(item) == LogicalOpType::OR) {
                has_or = true;
                break;
            }
        }

        if (!has_or) {
            for (const auto& item : predicates_)
            {
                if (std::holds_alternative<Predicate>(item))
                {
                    const auto& p = std::get<Predicate>(item);
                    if (p.op == PredicateOp::Equals && table_.get_schema().get_indexed_fields().count(p.field))
                    {
                        indexed_predicate = &p;
                        break; // Use the first suitable index found
                    }
                }
            }
        }

        std::vector<Row> source_rows;
        if (indexed_predicate)
        {
            source_rows = table_.select_by_index(indexed_predicate->field, indexed_predicate->value);
        }
        else
        {
            const auto all_rows = table_.get_all_rows();
            source_rows.reserve(all_rows.size());
            for (const auto& [id, row] : all_rows)
            {
                source_rows.push_back(row);
            }
        }

        std::vector<std::vector<Predicate>> or_groups;
        or_groups.emplace_back();

        for (const auto& item : predicates_) {
            if (std::holds_alternative<Predicate>(item)) {
                or_groups.back().push_back(std::get<Predicate>(item));
            } else if (std::holds_alternative<LogicalOpType>(item)) {
                if (std::get<LogicalOpType>(item) == LogicalOpType::OR) {
                    if (!or_groups.back().empty()) {
                        or_groups.emplace_back();
                    }
                }
            }
        }
        or_groups.erase(std::remove_if(or_groups.begin(), or_groups.end(),
            [](const auto& g){ return g.empty(); }), or_groups.end());

        results.reserve(source_rows.size());
        for (const auto &row : source_rows) {
            bool row_matches_any_or_group = false;
            for (const auto& group : or_groups) {
                bool group_match = std::all_of(group.begin(), group.end(),
                    [&](const Predicate &p) { return p.func(row); });

                if (group_match) {
                    row_matches_any_or_group = true;
                    break;
                }
            }
            if (row_matches_any_or_group) {
                results.push_back(row);
            }
        }

        return results;
    }

    inline std::vector<Row> Query::execute() const
    {
        auto results = get_filtered_rows();

        if (sorter_)
        {
            std::sort(results.begin(), results.end(), sorter_);
        }

        if (limit_ && results.size() > *limit_)
        {
            results.resize(*limit_);
        }
        return results;
    }

    inline size_t Query::count() const
    {
        return get_filtered_rows().size();
    }

    inline std::optional<FieldValue> Query::sum(const std::string &field) const
    {
        auto rows = get_filtered_rows();
        if (rows.empty()) {
            return std::nullopt;
        }

        const std::type_info* value_type = nullptr;
        for (const auto& row : rows) {
            const auto& val = row.at(field);
            if (val.has_value()) {
                value_type = &val.type();
                break;
            }
        }

        if (!value_type) {
            return std::nullopt;
        }

        if (*value_type == typeid(int))
        {
            long long total = 0;
            for (const auto &row : rows) {
                if(row.at(field).has_value()) {
                    total += std::any_cast<int>(row.at(field));
                }
            }
            return static_cast<int>(total);
        }
        if (*value_type == typeid(double))
        {
            double total = 0.0;
            for (const auto &row : rows) {
                if(row.at(field).has_value()) {
                    total += std::any_cast<double>(row.at(field));
                }
            }
            return total;
        }
        throw LightDBException("Cannot perform SUM on non-numeric field '" + field + "'.");
    }

    inline std::optional<FieldValue> Query::avg(const std::string &field) const
    {
        auto rows = get_filtered_rows();
        if (rows.empty())
            return std::nullopt;

        auto s = sum(field);
        if (!s)
            return std::nullopt;

        long non_null_count = 0;
        for(const auto& row : rows){
            if(row.at(field).has_value()) non_null_count++;
        }
        if (non_null_count == 0) return std::nullopt;


        if (s->type() == typeid(int))
        {
            return static_cast<double>(std::any_cast<int>(*s)) / non_null_count;
        }
        if (s->type() == typeid(double))
        {
            return std::any_cast<double>(*s) / non_null_count;
        }
        return std::nullopt;
    }

    inline std::optional<FieldValue> Query::min(const std::string &field) const
    {
        auto rows = get_filtered_rows();
        if (rows.empty()) return std::nullopt;

        const Row* min_row = nullptr;
        for(const auto& row : rows) {
            if (row.at(field).has_value()) {
                min_row = &row;
                break;
            }
        }
        if (!min_row) return std::nullopt;

        for (const auto& row : rows)
        {
            if (!row.at(field).has_value()) continue;
            const auto &current_val = row.at(field);
            const auto &min_val = min_row->at(field);
            const auto *handler = get_type_registry().get_handler(current_val.type());
            if (handler && handler->less_than(current_val, min_val))
            {
                min_row = &row;
            }
        }
        return min_row->at(field);
    }

    inline std::optional<FieldValue> Query::max(const std::string &field) const
    {
        auto rows = get_filtered_rows();
        if (rows.empty()) return std::nullopt;

        const Row* max_row = nullptr;
        for(const auto& row : rows) {
            if (row.at(field).has_value()) {
                max_row = &row;
                break;
            }
        }
        if (!max_row) return std::nullopt;

        for (const auto& row : rows)
        {
            if (!row.at(field).has_value()) continue;
            const auto &current_val = row.at(field);
            const auto &max_val = max_row->at(field);
            const auto *handler = get_type_registry().get_handler(current_val.type());
            if (handler && handler->less_than(max_val, current_val))
            {
                max_row = &row;
            }
        }
        return max_row->at(field);
    }

    inline int Query::remove()
    {
        auto rows_to_delete = get_filtered_rows();
        int count = 0;
        const std::string &pk_field = table_.get_schema().get_primary_key_field();
        Table *mutable_table = db_context_.get_table(table_.get_name());
        if (!mutable_table)
            return 0;

        std::vector<PrimaryKey> pks_to_delete;
        pks_to_delete.reserve(rows_to_delete.size());
        for (const auto& row : rows_to_delete) {
            pks_to_delete.push_back(std::any_cast<PrimaryKey>(row.at(pk_field)));
        }

        if (mutable_table->journal_for_txn_) {
            for(PrimaryKey pk : pks_to_delete) {
                if (mutable_table->remove_internal(pk, db_context_)) {
                    count++;
                }
            }
        } else {
            std::lock_guard<std::mutex> lock(mutable_table->table_mutex_);
            for(PrimaryKey pk : pks_to_delete) {
                if (mutable_table->remove_internal(pk, db_context_)) {
                    count++;
                }
            }
        }
        return count;
    }

    inline int Query::update(const Row &partial_row)
    {
        auto rows_to_update = get_filtered_rows();
        int count = 0;
        const std::string &pk_field = table_.get_schema().get_primary_key_field();
        Table *mutable_table = db_context_.get_table(table_.get_name());
        if (!mutable_table)
            return 0;

        std::vector<PrimaryKey> pks_to_update;
        pks_to_update.reserve(rows_to_update.size());
        for (const auto& row : rows_to_update) {
            pks_to_update.push_back(std::any_cast<PrimaryKey>(row.at(pk_field)));
        }

        if (mutable_table->journal_for_txn_) {
            for(PrimaryKey pk : pks_to_update) {
                if (mutable_table->update_internal(pk, partial_row, db_context_)) {
                    count++;
                }
            }
        } else {
            std::lock_guard<std::mutex> lock(mutable_table->table_mutex_);
            for(PrimaryKey pk : pks_to_update) {
                if (mutable_table->update_internal(pk, partial_row, db_context_)) {
                    count++;
                }
            }
        }
        return count;
    }

    inline PrimaryKey Table::insert(Row &row, LightDB &db_context)
    {
        if (journal_for_txn_) {
            const std::string &pk_field = schema_.get_primary_key_field();
            if (row.find(pk_field) == row.end())
            {
                row[pk_field] = next_id_;
            }

            for (const auto& [field_name, field_type] : schema_.get_fields()) {
                if (row.find(field_name) == row.end() && schema_.is_nullable(field_name)) {
                    row[field_name] = FieldValue{};
                }
            }

            auto err = schema_.get_validation_error(row);
            if (!err.empty()) throw LightDBException("Tx Insert failed: schema validation. " + err);

            PrimaryKey id = std::any_cast<PrimaryKey>(row.at(pk_field));
            if (data_.count(id)) throw LightDBException("Row with PK " + std::to_string(id) + " already exists in transaction.");

            validate_foreign_keys(row, db_context);

            data_[id] = row;
            update_indices_for_insert(row, id);

            if (id >= next_id_) next_id_ = id + 1;

            journal_for_txn_->push_back(InsertCommand{name_, row});
            return id;
        }

        std::lock_guard<std::mutex> lock(table_mutex_);
        const std::string &pk_field = schema_.get_primary_key_field();
        if (row.find(pk_field) == row.end())
        {
            row[pk_field] = next_id_;
        }
        else
        {
            PrimaryKey provided_id = std::any_cast<PrimaryKey>(row.at(pk_field));
            if (data_.count(provided_id))
            {
                throw LightDBException("Row with primary key " + std::to_string(provided_id) + " already exists.");
            }
        }

        for (const auto& [field_name, field_type] : schema_.get_fields()) {
            if (row.find(field_name) == row.end() && schema_.is_nullable(field_name)) {
                row[field_name] = FieldValue{};
            }
        }

        auto err = schema_.get_validation_error(row);
        if (!err.empty()) throw LightDBException("Insert failed: schema validation failed. " + err);

        PrimaryKey id = std::any_cast<PrimaryKey>(row.at(pk_field));
        validate_foreign_keys(row, db_context);
        data_[id] = row;
        update_indices_for_insert(row, id);
        if (id >= next_id_) next_id_ = id + 1;
        return id;
    }

    inline bool Table::update_internal(PrimaryKey id, const Row &partial_row, LightDB &db_context)
    {
        auto it = data_.find(id);
        if (it == data_.end()) return false;

        Row original_row = it->second;
        Row updated_row = original_row;

        for (const auto &[field, value] : partial_row) {
            if (field == schema_.get_primary_key_field()) throw LightDBException("Cannot update primary key field.");
            if (!schema_.get_fields().count(field)) throw LightDBException("Cannot update non-existent field '" + field + "'.");
            updated_row[field] = value;
        }

        auto err = schema_.get_validation_error(updated_row);
        if (!err.empty()) throw LightDBException("Update failed: schema validation failed. " + err);

        validate_foreign_keys(updated_row, db_context, &partial_row);
        update_indices_for_remove(original_row);
        it->second = updated_row;
        update_indices_for_insert(it->second, id);

        if (journal_for_txn_) {
            journal_for_txn_->push_back(UpdateCommand{name_, id, partial_row});
        }
        return true;
    }

    inline bool Table::update(PrimaryKey id, const Row &partial_row, LightDB &db_context)
    {
        if (journal_for_txn_) {
            return update_internal(id, partial_row, db_context);
        }

        std::lock_guard<std::mutex> lock(table_mutex_);
        return update_internal(id, partial_row, db_context);
    }

    inline bool Table::remove_internal(PrimaryKey id, LightDB &db_context)
    {
        auto it = data_.find(id);
        if (it == data_.end()) return false;

        check_referential_integrity(id, db_context);
        update_indices_for_remove(it->second);
        data_.erase(it);

        if (journal_for_txn_) {
            journal_for_txn_->push_back(DeleteCommand{name_, id});
        }
        return true;
    }

    inline bool Table::remove(PrimaryKey id, LightDB &db_context)
    {
        if (journal_for_txn_) {
            return remove_internal(id, db_context);
        }

        std::lock_guard<std::mutex> lock(table_mutex_);
        return remove_internal(id, db_context);
    }

    inline void Table::validate_foreign_keys(const Row &row, LightDB &db_context, const Row *partial_row)
    {
        for (const auto &fk : schema_.get_foreign_keys())
        {
            if (partial_row && !partial_row->count(fk.local_field))
                continue;

            const FieldValue &fk_val = row.at(fk.local_field);
            if (!fk_val.has_value())
            {
                continue;
            }

            const Table *target_table = db_context.get_table(fk.target_table);
            if (!target_table)
                throw LightDBException("FK validation failed: target table '" + fk.target_table + "' does not exist.");

            PrimaryKey target_id = std::any_cast<PrimaryKey>(fk_val);
            if (!target_table->get(target_id))
            {
                throw LightDBException("FK constraint failed: no row with PK " + std::to_string(target_id) + " in table '" + fk.target_table + "'.");
            }
        }
    }

    inline void Table::check_referential_integrity(PrimaryKey id_being_deleted, LightDB &db_context)
    {
        const std::string &pk_field_name = schema_.get_primary_key_field();
        const std::string &current_table_name = name_;

        for (const auto &table_name : db_context.get_table_names())
        {
            Table *referencing_table = db_context.get_table(table_name);
            for (const auto &fk : referencing_table->get_schema().get_foreign_keys())
            {
                if (fk.target_table == current_table_name && fk.target_field == pk_field_name)
                {
                    auto dependent_rows = referencing_table->query(db_context).where(fk.local_field).equals(id_being_deleted).execute();

                    if (!dependent_rows.empty())
                    {
                        if (fk.on_delete_action == OnDeleteAction::Restrict)
                        {
                            throw LightDBException("Cannot delete row with PK " + std::to_string(id_being_deleted) + " from '" +
                                                   current_table_name + "' because it is referenced by table '" + table_name + "'.");
                        }
                        if (fk.on_delete_action == OnDeleteAction::Cascade)
                        {
                            for (const auto &row_to_delete : dependent_rows)
                            {
                                PrimaryKey pk_to_delete = std::any_cast<PrimaryKey>(row_to_delete.at(referencing_table->get_schema().get_primary_key_field()));
                                referencing_table->remove(pk_to_delete, db_context);
                            }
                        }
                    }
                }
            }
        }
    }

    inline void LightDB::create_table(const std::string &name, const Schema &schema)
    {
        std::lock_guard<std::mutex> lock(db_mutex_);
        if (tables_.count(name))
        {
            throw LightDBException("Table '" + name + "' already exists.");
        }
        tables_.emplace(name, Table(schema, name));
    }

    inline Table* LightDB::get_table(const std::string &name) {
        std::lock_guard<std::mutex> lock(db_mutex_);
        auto it = tables_.find(name);
        return (it != tables_.end()) ? &it->second : nullptr;
    }

    inline const Table* LightDB::get_table(const std::string &name) const {
        std::lock_guard<std::mutex> lock(db_mutex_);
        auto it = tables_.find(name);
        return (it != tables_.end()) ? &it->second : nullptr;
    }

    inline std::vector<std::string> LightDB::get_table_names() const {
        std::lock_guard<std::mutex> lock(db_mutex_);
        std::vector<std::string> names;
        names.reserve(tables_.size());
        for (const auto &[name, table] : tables_)
            names.push_back(name);
        return names;
    }

    inline void LightDB::save(const std::string &path) const
    {
        std::lock_guard<std::mutex> lock(db_mutex_);

        json db_json;
        db_json["tables"] = json::object();

        for (const auto &[table_name, table] : tables_)
        {
            json table_json;
            const auto &schema = table.get_schema();

            json schema_json;
            schema_json["primary_key"] = schema.get_primary_key_field();

            schema_json["fields"] = json::object();
            for (const auto &[field_name, type_idx] : schema.get_fields())
            {
                schema_json["fields"][field_name]["type"] = get_type_registry().get_type_name(type_idx);
                schema_json["fields"][field_name]["nullable"] = schema.is_nullable(field_name);
            }

            schema_json["indices"] = json::array();
            for (const auto &indexed_field : schema.get_indexed_fields())
            {
                schema_json["indices"].push_back(indexed_field);
            }

            schema_json["foreign_keys"] = json::array();
            for (const auto &fk : schema.get_foreign_keys())
            {
                json fk_json;
                fk_json["local_field"] = fk.local_field;
                fk_json["target_table"] = fk.target_table;
                fk_json["target_field"] = fk.target_field;
                fk_json["on_delete"] = (fk.on_delete_action == OnDeleteAction::Cascade ? "CASCADE" : "RESTRICT");
                schema_json["foreign_keys"].push_back(fk_json);
            }
            table_json["schema"] = schema_json;

            json data_json = json::array();
            for (const auto &[pk, row] : table.get_all_rows())
            {
                json row_json = json::object();
                for (const auto &[field_name, value] : row)
                {
                    if (!value.has_value())
                    {
                        row_json[field_name] = nullptr;
                        continue;
                    }
                    const auto &type = value.type();
                    if (type == typeid(int))
                        row_json[field_name] = std::any_cast<int>(value);
                    else if (type == typeid(double))
                        row_json[field_name] = std::any_cast<double>(value);
                    else if (type == typeid(bool))
                        row_json[field_name] = std::any_cast<bool>(value);
                    else if (type == typeid(std::string))
                        row_json[field_name] = std::any_cast<const std::string &>(value);
                    else
                    {
                        const auto *handler = get_type_registry().get_handler(type);
                        if (!handler || !handler->serialise)
                        {
                            throw LightDBException("Save error: Cannot serialise unregistered or unserialisable type for field " + field_name);
                        }
                        row_json[field_name] = handler->serialise(value);
                    }
                }
                data_json.push_back(row_json);
            }
            table_json["data"] = data_json;

            db_json["tables"][table_name] = table_json;
        }

        std::filesystem::path filepath;
        if (base_save_path_.empty()) {
            filepath = path;
        } else {
            filepath = base_save_path_;
            filepath /= path;
        }

        if (filepath.has_parent_path()) {
            std::filesystem::create_directories(filepath.parent_path());
        }

        std::ofstream out(filepath);
        if (!out)
        {
            throw LightDBException("Failed to open file for writing: " + filepath.string());
        }
        out << db_json.dump(4);
    }

    inline LightDB LightDB::load(const std::string &path)
    {
        std::filesystem::path filepath;
        if (base_save_path_.empty()) {
            filepath = path;
        } else {
            filepath = base_save_path_;
            filepath /= path;
        }

        std::ifstream in(filepath);
        if (!in)
        {
            throw LightDBException("Failed to open file for reading: " + filepath.string());
        }

        LightDB db;
        json db_json;
        try
        {
            db_json = json::parse(in);
        }
        catch (json::parse_error &e)
        {
            throw LightDBException("Failed to parse JSON file: " + std::string(e.what()));
        }

        if (!db_json.contains("tables") || !db_json["tables"].is_object())
        {
            throw LightDBException("Load error: JSON file must contain a 'tables' object.");
        }

        for (auto const&[table_name, table_json] : db_json["tables"].items())
        {
            if (!table_json.contains("schema") || !table_json["schema"].is_object())
            {
                throw LightDBException("Load error: table '" + table_name + "' is missing 'schema' object.");
            }
            const auto &schema_json = table_json["schema"];

            std::string pk_name = schema_json.at("primary_key").get<std::string>();
            Schema schema(pk_name);

            for (auto const&[field_name, field_json] : schema_json.at("fields").items())
            {
                if (field_name == pk_name) continue;

                std::string type_name = field_json.at("type").get<std::string>();
                bool nullable = field_json.at("nullable").get<bool>();

                std::type_index type_idx = get_type_registry().get_type_index_by_name(type_name);
                if (type_idx == typeid(void))
                    throw LightDBException("Load error: Unknown type '" + type_name + "' for field '" + field_name + "'.");

                schema.fields.emplace(field_name, type_idx);
                if (nullable) schema.nullable_fields.insert(field_name);
            }

            if (schema_json.contains("indices") && schema_json["indices"].is_array())
            {
                for (const auto &index_json : schema_json["indices"])
                {
                    schema.add_index(index_json.get<std::string>());
                }
            }

            if (schema_json.contains("foreign_keys") && schema_json["foreign_keys"].is_array())
            {
                for (const auto &fk_json : schema_json["foreign_keys"])
                {
                    std::string local = fk_json.at("local_field").get<std::string>();
                    std::string target_t = fk_json.at("target_table").get<std::string>();
                    std::string target_f = fk_json.at("target_field").get<std::string>();
                    std::string action_str = fk_json.at("on_delete").get<std::string>();
                    OnDeleteAction action = (action_str == "CASCADE") ? OnDeleteAction::Cascade : OnDeleteAction::Restrict;
                    schema.add_foreign_key(local, target_t, target_f, action);
                }
            }

            db.create_table(table_name, schema);
        }

        for (auto const&[table_name, table_json] : db_json["tables"].items())
        {
            Table *table = db.get_table(table_name);
            if (!table_json.contains("data") || !table_json["data"].is_array())
            {
                throw LightDBException("Load error: table '" + table_name + "' is missing 'data' array.");
            }

            const auto &data_json = table_json["data"];
            const auto &schema = table->get_schema();

            for (const auto &row_json : data_json)
            {
                Row row;
                for(const auto& [field_name, expected_type] : schema.get_fields())
                {
                    if (!row_json.contains(field_name) || row_json.at(field_name).is_null())
                    {
                        row[field_name] = {};
                        continue;
                    }

                    const auto& j_val = row_json.at(field_name);

                    if (expected_type == typeid(int)) row[field_name] = j_val.get<int>();
                    else if (expected_type == typeid(double)) row[field_name] = j_val.get<double>();
                    else if (expected_type == typeid(bool)) row[field_name] = j_val.get<bool>();
                    else if (expected_type == typeid(std::string)) row[field_name] = j_val.get<std::string>();
                    else
                    {
                        const auto *handler = get_type_registry().get_handler(expected_type);
                        if (!handler || !handler->deserialise)
                        {
                            throw LightDBException("Load error: Cannot deserialise type for field '" + field_name + "'.");
                        }
                        row[field_name] = handler->deserialise(j_val.get<std::string>());
                    }
                }
                table->insert(row, db);
            }
        }
        return db;
    }

    inline void Transaction::commit() {
        std::lock_guard<std::mutex> db_lock(original_db_.db_mutex_);

        for (const auto& command : journal_) {
            std::visit([this](auto&& cmd) {
                using T = std::decay_t<decltype(cmd)>;

                if constexpr (std::is_same_v<T, CreateTableCommand>) {
                    if (original_db_.tables_.count(cmd.table_name)) {
                        throw LightDBException("Commit conflict: Table '" + cmd.table_name + "' created concurrently.");
                    }
                    original_db_.tables_.emplace(cmd.table_name, Table(cmd.schema, cmd.table_name));
                }
                else {
                    auto table_it = original_db_.tables_.find(cmd.table_name);
                    if (table_it == original_db_.tables_.end()) {
                        throw LightDBException("Commit failed: Table '" + cmd.table_name + "' not found.");
                    }
                    Table& table = table_it->second;

                    std::lock_guard<std::mutex> table_lock(table.table_mutex_);

                    if constexpr (std::is_same_v<T, InsertCommand>) {
                        Row row = cmd.row;
                        const PrimaryKey pk = std::any_cast<PrimaryKey>(row.at(table.schema_.get_primary_key_field()));
                        table.data_[pk] = row;
                        table.update_indices_for_insert(row, pk);
                        if (pk >= table.next_id_) {
                            table.next_id_ = pk + 1;
                        }
                    }
                    else if constexpr (std::is_same_v<T, UpdateCommand>) {
                        auto row_it = table.data_.find(cmd.pk);
                        if (row_it != table.data_.end()) {
                            Row original_row = row_it->second;
                            Row updated_row = original_row;
                            for (const auto& [field, value] : cmd.partial_row) {
                                updated_row[field] = value;
                            }
                            table.update_indices_for_remove(original_row);
                            row_it->second = updated_row;
                            table.update_indices_for_insert(updated_row, cmd.pk);
                        }
                    }
                    else if constexpr (std::is_same_v<T, DeleteCommand>) {
                        auto row_it = table.data_.find(cmd.pk);
                        if (row_it != table.data_.end()) {
                            table.update_indices_for_remove(row_it->second);
                            table.data_.erase(row_it);
                        }
                    }
                }
            }, command);
        }
    }

    inline void Transaction::execute(const std::function<void(LightDB &)> &operations)
    {
        journal_.clear();
        TransactionalView transactional_view(original_db_, journal_);

        try
        {
            operations(transactional_view);
            commit();
        }
        catch (...)
        {
            std::rethrow_exception(std::current_exception());
        }
    }

    inline void TransactionalView::create_table(const std::string &name, const Schema &schema)
    {
        if (tables_.count(name) || original_db_.get_table(name)) {
             throw LightDBException("Table '" + name + "' already exists.");
        }
        auto [it, success] = tables_.emplace(name, Table(schema, name));
        it->second.journal_for_txn_ = &journal_;
        journal_.push_back(CreateTableCommand{name, schema});
    }

    inline Table* TransactionalView::get_table(const std::string &name)
    {
        auto it = tables_.find(name);
        if (it != tables_.end()) {
            return &it->second;
        }

        Table* original_table = original_db_.get_table(name);
        if (original_table) {
            auto [emplaced_it, success] = tables_.emplace(name, *original_table);
            emplaced_it->second.journal_for_txn_ = &journal_;
            return &emplaced_it->second;
        }

        return nullptr;
    }

    inline const Table* TransactionalView::get_table(const std::string &name) const
    {
        return const_cast<TransactionalView*>(this)->get_table(name);
    }

    inline std::vector<std::string> TransactionalView::get_table_names() const
    {
        std::unordered_set<std::string> names;
        for (const auto& name : original_db_.get_table_names()) {
            names.insert(name);
        }
        for (const auto& [name, table] : tables_) {
            names.insert(name);
        }
        return {names.begin(), names.end()};
    }
}