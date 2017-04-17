////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OS_RESULTS_BASE_HPP
#define REALM_OS_RESULTS_BASE_HPP

#include "collection_notifications.hpp"
#include "impl/collection_notifier.hpp"

#include <realm/table_view.hpp>
#include <realm/util/optional.hpp>

namespace realm {
template<typename T> class BasicRowExpr;
using RowExpr = BasicRowExpr<Table>;
class Mixed;
class ObjectSchema;

namespace _impl {
class ResultsNotifier;

class ResultsBase {
public:
    // ResultsBase can be either be backed by nothing, a thin wrapper around a table,
    // or a wrapper around a query and a sort order which creates and updates
    // the tableview as needed
    ResultsBase();
    ResultsBase(std::shared_ptr<Realm> r, Table& table);
    ResultsBase(std::shared_ptr<Realm> r, Query q,
                SortDescriptor s = {}, SortDescriptor d = {});
    ResultsBase(std::shared_ptr<Realm> r, TableView tv,
                SortDescriptor s = {}, SortDescriptor d = {});
    ResultsBase(std::shared_ptr<Realm> r, LinkViewRef lv,
                util::Optional<Query> q = {}, SortDescriptor s = {});
    ~ResultsBase();

    // ResultsBase is copyable and moveable
    ResultsBase(ResultsBase&&);
    ResultsBase& operator=(ResultsBase&&);
    ResultsBase(const ResultsBase&);
    ResultsBase& operator=(const ResultsBase&);

    // Get the Realm
    std::shared_ptr<Realm> get_realm() const { return m_realm; }

    // Get a query which will match the same rows as is contained in this ResultsBase
    // Returned query will not be valid if the current mode is Empty
    Query get_query() const;

    // Get the currently applied sort order for this ResultsBase
    SortDescriptor const& get_sort() const noexcept { return m_sort; }

    // Get the currently applied distinct condition for this ResultsBase
    SortDescriptor const& get_distinct() const noexcept { return m_distinct; }
    
    // Get a tableview containing the same rows as this ResultsBase
    TableView get_tableview();

    // Get the LinkView this ResultsBase is derived from, if any
    LinkViewRef get_linkview() const noexcept { return m_link_view; }

    // Get the size of this results
    // Can be either O(1) or O(N) depending on the state of things
    size_t size();

    // Delete all of the rows in this ResultsBase from the Realm
    // size() will always be zero afterwards
    // Throws InvalidTransactionException if not in a write transaction
    void clear();

    enum class Mode {
        Empty, // Backed by nothing (for missing tables)
        Table, // Backed directly by a Table
        Query, // Backed by a query that has not yet been turned into a TableView
        LinkView,  // Backed directly by a LinkView
        TableView, // Backed by a TableView created from a Query
    };
    // Get the currrent mode of the ResultsBase
    // Ideally this would not be public but it's needed for some KVO stuff
    Mode get_mode() const { return m_mode; }

    // Is this ResultsBase associated with a Realm that has not been invalidated?
    bool is_valid() const;

    // The ResultsBase object has been invalidated (due to the Realm being invalidated)
    // All non-noexcept functions can throw this
    struct InvalidatedException : public std::logic_error {
        InvalidatedException() : std::logic_error("Access to invalidated ResultsBase objects") {}
    };

    // The input index parameter was out of bounds
    struct OutOfBoundsIndexException : public std::out_of_range {
        OutOfBoundsIndexException(size_t r, size_t c);
        const size_t requested;
        const size_t valid_count;
    };

    // The input Row object is not attached
    struct DetatchedAccessorException : public std::logic_error {
        DetatchedAccessorException() : std::logic_error("Atempting to access an invalid object") {}
    };

    // The input Row object belongs to a different table
    struct IncorrectTableException : public std::logic_error {
        IncorrectTableException(StringData e, StringData a, const std::string &error) :
            std::logic_error(error), expected(e), actual(a) {}
        const StringData expected;
        const StringData actual;
    };

    // The requested aggregate operation is not supported for the column type
    struct UnsupportedColumnTypeException : public std::logic_error {
        size_t column_index;
        StringData column_name;
        DataType column_type;

        UnsupportedColumnTypeException(size_t column, const Table* table, const char* operation);
    };

    // Create an async query from this ResultsBase
    // The query will be run on a background thread and delivered to the callback,
    // and then rerun after each commit (if needed) and redelivered if it changed
    NotificationToken async(std::function<void (std::exception_ptr)> target);
    NotificationToken add_notification_callback(CollectionChangeCallback cb) &;

    bool wants_background_updates() const { return m_wants_background_updates; }

    // Returns whether the rows are guaranteed to be in table order.
    bool is_in_table_order() const;

    // Helper type to let ResultsNotifier update the tableview without giving access
    // to any other privates or letting anyone else do so
    class Internal {
        friend class _impl::ResultsNotifier;
        static void set_table_view(ResultsBase& results, TableView&& tv);
    };

protected:
    void validate_read() const;
    void validate_write() const;

    void update_tableview(bool wants_notifications = true);
    bool update_linkview();

    void switch_to_query();
    void snapshot();

    Table* table() const noexcept { return m_table.get(); }
    TableView& tableview() noexcept { return m_table_view; }

    bool auto_update() const noexcept { return m_update_policy == UpdatePolicy::Auto; }

    enum class UpdatePolicy {
        Auto,  // Update automatically to reflect changes in the underlying data.
        Never, // Never update.
    };

private:
    std::shared_ptr<Realm> m_realm;
    Query m_query;
    TableView m_table_view;
    LinkViewRef m_link_view;
    TableRef m_table;
    SortDescriptor m_sort;
    SortDescriptor m_distinct;

    _impl::CollectionNotifier::Handle<_impl::ResultsNotifier> m_notifier;

    Mode m_mode = Mode::Empty;
    UpdatePolicy m_update_policy = UpdatePolicy::Auto;
    bool m_has_used_table_view = false;
    bool m_wants_background_updates = true;

    void prepare_async();

    void set_table_view(TableView&& tv);
};
} // namespace _impl
} // namespace realm

#endif // REALM_OS_RESULTS_BASE_HPP
